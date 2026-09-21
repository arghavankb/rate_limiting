import concurrent.futures
from unittest.mock import patch

from django.core.cache import cache
from django.test import RequestFactory, TestCase
from django_redis import get_redis_connection
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from api_throttles.algorithms.fixed_window import FixedWindowThrottle
from api_throttles.algorithms.sliding_window import SlidingWindowThrottle
from api_throttles.algorithms.token_bucket import TokenBucketThrottle


class FixedWindowTestView(APIView):
    throttle_classes = [FixedWindowThrottle]

    def get(self, request):
        return Response({"detail": "fixed window throttle success"})


class FixedWindowTestCase(TestCase):
    BASE_TIME = 1000.0

    def setUp(self):
        cache.clear()

        assert cache.__class__.__name__ != "DummyCache", (
            "Throttle tests require a real cache backend (LocMemCache/Redis), "
            "not DummyCache."
        )

        self.factory = RequestFactory()
        self.view = FixedWindowTestView.as_view()

    def _make_request(self, remote_addr="127.0.0.1"):
        return self.factory.get("/fake-url/", REMOTE_ADDR=remote_addr)

    def test_rate_limiting_allowed_under_limit(self):
        """Requests under the limit should all succeed"""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(FixedWindowThrottle.rate_limit - 1):
                response = self.view(self._make_request())
                self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_rate_limiting_blocked_over_limit(self):
        """The (rate_limit + 1)-th request in a window should be throttled."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(FixedWindowThrottle.rate_limit):
                response = self.view(self._make_request())
                self.assertEqual(response.status_code, status.HTTP_200_OK)

            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

    def test_rate_limiting_blocked_response_has_retry_after_header(self):
        """A 429 response should include a usable Retry-After header."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(FixedWindowThrottle.rate_limit):
                self.view(self._make_request())

            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)
            self.assertIn("Retry-After", response.headers)
            self.assertGreaterEqual(int(response.headers["Retry-After"]), 0)

    def test_rate_limiting_window_expiration(self):
        """Once the window rolls over, the counter should reset."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(FixedWindowThrottle.rate_limit):
                response = self.view(self._make_request())
                self.assertEqual(response.status_code, status.HTTP_200_OK)

            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

        window_seconds = FixedWindowThrottle.window_seconds
        new_time = self.BASE_TIME + window_seconds + 1

        with patch("time.time", return_value=new_time):
            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_different_clients_have_independent_limits(self):
        """Throttling for one client should not affect another."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(FixedWindowThrottle.rate_limit):
                response = self.view(self._make_request(remote_addr="1.1.1.1"))
                self.assertEqual(response.status_code, status.HTTP_200_OK)

            # Client A is now throttled.
            response_a = self.view(self._make_request(remote_addr="1.1.1.1"))
            self.assertEqual(response_a.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

            # Client B should be completely unaffected.
            response_b = self.view(self._make_request(remote_addr="2.2.2.2"))
            self.assertEqual(response_b.status_code, status.HTTP_200_OK)


class SlidingWindowTestView(APIView):
    throttle_classes = [SlidingWindowThrottle]

    def get(self, request):
        return Response({"detail": "sliding window success"})


class SlidingWindowTestCase(TestCase):
    BASE_TIME = 1000.0

    def setUp(self):
        self.redis_conn = get_redis_connection("default")
        self.redis_conn.flushdb()

        self.factory = RequestFactory()
        self.view = SlidingWindowTestView.as_view()

    def _make_request(self, remote_addr="127.0.0.1"):
        return self.factory.get("/fake-url/", REMOTE_ADDR=remote_addr)

    def test_rate_limiting_allowed_under_limit(self):
        """Requests under the limit should all succeed."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(SlidingWindowThrottle.rate_limit - 1):
                response = self.view(self._make_request())
                self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_rate_limiting_blocked_over_limit(self):
        """The (rate_limit + 1)-th request in the window should be throttled."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(SlidingWindowThrottle.rate_limit):
                response = self.view(self._make_request())
                self.assertEqual(response.status_code, status.HTTP_200_OK)

            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

    def test_rejected_request_is_not_persisted(self):
        """A rejected request must not leave a timestamp behind"""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(SlidingWindowThrottle.rate_limit):
                self.view(self._make_request())

            # This one should be rejected
            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

            # The sorted set should still only contain SlidingWindowThrottle.rate_limit
            # entries
            throttle = SlidingWindowThrottle()
            ident = throttle.get_ident(self._make_request())
            cache_key = f"throttle_sliding_window_{ident}"
            self.assertEqual(self.redis_conn.zcard(cache_key), 5)

    def test_retry_while_blocked_does_not_extend_window(self):
        """
        Repeatedly retrying while blocked must not push the effective
        unblock time further into the future.
        """
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(SlidingWindowThrottle.rate_limit):
                self.view(self._make_request())

        # Client hammers the endpoint every second while blocked.
        for offset in range(1, 10):
            with patch("time.time", return_value=self.BASE_TIME + offset):
                response = self.view(self._make_request())
                self.assertEqual(
                    response.status_code, status.HTTP_429_TOO_MANY_REQUESTS
                )

        # Once the original oldest entry (at BASE_TIME) has aged out of the
        # 60s window, the client should be allowed again
        with patch(
            "time.time",
            return_value=self.BASE_TIME + SlidingWindowThrottle.window_seconds + 0.1,
        ):
            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_rate_limiting_blocked_response_has_retry_after_header(self):
        """A 429 response should include a usable Retry-After header."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(SlidingWindowThrottle.rate_limit):
                self.view(self._make_request())

            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)
            self.assertIn("Retry-After", response.headers)
            self.assertGreaterEqual(int(response.headers["Retry-After"]), 0)

    def test_different_clients_have_independent_limits(self):
        """Throttling for one client should not affect another."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(SlidingWindowThrottle.rate_limit):
                response = self.view(self._make_request(remote_addr="1.1.1.1"))
                self.assertEqual(response.status_code, status.HTTP_200_OK)

            response_a = self.view(self._make_request(remote_addr="1.1.1.1"))
            self.assertEqual(response_a.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

            response_b = self.view(self._make_request(remote_addr="2.2.2.2"))
            self.assertEqual(response_b.status_code, status.HTTP_200_OK)


class TokenBucketTestView(APIView):
    throttle_classes = [TokenBucketThrottle]

    def get(self, request):
        return Response({"detail": "token bucket success"})


class TokenBucketTestCase(TestCase):
    BASE_TIME = 1000.0

    def setUp(self):
        self.redis_conn = get_redis_connection("default")
        self.redis_conn.flushdb()

        self.factory = RequestFactory()
        self.view = TokenBucketTestView.as_view()

    def _make_request(self, remote_addr="127.0.0.1"):
        return self.factory.get("/fake-url/", REMOTE_ADDR=remote_addr)

    def _cache_key(self, remote_addr="127.0.0.1"):
        throttle = TokenBucketThrottle()
        ident = throttle.get_ident(self._make_request(remote_addr))
        return f"throttle_token_bucket_{ident}"

    def test_requests_allowed_up_to_capacity(self):
        """All requests up to `capacity` should succeed with no elapsed time."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(TokenBucketThrottle.capacity):
                response = self.view(self._make_request())
                self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_request_blocked_when_bucket_empty(self):
        """The (capacity + 1)-th request with no elapsed time should be throttled."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(TokenBucketThrottle.capacity):
                self.view(self._make_request())

            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

    def test_rejected_request_does_not_consume_a_token(self):
        """A rejected request must not push the token count below zero."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(TokenBucketThrottle.capacity):
                self.view(self._make_request())

            # These should all be rejected without driving tokens negative.
            for _ in range(3):
                response = self.view(self._make_request())
                self.assertEqual(
                    response.status_code, status.HTTP_429_TOO_MANY_REQUESTS
                )

            tokens = float(self.redis_conn.hget(self._cache_key(), "tokens"))
            self.assertGreaterEqual(tokens, 0.0)
            self.assertLess(tokens, 1.0)

    def test_token_refill_after_time_passes(self):
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(TokenBucketThrottle.capacity):
                self.view(self._make_request())

            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

            refill_seconds = 1 / TokenBucketThrottle.refill_rate
            with patch(
                "time.time", return_value=self.BASE_TIME + refill_seconds + 0.05
            ):
                response = self.view(self._make_request())
                self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_blocked_response_has_retry_after_header(self):
        """A 429 response should include a usable Retry-After header."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(TokenBucketThrottle.capacity):
                self.view(self._make_request())

            response = self.view(self._make_request())
            self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)
            self.assertIn("Retry-After", response.headers)
            self.assertGreaterEqual(int(response.headers["Retry-After"]), 0)

    def test_different_clients_have_independent_limits(self):
        """Throttling for one client should not affect another."""
        with patch("time.time", return_value=self.BASE_TIME):
            for _ in range(TokenBucketThrottle.capacity):
                response = self.view(self._make_request(remote_addr="1.1.1.1"))
                self.assertEqual(response.status_code, status.HTTP_200_OK)

            response_a = self.view(self._make_request(remote_addr="1.1.1.1"))
            self.assertEqual(response_a.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

            response_b = self.view(self._make_request(remote_addr="2.2.2.2"))
            self.assertEqual(response_b.status_code, status.HTTP_200_OK)

    def test_concurrent_requests_do_not_exceed_capacity(self):
        num_requests = TokenBucketThrottle.capacity * 4

        def fire(_):
            response = self.view(self._make_request())
            return response.status_code

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=num_requests
        ) as executor:
            results = list(executor.map(fire, range(num_requests)))

        allowed = results.count(status.HTTP_200_OK)
        blocked = results.count(status.HTTP_429_TOO_MANY_REQUESTS)

        self.assertEqual(allowed, TokenBucketThrottle.capacity)
        self.assertEqual(blocked, num_requests - TokenBucketThrottle.capacity)

import math
import time

from django.core.cache import cache
from rest_framework.throttling import BaseThrottle


class FixedWindowThrottle(BaseThrottle):
    rate_limit = 5  # Max requests
    window_seconds = 60  # Time window

    def allow_request(self, request, view):
        ident = self.get_ident(request)
        current_window = int(time.time()) // self.window_seconds
        cache_key = f"throttle_fixed_window_{ident}_{current_window}"

        try:
            # Try to increment the counter if the key already exists
            current_requests = cache.incr(cache_key)
        except ValueError:
            # If the key doesn't exist, create it with a value of 1 and set expiration
            cache.add(cache_key, 0, timeout=self.window_seconds)
            current_requests = cache.incr(cache_key)

        return current_requests <= self.rate_limit

    def wait(self):
        current_window = int(time.time()) // self.window_seconds
        window_end = (current_window + 1) * self.window_seconds
        remaining = window_end - time.time()
        return max(0, math.ceil(remaining))

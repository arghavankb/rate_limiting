import math
import time
import uuid

from django_redis import get_redis_connection
from redis import RedisError
from rest_framework.throttling import BaseThrottle


class SlidingWindowThrottle(BaseThrottle):
    rate_limit = 5
    window_seconds = 60

    def __init__(self):
        self.ident = None

    def allow_request(self, request, view):
        self.ident = self.get_ident(request)
        redis_conn = get_redis_connection("default")

        now = time.time()
        clear_before = now - self.window_seconds
        cache_key = f"throttle_sliding_window_{self.ident}"
        member = f"{now}-{uuid.uuid4()}"

        pipeline = redis_conn.pipeline()
        pipeline.zremrangebyscore(cache_key, 0, clear_before)
        pipeline.zadd(cache_key, {member: now})
        pipeline.zcard(cache_key)
        pipeline.expire(cache_key, self.window_seconds)

        try:
            _, _, request_count, _ = pipeline.execute()
        except RedisError:
            return True

        if request_count > self.rate_limit:
            redis_conn.zrem(cache_key, member)
            return False
        return True

    def wait(self):
        redis_conn = get_redis_connection("default")
        cache_key = f"throttle_sliding_window_{self.ident}"
        try:
            oldest = redis_conn.zrange(cache_key, 0, 0, withscores=True)
        except RedisError:
            return None
        if not oldest:
            return 0
        oldest_ts = oldest[0][1]
        return max(0, math.ceil(oldest_ts + self.window_seconds - time.time()))

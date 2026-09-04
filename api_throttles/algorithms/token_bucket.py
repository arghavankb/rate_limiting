import time

import redis
from django_redis import get_redis_connection
from rest_framework.throttling import BaseThrottle

_TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(bucket[1])
local last_updated = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    last_updated = now
end

local elapsed = now - last_updated
if elapsed > 0 then
    tokens = math.min(capacity, tokens + (elapsed * refill_rate))
end

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call('HSET', key, 'tokens', tokens, 'last_updated', now)
redis.call('EXPIRE', key, ttl)

return {allowed, tostring(tokens)}
"""


class TokenBucketThrottle(BaseThrottle):
    capacity = 10  # Max bucket size (max burst)
    refill_rate = 1  # Tokens added per second

    _script = None

    fail_open = True

    def __init__(self):
        super().__init__()
        self._tokens_remaining = None

    def _get_script(self, redis_conn):
        if TokenBucketThrottle._script is None:
            TokenBucketThrottle._script = redis_conn.register_script(_TOKEN_BUCKET_LUA)
        return TokenBucketThrottle._script

    def allow_request(self, request, view):
        if self.capacity <= 0 or self.refill_rate <= 0:
            raise ValueError("capacity and refill_rate must both be positive")

        ident = self.get_ident(request)
        cache_key = f"throttle_token_bucket_{ident}"
        now = time.time()
        ttl = max(1, int((self.capacity / self.refill_rate) * 2))

        try:
            redis_conn = get_redis_connection("default")
            script = self._get_script(redis_conn)
            allowed, tokens_remaining = script(
                keys=[cache_key],
                args=[self.capacity, self.refill_rate, now, ttl],
            )
        except redis.RedisError:
            # Redis unavailable — degrade according to fail_open.
            self._tokens_remaining = None
            return self.fail_open

        self._tokens_remaining = float(tokens_remaining)
        return bool(allowed)

    def wait(self):
        """
        Seconds the client should wait before retrying, used by DRF to set
        the Retry-After header on a throttled (429) response.
        """
        tokens_remaining = getattr(self, "_tokens_remaining", None)
        if tokens_remaining is None:
            return None

        tokens_needed = 1 - tokens_remaining
        if tokens_needed <= 0:
            return None

        return tokens_needed / self.refill_rate

# DRF Redis Rate Limiter

Three rate-limiting algorithms for Django REST Framework, backed by Redis.
Each one is a custom DRF throttle class you attach to a view.

**Stack:** Python 3.12, Django 6.0.7, Django REST Framework 3.17.1, Redis, django-redis 7.0.0, redis-py 8.1.0, python-dotenv

## Algorithms

| Throttle | Default limit | How it works |
| --- | --- | --- |
| `FixedWindowThrottle` | 5 requests / 60 s | One counter per time window (Django cache) |
| `SlidingWindowThrottle` | 5 requests / 60 s | Redis sorted set of request timestamps |
| `TokenBucketThrottle` | Burst of 10, refills 1 token/s | Token bucket updated by an atomic Lua script |

Code lives in `api_throttles/algorithms/`.

## Setup

```bash
pip install -r requirements.txt
docker run -d --name redis -p 6379:6379 redis   # or: redis-server
```

Create a `.env` file in the project root (keep it out of git):

```
SECRET_KEY=change-me
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1
REDIS_URL=redis://127.0.0.1:6379/1
```

| Variable | Required | Default |
| --- | --- | --- |
| `SECRET_KEY` | Yes | - |
| `DEBUG` | No | `False` |
| `ALLOWED_HOSTS` | No | `localhost,127.0.0.1` |
| `REDIS_URL` | No | `redis://127.0.0.1:6379/1` |

Then:

```bash
python manage.py migrate
python manage.py runserver
```

## Configuration

`core/settings.py` already contains what the throttles need:

- **Cache:** the `default` cache is a django-redis cache built from `REDIS_URL`.
  The sliding window and token bucket use it directly through `get_redis_connection`.
- **`NUM_PROXIES`:** set to `0` in `REST_FRAMEWORK`, so DRF ignores the
  client-supplied `X-Forwarded-For` header. Change it to `1` if the app runs
  behind Nginx or a load balancer. Without this setting, a client can bypass
  every limit by sending a fake header.

## Usage

```python
from rest_framework.views import APIView
from api_throttles.algorithms.sliding_window import SlidingWindowThrottle


class PingView(APIView):
    throttle_classes = [SlidingWindowThrottle]
```

Change limits by subclassing: `rate_limit` and `window_seconds` (fixed and
sliding window), or `capacity`, `refill_rate` and `fail_open` (token bucket).

Over the limit, the API returns `429 Too Many Requests` with a `Retry-After` header.

## Good to Know

- Clients are identified by IP address, including logged-in users.
- Counters are per client and per algorithm, not per view or subclass.
- If Redis is down: fixed window returns a 500, sliding window allows the
  request, token bucket follows `fail_open` (default: allow).

## Tests

Tests need a running Redis. They call `flushdb()`, which wipes the whole Redis
database, so run them against a separate one using `REDIS_URL`:

```bash
# Linux / macOS
REDIS_URL=redis://127.0.0.1:6379/15 python manage.py test api_throttles
```

```powershell
# Windows PowerShell
$env:REDIS_URL="redis://127.0.0.1:6379/15"; python manage.py test api_throttles
```

"""
Redis client for application-level data operations.

This is NOT the Celery broker connection (db/0) or result backend (db/1).
This client is for structured application data — specifically the archive
buffer that accumulates raw energy readings before flushing to CSV.

Database allocation:
  db/0  Celery broker   — task queue messages (managed by Celery)
  db/1  Celery backend  — task result storage (managed by Celery)
  db/2  App data        — archive buffer, future: rate limit counters, etc.

Keeping application data on db/2 means:
  - FLUSHDB on db/0 clears only pending tasks, not buffered readings
  - FLUSHDB on db/2 clears only app data, not the task queue
  - Each layer is independently debuggable in redis-cli (SELECT 2 / KEYS *)
"""

import redis

from app.core.config import settings

# Derive db/2 URL from the base REDIS_URL
# rsplit on "/" once from the right to replace the db number safely
_buffer_url = settings.REDIS_URL.rsplit("/", 1)[0] + "/2"

# decode_responses=True means LRANGE returns str, not bytes —
# one less .decode() call everywhere in the tasks
redis_buffer: redis.Redis = redis.from_url(_buffer_url, decode_responses=True)

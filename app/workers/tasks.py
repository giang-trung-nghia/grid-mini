"""
Celery task definitions for grid-mini.

No business logic lives here yet — these stubs verify the worker boots,
connects to Redis, and can execute tasks end-to-end.

Convention used throughout this project:
  - Tasks are thin: validate input, delegate to a service function, return result.
  - Never import FastAPI request/response objects inside tasks — tasks must be
    runnable without a web server context.
  - bind=True gives the task access to `self` for retries and logging with
    the real task ID, which is essential for tracing long-running jobs.
"""

import logging
import time

from celery import Task

from app.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="workers.ping")
def ping(self: Task) -> dict:
    """
    Smoke-test task. Enqueue this from a shell or the /debug/ping endpoint
    to confirm the worker is alive and Redis round-trip works.

    Returns the Celery task ID so you can look it up in the result backend.
    """
    logger.info("ping task received [task_id=%s]", self.request.id)
    return {"status": "pong", "task_id": self.request.id}


@celery_app.task(bind=True, name="workers.slow_ping")
def slow_ping(self: Task, delay: int = 20) -> dict:
    """
    Slow smoke-test task — stays STARTED for `delay` seconds (default 20).

    Use this to observe the full Redis workflow:
      1. Call slow_ping.delay() → message appears in Redis db/0 (PENDING)
      2. Worker picks it up    → status moves to STARTED in Redis db/1
      3. Sleep completes       → status moves to SUCCESS in Redis db/1

    You have the full `delay` seconds to inspect Redis before the task finishes.

    Usage:
      result = slow_ping.delay()        # default 20s window
      result = slow_ping.delay(30)      # 30s window
    """
    logger.info(
        "slow_ping STARTED — sleeping %ds [task_id=%s]", delay, self.request.id
    )
    for elapsed in range(delay):
        time.sleep(1)
        logger.info(
            "slow_ping progress %d/%d [task_id=%s]", elapsed + 1, delay, self.request.id
        )
    logger.info("slow_ping DONE [task_id=%s]", self.request.id)
    return {"status": "pong", "slept_seconds": delay, "task_id": self.request.id}


@celery_app.task(
    bind=True,
    name="workers.process_energy_reading",
    max_retries=3,
    default_retry_delay=5,
)
def process_energy_reading(self: Task, payload: dict) -> dict:
    """
    Ingest a single energy reading into InfluxDB.

    Flow:
      1. Deserialize the dict (came from Redis/JSON) back into an EnergyReading
      2. Call the ingestion service to transform + write to InfluxDB
      3. On transient failure, retry up to 3 times with 5s backoff

    The payload travels as a plain dict through Redis (JSON serializable).
    We re-validate it with EnergyReading.from_dict() so bad data is caught
    before it reaches the DB — not silently stored as garbage.
    """
    from app.models.energy import EnergyReading
    from app.services.ingestion import write_reading

    reading = EnergyReading.from_dict(payload)
    logger.info(
        "process_energy_reading [site=%s, power_kw=%.2f, task_id=%s]",
        reading.site_id,
        reading.power_kw,
        self.request.id,
    )
    try:
        write_reading(reading)
    except Exception as exc:
        logger.warning(
            "InfluxDB write failed, retrying [attempt=%d, error=%s]",
            self.request.retries + 1,
            exc,
        )
        raise self.retry(exc=exc)

    return {
        "site_id": reading.site_id,
        "power_kw": reading.power_kw,
        "timestamp": reading.timestamp.isoformat(),
        "status": "written",
        "task_id": self.request.id,
    }


@celery_app.task(bind=True, name="workers.archive_energy_reading")
def archive_energy_reading(self: Task, payload: dict) -> dict:
    """
    Buffer a single energy reading in Redis (db/2) for batch archival.

    This task does NOT write to disk immediately. It pushes the serialized
    reading onto a Redis list (ARCHIVE_BUFFER_KEY). Disk writes happen in
    bulk when flush_archive_buffer runs — either triggered by Beat on a
    schedule, or immediately when the buffer reaches ARCHIVE_FLUSH_BATCH_SIZE.

    Why buffer in Redis instead of writing directly?
      - Each individual file.append() call is a filesystem syscall with
        its own open/write/close cycle. At 1 reading/sec × 50 sites that's
        50 syscalls/sec for cold storage that nobody is querying in real time.
      - Batching amortizes those syscalls: one flush = one append call
        regardless of how many readings accumulated.
      - Redis RPUSH is sub-millisecond — the task returns almost instantly,
        keeping worker slots free for high-priority ingestion tasks.
      - The buffer survives a worker restart (Redis is persistent);
        individual readings are not lost between flushes.
    """
    import json
    from app.core.config import settings
    from app.db.redis_client import redis_buffer

    logger.info(
        "buffering reading [site=%s, task_id=%s]",
        payload.get("site_id"), self.request.id,
    )

    redis_buffer.rpush(settings.ARCHIVE_BUFFER_KEY, json.dumps(payload))
    buffer_size = redis_buffer.llen(settings.ARCHIVE_BUFFER_KEY)

    logger.debug("archive buffer size: %d", buffer_size)

    # Size-based early flush — don't wait for Beat if buffer is already full
    if buffer_size >= settings.ARCHIVE_FLUSH_BATCH_SIZE:
        logger.info(
            "buffer reached threshold (%d), triggering immediate flush",
            settings.ARCHIVE_FLUSH_BATCH_SIZE,
        )
        flush_archive_buffer.delay()

    return {
        "site_id": payload.get("site_id"),
        "status": "buffered",
        "buffer_size": buffer_size,
        "task_id": self.request.id,
    }


@celery_app.task(bind=True, name="workers.flush_archive_buffer", max_retries=3)
def flush_archive_buffer(self: Task) -> dict:
    """
    Drain the Redis archive buffer and write all pending readings to CSV.

    Called by:
      1. Celery Beat on a fixed schedule (ARCHIVE_FLUSH_INTERVAL seconds)
      2. archive_energy_reading immediately when buffer hits ARCHIVE_FLUSH_BATCH_SIZE

    The drain is atomic at the pipeline level:
      LRANGE returns all items, DEL removes them in the same pipeline round-trip.
      If the flush task crashes after DEL but before writing to disk, those
      readings are lost. For production, use GETDEL + a dead-letter queue or
      move items to a "processing" key and delete only on success.

    For this simulation, pipeline atomicity is sufficient.
    """
    import json
    from app.core.config import settings
    from app.db.redis_client import redis_buffer
    from app.models.energy import EnergyReading
    from app.services.blob_service import blob_service

    # Atomically read all buffered items and clear the buffer in one round-trip
    pipe = redis_buffer.pipeline()
    pipe.lrange(settings.ARCHIVE_BUFFER_KEY, 0, -1)
    pipe.delete(settings.ARCHIVE_BUFFER_KEY)
    raw_records, _ = pipe.execute()

    if not raw_records:
        logger.debug("flush_archive_buffer: buffer empty, nothing to write")
        return {"status": "empty", "count": 0, "task_id": self.request.id}

    logger.info("flushing %d buffered readings to CSV", len(raw_records))

    try:
        readings = [EnergyReading.from_dict(json.loads(r)) for r in raw_records]
        keys = blob_service.archive_many(readings)
    except Exception as exc:
        logger.error("flush failed [error=%s], readings may be lost", exc)
        raise self.retry(exc=exc)

    return {
        "status": "flushed",
        "count": len(readings),
        "files_written": keys,
        "task_id": self.request.id,
    }


@celery_app.task(bind=True, name="workers.simulate_and_ingest")
def simulate_and_ingest(self: Task, site_id: str | None = None) -> dict:
    """
    Generate a fake reading and run the full dual-path pipeline:
      1. Hot path  → InfluxDB (queryable, real-time)
      2. Cold path → local blob storage CSV (raw archive)

    Both writes happen in the same task for the simulation.
    In production, the API would enqueue process_energy_reading and
    archive_energy_reading as two separate tasks so each retries independently.

    Usage:
      simulate_and_ingest.delay()                # random site
      simulate_and_ingest.delay(site_id="site-001")
    """
    from app.simulators.energy import mock_generate_reading
    from app.services.ingestion import write_reading

    reading = mock_generate_reading(site_id)
    logger.info(
        "simulated reading [site=%s, power_kw=%.2f]", reading.site_id, reading.power_kw
    )

    # Hot path — write to InfluxDB synchronously (real-time query must be current)
    write_reading(reading)

    # Cold path — enqueue as a separate task so each path retries independently.
    # archive_energy_reading buffers in Redis; flush_archive_buffer writes the CSV.
    archive_energy_reading.delay(reading.to_dict())

    return {
        "site_id": reading.site_id,
        "power_kw": reading.power_kw,
        "timestamp": reading.timestamp.isoformat(),
        "influxdb": "written",
        "archive": "buffered",
        "task_id": self.request.id,
    }

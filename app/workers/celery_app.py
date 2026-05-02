"""
Celery application factory for grid-mini.

--- Broker vs Backend ---

  Broker (Redis db/0):
    The message bus. FastAPI drops a task message here and returns immediately.
    The worker picks it up asynchronously. Redis works well as a broker because
    it supports the list/pub-sub primitives Celery needs and is very fast for
    short-lived messages.

  Result Backend (Redis db/1):
    Where Celery stores the return value (or error) of a finished task so the
    caller can retrieve it later via AsyncResult. We use a separate Redis DB
    (db/1) to keep task messages and results isolated — makes debugging and
    cache-flushing cleaner.

--- Process model ---

  FastAPI process  →  writes message to Redis broker  →  returns HTTP 202
  Celery worker    →  reads message from Redis         →  executes task
                                                       →  writes result to Redis backend

  These are separate OS processes. FastAPI never blocks waiting for the task.
  The worker can run on a different machine entirely; Redis is the only shared
  dependency between them.

--- Celery Beat (scheduler) ---

  Beat is a third process alongside FastAPI and the worker:

    uvicorn         → handles HTTP
    celery worker   → executes tasks
    celery beat     → fires tasks on a schedule (cron / interval)

  Beat does NOT execute tasks itself. It only enqueues them into Redis at the
  right time. The worker picks them up as normal. This means:
    - Beat can be stopped/restarted without losing tasks already in the queue
    - Worker scale is independent of the schedule

  beat_schedule is defined in conf.update() below.
  Run Beat with:
    celery -A app.workers.celery_app beat --loglevel=info

--- Task autodiscovery ---

  Setting `include` explicitly (rather than relying on autodiscover_tasks) keeps
  the worker boot predictable — no implicit filesystem scanning.
"""

from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown

from app.core.config import settings

celery_app = Celery(
    "grid-mini",
    broker=settings.REDIS_URL,
    # Use a separate Redis DB for results so broker messages and task results
    # don't interfere with each other.
    backend=settings.REDIS_URL.replace("/0", "/1"),
    include=["app.workers.tasks"],
)

@worker_process_init.connect
def init_worker_process(**kwargs):
    """
    Connect InfluxDB once per worker process, not once per task.

    Celery spawns multiple worker processes (one per CPU by default).
    This signal fires inside each child process after it forks — the right
    place to open persistent connections. Opening a connection per task would
    be extremely wasteful (TCP handshake + TLS for every IoT reading).

    This mirrors the FastAPI lifespan pattern: both guarantee one connection
    object per process lifetime.
    """
    from app.db.influx import influx_manager
    influx_manager.connect()


@worker_process_shutdown.connect
def shutdown_worker_process(**kwargs):
    """Flush write buffer and close the connection cleanly on worker exit."""
    from app.db.influx import influx_manager
    influx_manager.disconnect()


celery_app.conf.update(
    # Serialize messages as JSON — human-readable, avoids pickle security risks.
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Completed results expire after 1 hour. Tune this based on whether callers
    # poll for results (longer TTL) or fire-and-forget (shorter TTL).
    result_expires=3600,

    # UTC everywhere — this system ingests energy data with timestamps across
    # time zones; local time in task metadata is a bug waiting to happen.
    timezone="UTC",
    enable_utc=True,

    # Route all tasks to a single default queue for now.
    # Later: split into "ingestion", "processing", "alerting" queues.
    task_default_queue="default",

    # --- Beat schedule ---
    # Beat enqueues these tasks at the defined interval.
    # The worker executes them — Beat itself does nothing but tick and enqueue.
    #
    # Why 5 seconds for a simulation?
    #   Real IoT devices typically push at 1s–60s intervals depending on
    #   the sensor type. 5s gives a visible data stream in InfluxDB without
    #   flooding the worker during development.
    #
    # In production this would be replaced by:
    #   - a real device push endpoint (HTTP/MQTT) that calls .delay() on arrival
    #   - Beat used only for scheduled aggregation jobs (hourly average, daily peak)
    beat_schedule={
        "ingest-energy-every-5s": {
            "task": "workers.simulate_and_ingest",
            "schedule": 5.0,  # seconds
            # site_id=None → simulator picks a random site each tick
            "kwargs": {"site_id": None},
        },
        # Time-based flush: drain the archive buffer every 30 seconds.
        # Acts as a safety net for low-traffic periods where the buffer
        # never reaches ARCHIVE_FLUSH_BATCH_SIZE on its own.
        "flush-archive-buffer-every-30s": {
            "task": "workers.flush_archive_buffer",
            "schedule": 30.0,
        },
    },
)

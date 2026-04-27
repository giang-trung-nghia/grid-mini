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

--- Task autodiscovery ---

  Setting `include` explicitly (rather than relying on autodiscover_tasks) keeps
  the worker boot predictable — no implicit filesystem scanning.
"""

from celery import Celery

from app.core.config import settings

celery_app = Celery(
    "grid-mini",
    broker=settings.REDIS_URL,
    # Use a separate Redis DB for results so broker messages and task results
    # don't interfere with each other.
    backend=settings.REDIS_URL.replace("/0", "/1"),
    include=["app.workers.tasks"],
)

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
)

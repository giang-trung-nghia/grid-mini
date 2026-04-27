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
def process_energy_reading(self: Task, site_id: str, payload: dict) -> dict:
    """
    Placeholder for the real ingestion task.

    Will eventually:
      1. Validate the payload (pydantic schema)
      2. Write the reading to InfluxDB (hot path)
      3. Forward raw data to Azure Blob Storage (cold path)

    The retry config handles transient InfluxDB or network failures without
    losing readings — important for IoT data where gaps corrupt aggregations.
    """
    logger.info(
        "process_energy_reading received [site=%s, task_id=%s]",
        site_id,
        self.request.id,
    )
    # TODO: replace with real ingestion logic
    return {"site_id": site_id, "status": "queued", "task_id": self.request.id}

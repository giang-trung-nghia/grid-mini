"""
InfluxDB 2.x client layer.

InfluxDB is the hot path for all time-series data: power readings from IoT
devices and CAISO-like market prices. Key design decisions:

- A single InfluxDBClient is shared across the app lifetime (opened on
  startup, closed on shutdown) to avoid per-request TCP handshake overhead.

- write_api uses SYNCHRONOUS mode so writes block until InfluxDB acknowledges
  them. Switch to batching WriteOptions for high-throughput ingestion via
  Celery workers.

- query_api uses Flux queries. Always filter by _measurement + time range
  first to leverage InfluxDB's time-series index before filtering on tags.

Tag cardinality reminder:
  tags   = low-cardinality identifiers → site_id, device_type, region
  fields = measured numeric values     → power_kw, price_usd, frequency_hz
  Never use UUIDs or raw timestamps as tags.
"""

import logging
from typing import TYPE_CHECKING

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS

from app.core.config import settings

if TYPE_CHECKING:
    from influxdb_client.client.query_api import QueryApi
    from influxdb_client.client.write_api import WriteApi

logger = logging.getLogger(__name__)


class InfluxClientManager:
    """
    Lifecycle-aware wrapper around InfluxDBClient.

    Call connect() once at app startup and disconnect() at shutdown.
    The FastAPI lifespan handler in main.py owns this lifecycle.
    """

    def __init__(self) -> None:
        self._client: InfluxDBClient | None = None
        self._write_api: "WriteApi | None" = None

    def connect(self) -> None:
        self._client = InfluxDBClient(
            url=settings.INFLUX_URL,
            token=settings.INFLUX_TOKEN,
            org=settings.INFLUX_ORG,
        )
        # Create write_api once and reuse — instantiating per-write is wasteful
        # and loses batching state if you later switch from SYNCHRONOUS.
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        logger.info("InfluxDB client connected to %s", settings.INFLUX_URL)

    def disconnect(self) -> None:
        if self._write_api:
            self._write_api.close()
        if self._client:
            self._client.close()
        logger.info("InfluxDB client disconnected")

    @property
    def write_api(self) -> "WriteApi":
        if self._write_api is None:
            raise RuntimeError("InfluxDB client is not connected. Call connect() first.")
        return self._write_api

    @property
    def query_api(self) -> "QueryApi":
        if self._client is None:
            raise RuntimeError("InfluxDB client is not connected. Call connect() first.")
        return self._client.query_api()

    def check_connection(self) -> bool:
        """Ping InfluxDB. Used by the /health endpoint."""
        try:
            return bool(self._client and self._client.ping())
        except (InfluxDBError, Exception) as exc:
            logger.error("InfluxDB health check failed: %s", exc)
            return False


influx_manager = InfluxClientManager()


def get_influx_write():
    """FastAPI dependency — yields the shared write API."""
    return influx_manager.write_api


def get_influx_query():
    """FastAPI dependency — yields the shared query API."""
    return influx_manager.query_api

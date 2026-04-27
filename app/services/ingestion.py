"""
Energy ingestion service — the core of the data pipeline.

This layer owns two responsibilities:
  1. Transform a raw EnergyReading into an InfluxDB Point (schema mapping)
  2. Write that Point to InfluxDB (I/O)

Keeping these separate from the Celery task and the simulator means each
piece can be tested in isolation:
  - test_ingestion.py can test reading_to_point() with no DB connection
  - test_influx_write.py can mock write_api and test write_reading() logic
  - tasks.py stays thin — it just calls write_reading() and handles retries

--- Why transform at all? ---

Raw IoT data arrives as a simple dict: {"site_id": ..., "power_kw": ..., "timestamp": ...}
InfluxDB does NOT accept raw dicts. It uses a typed Point structure where:
  - measurement name is explicit
  - tags, fields, and timestamp are distinct typed slots
  - timestamp precision must be declared (nanosecond default vs second precision here)

Without this transformation layer, you'd scatter InfluxDB-specific code across
your codebase. With it, the service is the single place where the schema is defined.

--- Tags vs Fields ---

  tag   site_id  → string label, indexed, used in WHERE filters
                   low-cardinality: you have tens of sites, not millions
                   InfluxDB builds a time-series index per unique tag combination

  field power_kw → numeric value, NOT indexed, stored as a float series
                   high-cardinality: changes with every reading (50.0, 87.3, 142.1...)
                   this is the data you aggregate: mean(), max(), last()

--- What goes wrong with incorrect schema ---

  BAD:  .tag("power_kw", str(reading.power_kw))   ← power as a tag
        Every unique float value (50.12, 50.13, ...) creates a new time-series.
        With 3 sites × 1 reading/sec × 100 kW values → millions of series.
        InfluxDB's "high cardinality" problem: index bloat, memory explosion,
        query performance collapses. This is a known production failure mode.

  BAD:  .field("site_id", reading.site_id)         ← site_id as a field
        Fields are not indexed. Filtering by site_id becomes a full table scan
        over every data point. Unusable at scale.
"""

import logging

from influxdb_client import Point

from app.core.config import settings
from app.db.influx import influx_manager
from app.models.energy import EnergyReading

logger = logging.getLogger(__name__)


def mapping_energy_reading_to_influx_point(reading: EnergyReading) -> Point:
    """
    Transform an EnergyReading into an InfluxDB Point.

    Schema:
      measurement : power_usage
      tag         : site_id      (indexed label — used in GROUP BY / WHERE)
      field       : power_kw     (numeric value — what you plot and aggregate)
      precision   : seconds ("s") — IoT readings at 1s resolution is sufficient;
                    nanosecond default wastes storage for this use case.
    """
    return (
        Point("power_usage")
        .tag("site_id", reading.site_id)
        .field("power_kw", reading.power_kw)
        .time(reading.timestamp, write_precision="s")
    )


def write_reading(reading: EnergyReading) -> None:
    """
    Write a single EnergyReading to InfluxDB.

    Called by the Celery task — the task handles retries on failure,
    so this function just writes and raises on error.
    """
    point = mapping_energy_reading_to_influx_point(reading)
    influx_manager.write_api.write(
        bucket=settings.INFLUX_BUCKET,
        org=settings.INFLUX_ORG,
        record=point,
    )
    logger.info(
        "wrote reading to InfluxDB [site=%s, power_kw=%.2f, ts=%s]",
        reading.site_id,
        reading.power_kw,
        reading.timestamp.isoformat(),
    )


def write_batch(readings: list[EnergyReading]) -> None:
    """
    Write multiple readings in one network round-trip.

    Batching is more efficient for backfill scenarios or when Celery processes
    a group of readings that arrived together (e.g. from a message bus burst).
    InfluxDB accepts a list of Points in a single write call.
    """
    points = [mapping_energy_reading_to_influx_point(r) for r in readings]
    influx_manager.write_api.write(
        bucket=settings.INFLUX_BUCKET,
        org=settings.INFLUX_ORG,
        record=points,
    )
    logger.info("wrote batch of %d readings to InfluxDB", len(points))

"""
Power repository — raw InfluxDB access for power_usage data.

This layer has ONE responsibility: talk to InfluxDB and return domain objects.
It knows Flux syntax, InfluxDB connection details, and how to map records to
PowerReadingResponse. Nothing else.

The service layer above it has no idea how the data is fetched — it could be
InfluxDB, TimescaleDB, or a mock returning hardcoded data in tests. That's the
Repository Pattern: the data source is swappable without touching business logic.

--- Why Flux queries must start with range() ---

InfluxDB's TSM storage engine partitions data into time-ordered shards on disk.
Without a time range, there is no entry point into the index — it would scan
every shard ever written. InfluxDB enforces range() as a hard requirement.

Compare to SQL where full table scans are slow but allowed:
  SQL:     SELECT * FROM readings WHERE site_id = 'x'   ← works, just slow
  Flux:    filter without range                          ← rejected at parse time

--- Flux pipeline explained ---

  from(bucket)             source: all data in the bucket
  |> range(start: -Nm)     mandatory: open only the last N-minute shard window
  |> filter(_measurement)  which table inside the bucket (power_usage vs market_price)
  |> filter(_field)        which column (power_kw vs voltage vs frequency)
  |> keep(columns)         drop InfluxDB metadata we don't need in the response
  |> sort(_time)           oldest-first for natural time-series ordering

--- Timestamp precision notes ---

  - We write with precision="s" (seconds) in ingestion.py
  - InfluxDB stores as nanoseconds internally (pads with zeros)
  - Range queries work correctly regardless of write precision
  - get_time() returns a UTC-aware datetime — no timezone conversion needed
  - Danger: if the device sends a local time (UTC+7) without converting to UTC
    before writing, range queries return wrong windows. Always normalize at ingestion.
"""

import logging

from app.core.config import settings
from app.db.influx import influx_manager
from app.models.energy import PowerReadingResponse

logger = logging.getLogger(__name__)


class PowerRepository:
    """
    Encapsulates all InfluxDB read operations for power_usage data.

    Using a class (rather than module-level functions) makes it trivial to
    swap implementations in tests:

        # In tests:
        class MockPowerRepository(PowerRepository):
            def get_recent(self, range_str):
                return [PowerReadingResponse(...)]   # no DB needed
    """

    def get_recent(self, range_str: str) -> list[PowerReadingResponse]:
        """
        Fetch all power_usage readings in the last `range_str` window.

        Args:
            range_str: validated Flux duration — "5m", "1h", "30s", "1d"
                       Caller (service layer) is responsible for validation.

        Returns:
            Records ordered by timestamp ascending (oldest first).
        """
        flux_query = f"""
        from(bucket: "{settings.INFLUX_BUCKET}")
          |> range(start: -{range_str})
          |> filter(fn: (r) => r._measurement == "power_usage")
          |> filter(fn: (r) => r._field == "power_kw")
          |> keep(columns: ["_time", "_value", "site_id"])
          |> sort(columns: ["_time"])
        """

        logger.debug("flux query [range=-%s]: %s", range_str, flux_query.strip())

        tables = influx_manager.query_api.query(flux_query, org=settings.INFLUX_ORG)

        results: list[PowerReadingResponse] = []
        for table in tables:
            for record in table.records:
                results.append(
                    PowerReadingResponse(
                        timestamp=record.get_time(),
                        site_id=record.values.get("site_id", "unknown"),
                        power_kw=float(record.get_value()),
                    )
                )

        logger.debug("repository returned %d records [range=-%s]", len(results), range_str)
        return results


# Module-level singleton — one instance shared across all requests.
# The repository holds no state (connection is on influx_manager), so
# a singleton is safe and avoids unnecessary object creation per request.
power_repository = PowerRepository()

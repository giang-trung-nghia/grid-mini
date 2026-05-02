"""
Blob service — business logic for raw data archival.

Responsibility: decide WHERE and HOW to store raw energy readings,
then delegate the actual I/O to a StorageService implementation.

This layer knows about:
  - EnergyReading domain objects
  - Blob key naming convention (date-partitioned CSV paths)
  - CSV formatting rules

This layer does NOT know about:
  - Whether storage is local disk or Azure Blob
  - Filesystem paths or Azure container names
  - InfluxDB (that's the ingestion service's concern)

--- Why raw data in blob storage AND processed data in InfluxDB? ---

They serve different purposes:

  InfluxDB (hot path):
    - Stores aggregated / queryable time-series points
    - Optimized for range queries and dashboard reads
    - Data expires after retention period (e.g. 90 days)
    - You query it: "what was the mean power for site-001 last hour?"

  Blob Storage (cold path):
    - Stores the original, unmodified readings exactly as received
    - Append-only, never mutated — forensic record
    - Retained indefinitely (cheap storage)
    - Used for: reprocessing, auditing, ML training, regulatory compliance

  If your InfluxDB schema changes (e.g. you add a new field), you can
  reprocess the raw CSV files to backfill the new data. Without the raw
  archive, that historical data is gone forever.

--- Blob key convention ---

  "2026-04-27/power_usage_2026-04-27.csv"
   └── date dir ──┘ └──── filename ─────┘

  One CSV per calendar day. Readings are appended as they arrive.
  The date comes from the reading's own timestamp (device time), NOT
  the wall clock at the time of archival. This matters: if a reading
  arrives late (network delay), it belongs to the day it was measured,
  not the day it was stored.

--- CSV format ---

  timestamp,site_id,power_kw
  2026-04-27T20:00:00+00:00,site-001,87.4
  2026-04-27T20:00:05+00:00,site-002,112.1
"""

import csv
import io
import logging

from app.models.energy import EnergyReading
from app.storage.base import StorageService

logger = logging.getLogger(__name__)

_CSV_HEADER = ["timestamp", "site_id", "power_kw"]


class BlobService:
    """
    Inject StorageService (not hardcoded to LocalStorage) so the same
    BlobService works in production (Azure) and tests (MockStorage).
    """

    def __init__(self, storage: StorageService) -> None:
        self._storage = storage

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def archive_reading(self, reading: EnergyReading) -> str:
        """
        Append a single reading to the daily CSV file for its site.

        Returns the blob key where the data was stored.
        """
        key = self._build_key(reading)
        row = self._to_csv_row(reading)

        if not self._storage.exists(key):
            # First write of the day — create file with header
            self._storage.save(key, self._header_line() + row)
        else:
            self._storage.append(key, row)

        logger.info(
            "archived reading [key=%s, site=%s, power_kw=%.2f]",
            key, reading.site_id, reading.power_kw,
        )
        return key

    def archive_batch(self, readings: list[EnergyReading]) -> str:
        """
        Write multiple readings in one I/O operation.

        More efficient than calling archive_reading() in a loop — all rows
        are serialized into one string and written in a single storage call.
        Assumes all readings belong to the same calendar day.

        Returns the blob key where the batch was stored.
        """
        if not readings:
            raise ValueError("archive_batch called with empty readings list")

        key = self._build_key(readings[0])

        # Check existence once and cache the result — avoids a race condition
        # where exists() returns False, file gets created by another process,
        # and save() then silently overwrites it.
        file_exists = self._storage.exists(key)

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")

        if not file_exists:
            writer.writerow(_CSV_HEADER)

        for r in readings:
            writer.writerow([r.timestamp.isoformat(), r.site_id, r.power_kw])

        content = buf.getvalue()

        if not file_exists:
            self._storage.save(key, content)
        else:
            self._storage.append(key, content)

        logger.info("archived batch [key=%s, count=%d]", key, len(readings))
        return key

    def archive_many(self, readings: list[EnergyReading]) -> list[str]:
        """
        Archive readings that may span multiple calendar days.

        Used by the flush task which drains the Redis buffer — readings
        accumulated over a 30-second window could straddle midnight if the
        flush runs just after 00:00 UTC.

        Groups by date and calls archive_batch() per group so each day's
        CSV gets the right rows.

        Returns the list of blob keys written (one per distinct date).
        """
        if not readings:
            return []

        from itertools import groupby

        sorted_readings = sorted(readings, key=lambda r: r.timestamp.date())
        keys = []
        for _date, group_iter in groupby(
            sorted_readings, key=lambda r: r.timestamp.date()
        ):
            key = self.archive_batch(list(group_iter))
            keys.append(key)

        logger.info(
            "archive_many: %d readings → %d file(s)", len(readings), len(keys)
        )
        return keys

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_key(self, reading: EnergyReading) -> str:
        """
        Build the blob key from the reading's own timestamp.
        Using the measurement timestamp (not now()) ensures a late-arriving
        reading is filed under the day it was actually measured.
        """
        date_str = reading.timestamp.strftime("%Y-%m-%d")
        return f"{date_str}/power_usage_{date_str}.csv"

    def _header_line(self) -> str:
        return ",".join(_CSV_HEADER) + "\n"

    def _to_csv_row(self, reading: EnergyReading) -> str:
        return f"{reading.timestamp.isoformat()},{reading.site_id},{reading.power_kw}\n"


# ------------------------------------------------------------------
# Singleton wired to LocalStorage for development
# Swap the StorageService argument to switch backends.
# ------------------------------------------------------------------

def _make_blob_service() -> BlobService:
    from app.core.config import settings
    from app.storage.local import LocalStorage
    return BlobService(LocalStorage(base_dir=settings.LOCAL_STORAGE_PATH))


blob_service: BlobService = _make_blob_service()

"""
Local filesystem implementation of StorageService.

Mirrors Azure Blob Storage behaviour using the local disk:
  - blob_key  →  relative path within base_dir
  - container →  base_dir (LOCAL_STORAGE_PATH from config)

Directory structure on disk:
  data/raw/
    2026-04-27/
      power_usage_2026-04-27.csv
    2026-04-28/
      power_usage_2026-04-28.csv

The date-partitioned layout maps directly to how Azure organizes blobs with
virtual directories (blob name prefix). When migrating to Azure, the blob key
"2026-04-27/power_usage_2026-04-27.csv" becomes the blob name verbatim.
"""

import logging
from pathlib import Path

from app.storage.base import StorageService

logger = logging.getLogger(__name__)


class LocalStorage(StorageService):
    def __init__(self, base_dir: str) -> None:
        """
        Args:
            base_dir: Root directory for all stored files.
                      Passed in from config (LOCAL_STORAGE_PATH).
                      Using dependency injection here (not reading config directly)
                      keeps LocalStorage independently testable with any base_dir.
        """
        self._base = Path(base_dir)

    def _resolve(self, blob_key: str) -> Path:
        """Convert a blob key to an absolute filesystem path."""
        # Use forward-slash split to handle the key platform-independently.
        return self._base.joinpath(*blob_key.split("/"))

    def save(self, blob_key: str, content: str) -> None:
        path = self._resolve(blob_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        logger.debug("saved [key=%s, bytes=%d]", blob_key, len(content))

    def append(self, blob_key: str, content: str) -> None:
        path = self._resolve(blob_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(content)
        logger.debug("appended [key=%s, bytes=%d]", blob_key, len(content))

    def exists(self, blob_key: str) -> bool:
        return self._resolve(blob_key).exists()

    def read(self, blob_key: str) -> str:
        path = self._resolve(blob_key)
        if not path.exists():
            raise FileNotFoundError(f"Blob not found: {blob_key}")
        return path.read_text(encoding="utf-8")

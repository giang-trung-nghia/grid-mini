"""
Abstract storage interface — the contract every storage backend must fulfill.

Why an abstract base class instead of concrete calls to local filesystem?

The answer is replaceability. Today we write to disk. Tomorrow we write to
Azure Blob Storage. The BlobService (business layer above this) calls
storage.save(), storage.append(), storage.exists() — it has no idea whether
those calls hit a local Path, an Azure container, or an S3 bucket.

Migration path:
  LocalStorage     → development, CI, single-machine deployments
  AzureBlobStorage → production (swap one line in blob_service.py)
  MockStorage      → unit tests (in-memory dict, no filesystem)

This is the same "dependency inversion" principle used in the repository layer:
depend on an abstraction, not a concrete implementation.

--- Blob key convention ---

All methods use a `blob_key` string as the file identifier:
  "2026-04-27/power_usage_2026-04-27.csv"

LocalStorage prepends the LOCAL_STORAGE_PATH base directory to build the
full filesystem path. Azure Blob Storage would use the blob_key directly
as the blob name within a container. The same key works for both — no
translation needed at migration time.
"""

from abc import ABC, abstractmethod


class StorageService(ABC):
    """
    Interface for blob-style storage backends.

    All paths use forward-slash notation regardless of OS — the
    concrete implementation handles platform differences.
    """

    @abstractmethod
    def save(self, blob_key: str, content: str) -> None:
        """
        Create or overwrite a file/blob with the given content.

        Used when writing a fresh daily CSV (the header write).
        In Azure: PutBlockBlob (overwrites completely).
        In local: open(path, 'w').
        """
        ...

    @abstractmethod
    def append(self, blob_key: str, content: str) -> None:
        """
        Append text to an existing file/blob. Creates it if not present.

        Used when adding rows to an existing daily CSV.
        In Azure: requires Append Blob type — different from standard blobs.
        In local: open(path, 'a').

        Thread safety note: on Linux, single appends < 4 KB are atomic on
        most filesystems. On Windows or for larger writes, use a lock or
        write separate files per task and compact periodically.
        """
        ...

    @abstractmethod
    def exists(self, blob_key: str) -> bool:
        """Return True if the blob/file already exists."""
        ...

    @abstractmethod
    def read(self, blob_key: str) -> str:
        """Read and return the full text content of a blob/file."""
        ...

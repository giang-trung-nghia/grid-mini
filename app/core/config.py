from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- SQL Server (metadata: sites, devices, contracts) ---
    SQL_SERVER_HOST: str = "localhost"
    SQL_SERVER_PORT: int = 1433
    SQL_SERVER_USER: str = "sa"
    SQL_SERVER_PASSWORD: str = ""
    SQL_SERVER_DB: str = "energy_db"

    # --- InfluxDB (time-series: power readings, market prices) ---
    INFLUX_URL: str = "http://localhost:8086"
    INFLUX_TOKEN: str = ""
    INFLUX_ORG: str = "energy-org"
    INFLUX_BUCKET: str = "energy"

    # --- Redis (Celery broker + result backend) ---
    REDIS_URL: str = "redis://localhost:6379/0"

    # --- Local storage (simulates Azure Blob Storage) ---
    # In production: swap BlobService's StorageService to AzureBlobStorage.
    # The blob key format ("YYYY-MM-DD/power_usage_YYYY-MM-DD.csv") stays the same.
    LOCAL_STORAGE_PATH: str = "data/raw"

    # --- Archive buffer (Redis db/2) ---
    # Readings accumulate in Redis before being flushed to CSV in batches.
    # ARCHIVE_FLUSH_BATCH_SIZE: flush immediately when buffer reaches this size.
    # ARCHIVE_FLUSH_INTERVAL:   Beat also flushes on a fixed schedule (seconds)
    #                           so readings don't wait forever in small-batch scenarios.
    ARCHIVE_BUFFER_KEY: str = "archive:buffer"
    ARCHIVE_FLUSH_BATCH_SIZE: int = 20
    ARCHIVE_FLUSH_INTERVAL: int = 30

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()

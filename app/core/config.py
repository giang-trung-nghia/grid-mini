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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()

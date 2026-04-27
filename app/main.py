from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db.influx import influx_manager
from app.db.sql import check_connection as sql_check
from app.routers import power


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    influx_manager.connect()
    yield
    # --- shutdown ---
    influx_manager.disconnect()


app = FastAPI(
    title="grid-mini",
    description="Mini energy management system — GridBeyond-inspired",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(power.router)


@app.get("/")
def root():
    return {"message": "grid-mini energy system online"}


@app.get("/health")
def health():
    """
    Shallow connectivity check for both data stores.
    Returns 200 even when a store is unreachable so the app stays up;
    the `healthy` flags let monitoring systems detect partial failures.
    """
    sql_ok = sql_check()
    influx_ok = influx_manager.check_connection()
    return {
        "status": "ok" if (sql_ok and influx_ok) else "degraded",
        "stores": {
            "sqlserver": "up" if sql_ok else "down",
            "influxdb": "up" if influx_ok else "down",
        },
    }

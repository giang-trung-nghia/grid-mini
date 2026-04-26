"""
SQL Server client layer using SQLAlchemy (sync).

SQL Server holds relational metadata — sites, devices, contracts — not
time-series data. Queries here are infrequent and transactional, so a
synchronous engine with connection pooling is the right fit. Async MSSQL
(aioodbc) adds Windows ODBC complexity with little gain at this scale.

Dependency injection via get_db() keeps sessions properly scoped to a
single request and guarantees cleanup even on errors.
"""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.config import settings

logger = logging.getLogger(__name__)


def _build_url() -> URL:
    # URL.create handles special characters in passwords (e.g. '@', '#')
    # without manual percent-encoding — critical for SA_PASSWORD values.
    return URL.create(
        drivername="mssql+pyodbc",
        username=settings.SQL_SERVER_USER,
        password=settings.SQL_SERVER_PASSWORD,
        host=settings.SQL_SERVER_HOST,
        port=settings.SQL_SERVER_PORT,
        database=settings.SQL_SERVER_DB,
        query={"driver": "ODBC Driver 17 for SQL Server"},
    )


engine = create_engine(
    _build_url(),
    pool_pre_ping=True,   # drop stale connections before use
    pool_size=5,
    max_overflow=10,
    echo=False,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""
    pass


def get_db():
    """FastAPI dependency — yields a scoped DB session per request."""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_connection() -> bool:
    """Ping SQL Server. Used by the /health endpoint at startup."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as exc:
        logger.error("SQL Server connection failed: %s", exc)
        return False

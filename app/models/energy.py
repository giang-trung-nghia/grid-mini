from datetime import datetime

from pydantic import BaseModel, Field


class PowerReadingResponse(BaseModel):
    """API response schema for a single power reading returned from InfluxDB."""

    timestamp: datetime = Field(..., description="UTC timestamp of the reading")
    site_id: str = Field(..., description="Site that produced the reading")
    power_kw: float = Field(..., description="Power reading in kilowatts")


class EnergyReading(BaseModel):
    """
    Canonical representation of a single IoT energy reading.

    This is the contract between three layers:
      - Simulator  → produces EnergyReading objects
      - Service    → transforms EnergyReading into an InfluxDB Point
      - Celery task → receives EnergyReading serialized as dict over Redis

    Using a Pydantic model (rather than raw dicts) means validation happens
    at the boundary — bad data from a device is rejected before it ever
    reaches the database.
    """

    site_id: str = Field(..., description="Unique identifier for the energy site")
    power_kw: float = Field(..., ge=0, description="Power reading in kilowatts")
    timestamp: datetime = Field(..., description="UTC timestamp of the reading")

    def to_dict(self) -> dict:
        """Serialize to a JSON-safe dict for passing through Celery/Redis."""
        return {
            "site_id": self.site_id,
            "power_kw": self.power_kw,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "EnergyReading":
        """Deserialize from the dict that arrives inside a Celery task."""
        return cls(**data)

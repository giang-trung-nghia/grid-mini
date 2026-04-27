"""
Fake IoT energy data generator.

In production, this layer is replaced by real device telemetry arriving via
MQTT, HTTP push, or a message bus. The simulator produces the exact same
EnergyReading schema that real devices would send — so the ingestion pipeline
doesn't care whether the source is real or simulated.

Power range 50–150 kW mirrors a realistic small commercial site:
  - 50 kW  ~ off-peak / night
  - 150 kW ~ peak daytime load
"""

import random
from datetime import datetime, timezone

from app.models.energy import EnergyReading

SIMULATED_SITES = ["site-001", "site-002", "site-003"]


def mock_generate_reading(site_id: str | None = None) -> EnergyReading:
    """
    Generate a single fake energy reading for the given site.
    If no site_id is given, one is picked at random from SIMULATED_SITES.
    """
    return EnergyReading(
        site_id=site_id or random.choice(SIMULATED_SITES),
        power_kw=round(random.uniform(50.0, 150.0), 2),
        timestamp=datetime.now(timezone.utc),
    )


def generate_batch(n: int, site_id: str | None = None) -> list[EnergyReading]:
    """
    Generate n readings in sequence.
    Simulates a burst of device telemetry or a replay of historical data.
    """
    return [mock_generate_reading(site_id) for _ in range(n)]

"""
Power service — business logic for energy read operations.

This layer sits between the router and the repository:

  Router          HTTP concerns only (parse params, raise HTTPException)
    ↓
  PowerService    business rules (validate range, could add: alert thresholds,
    ↓             site-level access control, result enrichment)
  PowerRepository raw InfluxDB queries, result mapping

The service never imports from fastapi (no HTTPException, no Request).
It raises plain Python exceptions — the router translates them to HTTP errors.
This keeps the service testable without a running web server.

--- Why separate service from repository? ---

Right now the service is thin — it validates range and calls the repository.
But as the system grows, business logic accumulates here:

  - Check if site_id belongs to the authenticated user
  - Compute peak demand over the queried window
  - Flag readings that exceed the contracted power threshold
  - Trigger an alert task if power > 140 kW for > 3 consecutive readings

None of that logic belongs in the repository (which only fetches data) or
the router (which only handles HTTP). The service is the right place.
"""

import logging
import re

from app.models.energy import PowerReadingResponse
from app.repositories.power_repository import power_repository

logger = logging.getLogger(__name__)

_VALID_RANGE_PATTERN = re.compile(r"^\d+[smhd]$")


class InvalidRangeError(ValueError):
    """Raised when the range string is not a valid Flux duration."""
    pass


class PowerService:
    def get_recent_readings(self, range_str: str) -> list[PowerReadingResponse]:
        """
        Return power readings for the last `range_str` window.

        Validates the range before passing it to the repository — the
        repository trusts that its inputs are already validated.

        Raises:
            InvalidRangeError: if range_str is not a valid Flux duration.
        """
        if not re.match(_VALID_RANGE_PATTERN, range_str):
            raise InvalidRangeError(
                f"Invalid range '{range_str}'. "
                "Use a positive integer followed by s, m, h, or d. "
                "Examples: 5m, 1h, 30s, 1d"
            )

        readings = power_repository.get_recent(range_str)
        logger.info(
            "get_recent_readings [range=-%s] → %d records", range_str, len(readings)
        )
        return readings


power_service = PowerService()

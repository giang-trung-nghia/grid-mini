from fastapi import APIRouter, HTTPException, Query

from app.models.energy import PowerReadingResponse
from app.services.power_service import InvalidRangeError, power_service

router = APIRouter(prefix="/power", tags=["power"])


@router.get("", response_model=list[PowerReadingResponse])
def get_power_readings(
    range: str = Query(
        default="5m",
        description=(
            "Flux duration string for the lookback window. "
            "Supported units: s (seconds), m (minutes), h (hours), d (days). "
            "Examples: 5m, 1h, 30s, 1d"
        ),
        examples={"5 minutes": {"value": "5m"}, "1 hour": {"value": "1h"}},
    ),
):
    """
    Return all power_usage readings from the last `range` window.

    The router's ONLY jobs:
      1. Parse the HTTP query parameter
      2. Call the service
      3. Translate domain exceptions → HTTP errors

    No Flux syntax, no business rules, no InfluxDB imports live here.
    """
    try:
        return power_service.get_recent_readings(range_str=range)
    except InvalidRangeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

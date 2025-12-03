from datetime import datetime
from typing import Any, Dict, Optional

import requests

from config import EsiosConfig, HEADERS


def iso_with_z(dt: datetime) -> str:
    """Format a naive or offset-aware datetime as ISO 8601 with Z suffix."""
    if dt.tzinfo is None:
        return dt.isoformat() + "Z"
    return dt.astimezone(tz=None).isoformat().replace("+00:00", "Z")


def get_indicator_data(
    indicator_id: int,
    start: str,
    end: str,
    time_trunc: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    Fetch raw indicator data from ESIOS.

    - ``start`` and ``end`` must be ISO 8601 strings with a timezone (e.g. ``2025-01-01T00:00:00Z``).
    - ``time_trunc`` can be ``hour`` or ``quarter`` (15-minute), or None for native resolution.
    """
    cfg = EsiosConfig()
    url = f"{cfg.base_url}/indicators/{indicator_id}"

    params: Dict[str, Any] = {
        "start_date": start,
        "end_date": end,
    }
    if time_trunc:
        params["time_trunc"] = time_trunc

    client = session or requests
    resp = client.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json()



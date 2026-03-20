"""
Google Maps commute enrichment using the Routes API v2.

Uses Routes API v2 (computeRoutes) with computeAlternativeRoutes=true so all
available transit routes are returned and the shortest journey time is selected.
This consistently finds faster options than Distance Matrix or Directions best-only,
which both return Google's "preferred" route (not necessarily the quickest).

One API call per property per destination (Routes API doesn't support batching).
Arrival times are computed dynamically for the next Monday from today using the
Europe/London timezone (handles BST/GMT automatically).

Destinations:
  - School : (51.41188, -0.29607)  arrive by 08:30
  - Office : (51.51922, -0.09738)  arrive by 10:00
"""

import logging
import urllib.request
import json
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
from typing import Optional

logger = logging.getLogger("commute")

SCHOOL_LAT, SCHOOL_LNG = 51.41188, -0.29607
OFFICE_LAT, OFFICE_LNG = 51.51922, -0.09738

ROUTES_URL = "https://routes.googleapis.com/directions/v2:computeRoutes"
FIELD_MASK = "routes.duration,routes.distanceMeters,routes.localizedValues"

_LONDON = ZoneInfo("Europe/London")


def _next_monday() -> date:
    """Return the next Monday (never today, even if today is Monday)."""
    today = datetime.now(_LONDON).date()
    days_until_monday = (7 - today.weekday()) % 7 or 7  # always 1–7
    return today + timedelta(days=days_until_monday)


def _arrival_ts(target_date: date, hour: int, minute: int) -> int:
    """Unix timestamp for a given date and local time in Europe/London."""
    dt = datetime(target_date.year, target_date.month, target_date.day,
                  hour, minute, 0, tzinfo=_LONDON)
    return int(dt.timestamp())


def _arrival_rfc3339(ts: int) -> str:
    """Convert Unix timestamp to RFC3339 UTC string required by Routes API."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _call_routes(origin_lat: float, origin_lng: float,
                 dest_lat: float, dest_lng: float,
                 arrival_time_rfc3339: str, api_key: str) -> dict:
    """Single Routes API v2 call with alternatives enabled."""
    body = {
        "origin":      {"location": {"latLng": {"latitude": origin_lat,  "longitude": origin_lng}}},
        "destination": {"location": {"latLng": {"latitude": dest_lat,    "longitude": dest_lng}}},
        "travelMode":  "TRANSIT",
        "arrivalTime": arrival_time_rfc3339,
        "computeAlternativeRoutes": True,
    }
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        ROUTES_URL, data=data, method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _shortest_route(data: dict) -> tuple[Optional[int], str, Optional[float]]:
    """
    Pick the shortest route from a Routes API response.
    Returns (seconds, human_text, distance_km).
    Uses Google's localizedValues.duration.text for the human-readable string.
    """
    best_secs: Optional[int] = None
    best_text: str = ""
    best_km: Optional[float] = None

    for route in data.get("routes", []):
        dur_str = route.get("duration", "")           # e.g. "4018s"
        secs = int(dur_str.rstrip("s")) if dur_str else None
        if secs is None:
            continue
        if best_secs is None or secs < best_secs:
            best_secs = secs
            best_text = (route.get("localizedValues", {})
                              .get("duration", {})
                              .get("text", ""))
            dist_m = route.get("distanceMeters")
            best_km = round(dist_m / 1000, 2) if dist_m else None

    return best_secs, best_text, best_km


def enrich_commutes(properties: list, api_key: str) -> int:
    """
    Enrich properties in-place with school/office commute data.

    Uses Routes API v2 with computeAlternativeRoutes=true; picks the shortest.
    Arrival times target the next Monday from today (Europe/London timezone).
    Skips properties that already have commute data or no coordinates.
    Returns count of properties updated.
    """
    to_update = [
        p for p in properties
        if p.latitude and p.longitude and p.school_commute_seconds is None
    ]

    if not to_update:
        logger.info("Commute: all properties already enriched, nothing to do")
        return 0

    monday = _next_monday()
    school_ts = _arrival_ts(monday, 8, 30)
    office_ts = _arrival_ts(monday, 10, 0)
    school_rfc = _arrival_rfc3339(school_ts)
    office_rfc = _arrival_rfc3339(office_ts)

    logger.info(
        "Commute: enriching %d properties (skipping %d already done) — "
        "arrival Monday %s (school 08:30, office 10:00 London time) [Routes API v2 + alternatives]",
        len(to_update), len(properties) - len(to_update), monday.isoformat(),
    )

    updated = 0
    total = len(to_update)
    for i, prop in enumerate(to_update, 1):
        lat, lng = float(prop.latitude), float(prop.longitude)

        # --- School ---
        try:
            data = _call_routes(lat, lng, SCHOOL_LAT, SCHOOL_LNG, school_rfc, api_key)
            n = len(data.get("routes", []))
            secs, text, km = _shortest_route(data)
            prop.school_commute_seconds = secs
            prop.school_commute_text = text
            prop.school_distance_km = km
            logger.debug("School [%d/%d] %d alternatives, best=%s", i, total, n, text)
        except Exception as e:
            logger.error("School [%d/%d] %s: %s", i, total, prop.address[:35], e)

        # --- Office ---
        try:
            data = _call_routes(lat, lng, OFFICE_LAT, OFFICE_LNG, office_rfc, api_key)
            n = len(data.get("routes", []))
            secs, text, km = _shortest_route(data)
            prop.office_commute_seconds = secs
            prop.office_commute_text = text
            prop.office_distance_km = km
            logger.debug("Office [%d/%d] %d alternatives, best=%s", i, total, n, text)
        except Exception as e:
            logger.error("Office [%d/%d] %s: %s", i, total, prop.address[:35], e)

        updated += 1
        if i % 25 == 0 or i == total:
            logger.info("Commute [%d/%d] %s — school=%s office=%s",
                        i, total, prop.address[:40],
                        prop.school_commute_text, prop.office_commute_text)

    return updated

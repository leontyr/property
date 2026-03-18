"""
Google Maps Distance Matrix commute enrichment.

Calls the Distance Matrix API in batches (50 origins × 2 destinations = 100 elements
per request, the API maximum). Two separate requests are made — one per destination —
because the target arrival times differ.

Destinations:
  - School : (51.41188, -0.29607)  arrive by 08:30 BST on 6 Apr 2026
  - Office : (51.51922, -0.09738)  arrive by 10:00 BST on 6 Apr 2026
"""

import logging
import urllib.parse
import urllib.request
import json
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger("commute")

# ----- Destinations --------------------------------------------------------
SCHOOL_LATLNG = "51.41188,-0.29607"
OFFICE_LATLNG = "51.51922,-0.09738"

# ----- Target arrival times ------------------------------------------------
# April 6, 2026 at 08:30 and 10:00 BST (UTC+1)
_BST = timezone(timedelta(hours=1))
SCHOOL_ARRIVAL_TS = int(datetime(2026, 4, 6, 8, 30, 0, tzinfo=_BST).timestamp())
OFFICE_ARRIVAL_TS = int(datetime(2026, 4, 6, 10, 0, 0, tzinfo=_BST).timestamp())

GMAPS_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"
BATCH_SIZE = 25  # Distance Matrix API allows max 25 origins per request


# ---------------------------------------------------------------------------

def _call_api(origins: list[str], destination: str, arrival_time: int, api_key: str) -> dict:
    """Single Distance Matrix API call. Returns the full JSON response."""
    params = {
        "origins": "|".join(origins),
        "destinations": destination,
        "mode": "transit",
        "arrival_time": arrival_time,
        "key": api_key,
    }
    url = GMAPS_URL + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _parse_element(el: dict) -> tuple[Optional[int], str, Optional[float]]:
    """Extract (seconds, text, distance_km) from a single matrix element."""
    if el.get("status") != "OK":
        logger.debug("Element status: %s", el.get("status"))
        return None, "", None
    duration = el.get("duration", {})
    distance = el.get("distance", {})
    seconds = duration.get("value")
    text = duration.get("text", "")
    km = round(distance.get("value", 0) / 1000, 2) if distance.get("value") else None
    return seconds, text, km


def enrich_commutes(properties: list, api_key: str) -> int:
    """
    Enrich properties in-place with school/office commute data.

    Skips properties that already have commute data or no coordinates.
    Returns count of properties updated.
    """
    # Only process properties that have lat/lng but no commute data yet
    to_update = [
        p for p in properties
        if p.latitude and p.longitude and p.school_commute_seconds is None
    ]

    if not to_update:
        logger.info("Commute: all properties already enriched, nothing to do")
        return 0

    logger.info("Commute: enriching %d properties (skipping %d already done)",
                len(to_update), len(properties) - len(to_update))

    updated = 0
    for i in range(0, len(to_update), BATCH_SIZE):
        batch = to_update[i:i + BATCH_SIZE]
        origins = [f"{p.latitude},{p.longitude}" for p in batch]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(to_update) + BATCH_SIZE - 1) // BATCH_SIZE

        # --- School ---
        logger.info("Commute batch %d/%d: school (%d properties)", batch_num, total_batches, len(batch))
        try:
            resp = _call_api(origins, SCHOOL_LATLNG, SCHOOL_ARRIVAL_TS, api_key)
            if resp.get("status") != "OK":
                logger.warning("School batch %d API error: %s", batch_num, resp.get("status"))
            else:
                for prop, row in zip(batch, resp["rows"]):
                    el = row["elements"][0]
                    secs, text, km = _parse_element(el)
                    prop.school_commute_seconds = secs
                    prop.school_commute_text = text
                    prop.school_distance_km = km
        except Exception as e:
            logger.error("School batch %d failed: %s", batch_num, e)

        # --- Office ---
        logger.info("Commute batch %d/%d: office (%d properties)", batch_num, total_batches, len(batch))
        try:
            resp = _call_api(origins, OFFICE_LATLNG, OFFICE_ARRIVAL_TS, api_key)
            if resp.get("status") != "OK":
                logger.warning("Office batch %d API error: %s", batch_num, resp.get("status"))
            else:
                for prop, row in zip(batch, resp["rows"]):
                    el = row["elements"][0]
                    secs, text, km = _parse_element(el)
                    prop.office_commute_seconds = secs
                    prop.office_commute_text = text
                    prop.office_distance_km = km
        except Exception as e:
            logger.error("Office batch %d failed: %s", batch_num, e)

        updated += len(batch)
        logger.info(
            "Commute batch %d/%d done — sample: %s school=%s office=%s",
            batch_num, total_batches,
            batch[0].address[:40],
            batch[0].school_commute_text,
            batch[0].office_commute_text,
        )

    return updated

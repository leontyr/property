"""
Parse Zoopla search results page RSC payload.

Zoopla uses Next.js App Router. Property data is in:
  self.__next_f.push([1, "..."]) scripts → RSC wire format

The listing array is at key: regularListingsFormatted
Confirmed field names from live page:
  listingId, priceUnformatted, features[{iconId, content}],
  address, pos.lat/lng, listingUris.detail, lastPublishedDate,
  summaryDescription, propertyType, tags[{content}]
"""
import json
import logging
import re

logger = logging.getLogger(__name__)


def _find_json_fragment(rsc_text: str, key: str) -> str | None:
    """
    Find the first occurrence of a JSON array/object containing `key` in the RSC text.
    Returns the raw JSON string or None.
    """
    idx = rsc_text.find(f'"{key}"')
    if idx == -1:
        return None
    # Scan backwards to find the start of the enclosing JSON object/array
    for start in range(idx, max(0, idx - 10000), -1):
        if rsc_text[start] in ('{', '['):
            break
    # Find matching close bracket
    open_char = rsc_text[start]
    close_char = '}' if open_char == '{' else ']'
    depth = 0
    for end in range(start, min(len(rsc_text), start + 50000)):
        if rsc_text[end] == open_char:
            depth += 1
        elif rsc_text[end] == close_char:
            depth -= 1
            if depth == 0:
                return rsc_text[start:end + 1]
    return None


def parse_listings(rsc_text: str) -> list[dict]:
    """
    Extract listing dicts from RSC payload.
    Returns raw listing list from regularListingsFormatted.
    """
    # Find the array starting at regularListingsFormatted
    idx = rsc_text.find('"regularListingsFormatted":[')
    if idx == -1:
        raise ValueError(
            "Could not find 'regularListingsFormatted' in RSC payload. "
            "Inspect samples/search_raw.txt to debug."
        )
    array_start = rsc_text.index('[', idx)
    # Find matching ]
    depth = 0
    for i in range(array_start, min(len(rsc_text), array_start + 200000)):
        if rsc_text[i] == '[':
            depth += 1
        elif rsc_text[i] == ']':
            depth -= 1
            if depth == 0:
                array_end = i + 1
                break
    else:
        raise ValueError("Could not find end of regularListingsFormatted array")

    listings = json.loads(rsc_text[array_start:array_end])
    logger.info("Found %d listings in RSC payload", len(listings))
    return listings


def parse_pagination(rsc_text: str, html: str = "") -> dict:
    """
    Extract pagination info from RSC or __ZAD_TARGETING__ JSON.
    Returns {current_page, total_pages, total_results}.
    """
    # Try __ZAD_TARGETING__ for total_results
    total = 0
    zad_match = re.search(r'"search_results_count"\s*:\s*"(\d+)"', rsc_text + html)
    if zad_match:
        total = int(zad_match.group(1))

    # Try to find pagination object in RSC
    for key in ('"totalPages"', '"pageCount"', '"total_pages"'):
        m = re.search(key + r'\s*:\s*(\d+)', rsc_text)
        if m:
            return {"current_page": 1, "total_pages": int(m.group(1)), "total_results": total}

    # Estimate total pages from total results (25 per page default)
    per_page = 25
    total_pages = max(1, (total + per_page - 1) // per_page) if total else 1

    logger.debug("Pagination: total_results=%d, total_pages=%d", total, total_pages)
    return {"current_page": 1, "total_pages": total_pages, "total_results": total}


def _parse_price(price_raw) -> int | None:
    if price_raw is None:
        return None
    if isinstance(price_raw, (int, float)):
        return int(price_raw)
    if isinstance(price_raw, str):
        cleaned = re.sub(r'[£,\s]', '', price_raw).split()[0] if price_raw.strip() else ''
        try:
            return int(cleaned)
        except ValueError:
            return None
    return None


def extract_listing_summary(raw: dict) -> dict:
    """
    Extract summary fields from a single raw listing dict.
    Field names confirmed from live Zoopla page (2024/2025).
    """
    # Property ID
    property_id = str(raw.get("listingId", ""))

    # Price — priceUnformatted is the clean integer
    listing_price = _parse_price(raw.get("priceUnformatted") or raw.get("price"))

    # Beds/baths — in features array with iconId "bed"/"bath"
    beds = None
    baths = None
    for feature in raw.get("features", []):
        icon = feature.get("iconId", "")
        content = feature.get("content")
        if icon == "bed":
            beds = int(content) if content is not None else None
        elif icon == "bath":
            baths = int(content) if content is not None else None

    # Address
    address = raw.get("address", "")

    # Coordinates — in pos.lat/lng
    pos = raw.get("pos") or {}
    latitude = pos.get("lat")
    longitude = pos.get("lng")

    # Detail URL
    detail_url = (raw.get("listingUris") or {}).get("detail", "")
    if detail_url and not detail_url.startswith("http"):
        detail_url = "https://www.zoopla.co.uk" + detail_url

    return {
        "property_id": property_id,
        "listing_price": listing_price,
        "beds": beds,
        "baths": baths,
        "address": address,
        "latitude": float(latitude) if latitude is not None else None,
        "longitude": float(longitude) if longitude is not None else None,
        "detail_url": detail_url,
    }

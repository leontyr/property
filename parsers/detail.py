"""
Parse Zoopla property detail page RSC payload.

Confirmed field paths from live page (2024/2025):
  Main object keys: __typename, displayAddress, listingId, title, propertyType,
    detailedDescription ($KEY reference → resolved via T-blob),
    counts.{numBathrooms,numBedrooms}, floorArea, ingested.sizeSqft,
    location.{postalCode,uprn,coordinates,propertyNumberOrName,streetName},
    pricing.internalValue, tenure, publishedOn

  The main object is identified by containing both "uprn" and "tenure" and "location".
  detailedDescription may be a RSC reference "$KEY" — resolved to a T-blob.
"""
import json
import logging
import re

logger = logging.getLogger(__name__)


def _extract_main_listing_object(rsc_text: str) -> dict | None:
    """
    Find the main property data object in the RSC payload.
    Identified by containing both 'uprn' and 'tenure' and 'location' keys.
    Returns parsed dict or None.
    """
    # Find the first occurrence of the location object that has "uprn"
    uprn_idx = rsc_text.find('"uprn"')
    if uprn_idx == -1:
        return None

    # Walk backwards to find the top-level object containing this uprn
    # The parent object of "location": {... "uprn": ...} also has "tenure", "pricing" etc.
    # Strategy: find "location":{...uprn...} and then go one level up

    # Find "location":{ preceding the uprn
    loc_idx = rsc_text.rfind('"location":{', 0, uprn_idx)
    if loc_idx == -1:
        loc_idx = uprn_idx

    # Walk backwards from loc_idx to find the enclosing parent {
    depth = 0
    start = loc_idx
    for i in range(loc_idx - 1, max(0, loc_idx - 15000), -1):
        if rsc_text[i] == '}':
            depth += 1
        elif rsc_text[i] == '{':
            if depth == 0:
                start = i
                break
            depth -= 1

    # Find matching end
    depth = 0
    end = start
    for i in range(start, min(len(rsc_text), start + 25000)):
        if rsc_text[i] == '{':
            depth += 1
        elif rsc_text[i] == '}':
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    try:
        obj = json.loads(rsc_text[start:end])
        # Validate it's the right object
        if "location" in obj and "tenure" in obj:
            return obj
        # Maybe we got the location sub-object — try going one level further up
    except json.JSONDecodeError:
        pass

    # Fallback: search for an object with all three key indicators
    # Try regex to find {"__typename":"ListingDetails" or "displayAddress":
    for anchor in ('"__typename":"ListingDetails"', '"displayAddress":'):
        anchor_idx = rsc_text.find(anchor)
        if anchor_idx == -1:
            continue
        # Find enclosing object
        depth = 0
        start = anchor_idx
        for i in range(anchor_idx - 1, max(0, anchor_idx - 100), -1):
            if rsc_text[i] == '{':
                start = i
                break
        depth = 0
        end = start
        for i in range(start, min(len(rsc_text), start + 30000)):
            if rsc_text[i] == '{':
                depth += 1
            elif rsc_text[i] == '}':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        try:
            obj = json.loads(rsc_text[start:end])
            if "location" in obj:
                return obj
        except json.JSONDecodeError:
            continue

    return None


def _resolve_description(ref: str, rsc_text: str) -> str:
    """
    Resolve a RSC reference like "$81" to the actual text blob.
    RSC T-blob format: KEY:Thex_length,content
    """
    if not ref or not ref.startswith("$"):
        return ref or ""
    key = ref[1:]  # strip the $
    pattern = rf'\b{re.escape(key)}:T([0-9a-f]+),'
    m = re.search(pattern, rsc_text)
    if not m:
        logger.debug("Could not resolve RSC reference %s", ref)
        return ""
    hex_length = m.group(1)
    length = int(hex_length, 16)
    content_start = m.end()
    text = rsc_text[content_start:content_start + length]
    # Strip HTML tags for plain text
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean


def parse_detail(rsc_text: str) -> dict:
    """
    Extract detail fields from property detail page RSC payload.
    Returns flat dict matching Property fields.
    """
    obj = _extract_main_listing_object(rsc_text)
    if not obj:
        raise ValueError(
            "Could not find main listing object in RSC payload. "
            "Inspect samples/detail_rsc_raw.txt for correct structure."
        )

    location = obj.get("location") or {}
    counts = obj.get("counts") or {}
    ingested = obj.get("ingested") or {}
    pricing = obj.get("pricing") or {}

    # UPRN — treat explicit null as empty string
    uprn = str(location.get("uprn") or "")

    # Full address with door number
    # displayAddress is e.g. "Beresford Road, Kingston Upon Thames KT2"
    # propertyNumberOrName is e.g. "22"
    # Prepend number if displayAddress doesn't already start with it
    number = str(location.get("propertyNumberOrName") or "")
    display = obj.get("displayAddress", "")
    if number and display and not display.startswith(number):
        full_address = f"{number} {display}"
    else:
        full_address = display

    # Postcode
    postcode = location.get("postalCode", "")

    # Property name/title
    property_name = obj.get("title", "")

    # Description — may be a RSC reference like "$81"
    raw_desc = obj.get("detailedDescription", "")
    if isinstance(raw_desc, str) and raw_desc.startswith("$"):
        description = _resolve_description(raw_desc, rsc_text)
    elif isinstance(raw_desc, str):
        # Strip any HTML
        description = re.sub(r'<[^>]+>', ' ', raw_desc)
        description = re.sub(r'\s+', ' ', description).strip()
    else:
        description = ""

    # Tenure
    tenure = obj.get("tenure", "")

    # Floor size — check floorArea first, then ingested.sizeSqft
    floor_area = obj.get("floorArea")
    if floor_area and isinstance(floor_area, dict):
        val = floor_area.get("value") or floor_area.get("sqFeet")
        unit = floor_area.get("unit", "")
        floor_size = f"{val} {unit}".strip() if val else ""
    elif floor_area:
        floor_size = str(floor_area)
    else:
        sqft = ingested.get("sizeSqft")
        floor_size = f"{sqft} sqft" if sqft else ""

    # Published/update date
    listing_update_date = obj.get("publishedOn", "")
    if listing_update_date and "T" in listing_update_date:
        listing_update_date = listing_update_date.split("T")[0]

    # Chain free — lives in analytics blob, not main listing obj; regex-extract directly
    chain_free_match = re.search(r'"chainFree"\s*:\s*(true|false)', rsc_text)
    if chain_free_match:
        chain_free = chain_free_match.group(1) == "true"
    else:
        chain_free = None

    # EPC rating — derivedEPC.efficiencyRating in main listing object
    derived_epc = obj.get("derivedEPC") or {}
    epc_rating = derived_epc.get("efficiencyRating", "")

    # Council tax band — lives in ntsInfo list as {"key":"council_tax_band","value":"D",...}
    council_tax_band = ""
    for item in (obj.get("ntsInfo") or []):
        if isinstance(item, dict) and item.get("key") == "council_tax_band":
            council_tax_band = item.get("value", "")
            break

    logger.debug(
        "Parsed detail: uprn=%s postcode=%s tenure=%s address=%s chain_free=%s epc=%s ctb=%s",
        uprn, postcode, tenure, full_address, chain_free, epc_rating, council_tax_band
    )

    return {
        "uprn": uprn,
        "property_name": property_name,
        "description": description,
        "tenure": tenure,
        "floor_size": floor_size,
        "listing_update_date": listing_update_date,
        "postcode": postcode,
        "address": full_address,
        "chain_free": chain_free,
        "epc_rating": epc_rating,
        "council_tax_band": council_tax_band,
    }

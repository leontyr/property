"""
Zoopla Property Scraper
=======================
Usage:
    python scraper.py --url "https://www.zoopla.co.uk/for-sale/houses/kt2-6rl/?..." \
                      --max-pages 3 \
                      --output output/properties.json \
                      --save-samples \
                      --headless

Steps per property:
  1. Search page  → listing summary (price, beds, baths, address, lat/lng)
  2. Detail page  → UPRN, description, tenure, floor size, update date
  3. Estimate page → estimated price (low / mid / high)
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import pandas as pd

from browser import browser_session
from models import Property
from parsers.search import parse_listings, parse_pagination, extract_listing_summary
from parsers.detail import parse_detail
from parsers.estimate import parse_estimate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scraper")

SAMPLES_DIR = Path("samples")
OUTPUT_DIR = Path("output")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_page_url(base_url: str, page: int) -> str:
    """Append ?pn=N to a search URL for pagination."""
    parsed = urlparse(base_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["pn"] = [str(page)]
    new_query = urlencode({k: v[0] if len(v) == 1 else v for k, v in params.items()}, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def save_sample(name: str, data, force: bool = False):
    """Save raw JSON blob to samples/ directory (only once unless forced)."""
    SAMPLES_DIR.mkdir(exist_ok=True)
    path = SAMPLES_DIR / name
    if path.exists() and not force:
        return
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(data, str):
            f.write(data)
        else:
            json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("Saved sample → %s", path)


def load_existing(output_path: Path) -> dict[str, dict]:
    """Load existing CSV into a dict keyed by property_id."""
    csv_path = output_path.with_suffix(".csv")
    if not csv_path.exists():
        return {}
    df = pd.read_csv(csv_path, dtype=str)
    # Convert numeric-ish columns back
    int_cols = ["listing_price", "beds", "baths", "estimate_price", "estimate_low", "estimate_high"]
    float_cols = ["latitude", "longitude"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    records = df.where(pd.notna(df), None).to_dict(orient="records")
    result = {}
    for r in records:
        pid = str(r.get("property_id", ""))
        if pid:
            # Convert Int64 → plain int/None
            for col in int_cols:
                v = r.get(col)
                r[col] = int(v) if v is not None else None
            result[pid] = r
    logger.info("Loaded %d existing properties from %s", len(result), csv_path)
    return result


# Fields that come from the search page that can meaningfully change between runs.
# NOTE: address is intentionally excluded — the detail page gives the more complete
# version (with door number); we never want to overwrite it with the search-page
# short form.
_SEARCH_FIELDS = {"listing_price", "beds", "baths", "latitude", "longitude"}


def save_results(properties: list[Property], output_path: Path):
    """Save results as JSON and CSV."""
    output_path.parent.mkdir(exist_ok=True)
    records = [p.to_dict() for p in properties]

    # JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    logger.info("Saved %d properties → %s", len(records), output_path)

    # CSV
    csv_path = output_path.with_suffix(".csv")
    df = pd.DataFrame(records)
    df.to_csv(csv_path, index=False)
    logger.info("Saved CSV → %s", csv_path)


# ---------------------------------------------------------------------------
# Scraping stages
# ---------------------------------------------------------------------------

async def scrape_search_page(browser, url: str, save_samples: bool, page_num: int) -> tuple[list[dict], dict]:
    """Fetch one search results page, return (listing_summaries, pagination_info)."""
    # Wait for a listing card to confirm page loaded
    rsc = await browser.get_rsc_payload(url)

    if save_samples and page_num == 1:
        save_sample("search_raw.txt", rsc)

    listings_raw = parse_listings(rsc)
    summaries = []
    for raw in listings_raw:
        try:
            summaries.append(extract_listing_summary(raw))
        except Exception as e:
            logger.warning("Failed to parse listing summary: %s", e)

    pagination = parse_pagination(rsc)
    logger.info(
        "Page %d/%d — found %d listings (total=%d)",
        page_num,
        pagination["total_pages"],
        len(summaries),
        pagination["total_results"],
    )
    return summaries, pagination


async def scrape_detail_page(browser, detail_url: str, save_samples: bool, is_first: bool) -> Optional[dict]:
    """Fetch property detail page, return parsed fields (including UPRN)."""
    try:
        rsc = await browser.get_rsc_payload(detail_url)
        if save_samples and is_first:
            save_sample("detail_raw.txt", rsc)
        return parse_detail(rsc)
    except Exception as e:
        logger.warning("Detail page failed (%s): %s", detail_url, e)
        return None


async def scrape_estimate_page(browser, uprn: str, save_samples: bool, is_first: bool) -> Optional[dict]:
    """Fetch UPRN estimate page, return parsed estimate fields."""
    estimate_url = f"https://www.zoopla.co.uk/property/uprn/{uprn}/"
    try:
        # Wait for estimate value to render (shorter timeout — not all properties have estimates)
        html = await browser.get_page_content(
            estimate_url,
            wait_selector='[data-testid="estimate-blurred"]',
            selector_timeout=8000,
            retries=1,
        )
        if save_samples and is_first:
            save_sample("estimate_raw.html", html)
        return parse_estimate(html, uprn)
    except Exception as e:
        logger.warning("Estimate page failed (uprn=%s): %s", uprn, e)
        return None


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def scrape(
    search_url: str,
    max_pages: int = 1,
    output_path: Path = OUTPUT_DIR / "properties.json",
    save_samples: bool = False,
    headless: bool = True,
    max_properties: Optional[int] = None,
):
    # Load any previously scraped records so we can skip detail/estimate pages
    existing: dict[str, dict] = load_existing(output_path)

    # We'll build an ordered dict: property_id → Property, seeded with existing
    seen: dict[str, Property] = {
        pid: Property(**{k: v for k, v in rec.items() if hasattr(Property, k) or k in Property.__dataclass_fields__})
        for pid, rec in existing.items()
    }
    # Track which IDs appeared in this search run (to preserve ordering: new ones first)
    seen_in_run: list[str] = []

    first_detail = True
    first_estimate = True

    async with browser_session(headless=headless) as browser:
        page_num = 1

        while page_num <= max_pages:
            url = build_page_url(search_url, page_num) if page_num > 1 else search_url
            try:
                summaries, pagination = await scrape_search_page(browser, url, save_samples, page_num)
            except Exception as e:
                logger.error("Search page %d failed: %s", page_num, e)
                break

            for summary in summaries:
                if max_properties and len(seen_in_run) >= max_properties:
                    break

                pid = str(summary.get("property_id", ""))
                if not pid:
                    continue

                if pid in existing:
                    # --- Already scraped: reuse detail+estimate, update search fields if changed ---
                    prop = seen[pid]
                    changed_fields = []
                    for field in _SEARCH_FIELDS:
                        new_val = summary.get(field)
                        old_val = getattr(prop, field, None)
                        if new_val != old_val:
                            setattr(prop, field, new_val)
                            changed_fields.append(f"{field}: {old_val!r} → {new_val!r}")
                    if changed_fields:
                        logger.info(
                            "UPDATED  [%s] %s — changes: %s",
                            pid, prop.address, "; ".join(changed_fields),
                        )
                    else:
                        logger.info(
                            "SKIPPED  [%s] %s — already in CSV, no changes",
                            pid, prop.address,
                        )
                else:
                    # --- New property: full scrape ---
                    prop = Property(**summary)
                    logger.info(
                        "NEW      [%s] %s — £%s",
                        prop.property_id,
                        prop.address,
                        f"{prop.listing_price:,}" if prop.listing_price else "?",
                    )

                    # Stage 2: detail page
                    if prop.detail_url:
                        detail = await scrape_detail_page(
                            browser, prop.detail_url, save_samples, first_detail
                        )
                        if detail:
                            first_detail = False
                            for field, value in detail.items():
                                if value:
                                    setattr(prop, field, value)

                    # Stage 3: estimate page (skip if no UPRN)
                    if prop.uprn and prop.uprn != "None":
                        estimates = await scrape_estimate_page(
                            browser, prop.uprn, save_samples, first_estimate
                        )
                        if estimates:
                            first_estimate = False
                            for field, value in estimates.items():
                                setattr(prop, field, value)
                    else:
                        logger.warning("No UPRN for property %s — skipping estimate", prop.property_id)

                    logger.info(
                        "  → estimate £%s (£%s – £%s)",
                        prop.estimate_price,
                        prop.estimate_low,
                        prop.estimate_high,
                    )
                    seen[pid] = prop

                if pid not in seen_in_run:
                    seen_in_run.append(pid)

            if max_properties and len(seen_in_run) >= max_properties:
                logger.info("Reached max_properties=%d, stopping", max_properties)
                break

            total_pages = pagination.get("total_pages", 1)
            if page_num >= total_pages or page_num >= max_pages:
                break
            page_num += 1

    # Output: properties found in this run first, then any existing ones not in this search
    run_props = [seen[pid] for pid in seen_in_run if pid in seen]
    extra_props = [seen[pid] for pid in existing if pid not in seen_in_run]
    all_props = run_props + extra_props

    stats_new = sum(1 for pid in seen_in_run if pid not in existing)
    stats_updated = sum(1 for pid in seen_in_run if pid in existing)
    logger.info(
        "Run summary: %d new, %d existing (%d carried from previous runs)",
        stats_new, stats_updated, len(extra_props),
    )

    save_results(all_props, output_path)
    return all_props


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape Zoopla property listings")
    parser.add_argument(
        "--url",
        required=True,
        help="Zoopla search URL",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Maximum number of search result pages to scrape (default: 1)",
    )
    parser.add_argument(
        "--max-properties",
        type=int,
        default=None,
        help="Stop after N properties (useful for testing)",
    )
    parser.add_argument(
        "--output",
        default="output/properties.json",
        help="Output JSON file path (default: output/properties.json)",
    )
    parser.add_argument(
        "--save-samples",
        action="store_true",
        help="Save raw JSON blobs to samples/ for debugging",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        default=True,
        help="Run browser headlessly (default: True)",
    )
    parser.add_argument(
        "--no-headless",
        dest="headless",
        action="store_false",
        help="Run browser with visible window (useful for debugging Cloudflare)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    output_path = Path(args.output)

    results = asyncio.run(
        scrape(
            search_url=args.url,
            max_pages=args.max_pages,
            output_path=output_path,
            save_samples=args.save_samples,
            headless=args.headless,
            max_properties=args.max_properties,
        )
    )

    print(f"\nDone. Scraped {len(results)} properties → {output_path}")


if __name__ == "__main__":
    main()

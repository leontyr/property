"""
Re-enrich commute data for all properties using Routes API v2 (computeRoutes).

Clears existing commute fields on every property then re-populates using the
shortest of all alternative transit routes. Rewrites JSON, CSV and web JS.

Usage:
    python reenrich_commutes.py --gmaps-key YOUR_KEY [--output output/properties.json]
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from models import Property
from commute import enrich_commutes
from scraper import save_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("reenrich")

COMMUTE_FIELDS = [
    "school_commute_seconds", "school_commute_text", "school_distance_km",
    "office_commute_seconds", "office_commute_text", "office_distance_km",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--gmaps-key", required=True)
    parser.add_argument("--output", default="output/properties.json")
    args = parser.parse_args()

    output_path = Path(args.output)
    if not output_path.exists():
        logger.error("Not found: %s", output_path)
        sys.exit(1)

    with open(output_path, encoding="utf-8") as f:
        records = json.load(f)
    logger.info("Loaded %d properties from %s", len(records), output_path)

    # Clear commute fields on every record
    for r in records:
        for field in COMMUTE_FIELDS:
            r[field] = None
    logger.info("Cleared commute fields on all %d properties", len(records))

    # Reconstruct Property objects
    fields = set(Property.__dataclass_fields__)
    properties = [Property(**{k: v for k, v in r.items() if k in fields}) for r in records]

    # Re-enrich
    updated = enrich_commutes(properties, args.gmaps_key)
    logger.info("Commute enrichment complete: %d properties updated", updated)

    # Save
    save_results(properties, output_path)


if __name__ == "__main__":
    main()

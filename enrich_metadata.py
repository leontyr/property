"""
Standalone script to batch-enrich all existing properties with LLM metadata.

Skips properties where garden_facing is already populated.
Saves a checkpoint every 50 properties so it's safe to interrupt and resume.

Usage:
    python enrich_metadata.py
    python enrich_metadata.py --output output/properties.json  # default
"""

import argparse
import json
import logging
import time
from pathlib import Path

from models import Property
from extract_metadata import extract_property_metadata, metadata_to_fields
from scraper import save_results, load_existing

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("enrich_metadata")

CHECKPOINT_EVERY = 50


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="output/properties.json")
    args = parser.parse_args()

    output_path = Path(args.output)

    # Load existing properties
    with open(output_path) as f:
        records = json.load(f)

    properties = [
        Property(**{k: v for k, v in r.items() if k in Property.__dataclass_fields__})
        for r in records
    ]
    logger.info("Loaded %d properties from %s", len(properties), output_path)

    # Only process properties that have a description but no metadata yet
    to_update = [p for p in properties if p.description and not p.garden_facing]
    skip = len(properties) - len(to_update)
    logger.info("To enrich: %d  |  Already done / no description: %d", len(to_update), skip)

    if not to_update:
        logger.info("Nothing to do.")
        return

    updated = 0
    total_secs = 0.0

    for i, prop in enumerate(to_update, 1):
        t0 = time.time()
        try:
            meta = extract_property_metadata(prop.description)
            for field, value in metadata_to_fields(meta).items():
                setattr(prop, field, value)
            elapsed = time.time() - t0
            total_secs += elapsed
            updated += 1

            if i % 10 == 0 or i == len(to_update):
                avg = total_secs / i
                remaining = (len(to_update) - i) * avg
                logger.info(
                    "[%d/%d] %s | garden=%-12s parking=%-10s dev=%s | %.1fs (avg %.1fs, ~%dm left)",
                    i, len(to_update),
                    prop.address[:35],
                    prop.garden_facing,
                    prop.parking_type,
                    prop.dev_types,
                    elapsed,
                    avg,
                    remaining / 60,
                )
        except Exception as e:
            logger.error("[%d/%d] Failed %s: %s", i, len(to_update), prop.property_id, e)

        # Checkpoint
        if i % CHECKPOINT_EVERY == 0:
            save_results(properties, output_path)
            logger.info("Checkpoint saved (%d/%d done)", i, len(to_update))

    # Final save
    for p in properties:
        p.compute_derived()
    save_results(properties, output_path)
    logger.info("Done. Enriched %d properties in %.1f min", updated, total_secs / 60)


if __name__ == "__main__":
    main()

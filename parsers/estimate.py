"""
Parse Zoopla estimated price page (/property/uprn/{uprn}/).

Confirmed data-testid selectors from live page:
  [data-testid="low-estimate-blurred"]   → lower bound  (e.g. "£947k")
  [data-testid="estimate-blurred"]       → mid estimate  (e.g. "£997k")
  [data-testid="high-estimate-blurred"]  → upper bound   (e.g. "£1.05m")

Values use shorthand: "£947k", "£1.05m" → converted to integers.
"""
import logging
import re
from typing import Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def _parse_price_text(text: str) -> Optional[int]:
    """Convert '£947k', '£1.05m', '£950,000' to int."""
    if not text:
        return None
    cleaned = re.sub(r'[£,\s]', '', text.strip()).lower()
    if cleaned.endswith('k'):
        try:
            return int(float(cleaned[:-1]) * 1_000)
        except ValueError:
            return None
    if cleaned.endswith('m'):
        try:
            return int(float(cleaned[:-1]) * 1_000_000)
        except ValueError:
            return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_estimate(html: str, uprn: str) -> dict:
    """
    Extract estimated prices from the UPRN page HTML.
    Returns dict with estimate_price, estimate_low, estimate_high, estimate_url.
    """
    soup = BeautifulSoup(html, "lxml")

    def get_testid(testid: str) -> Optional[str]:
        el = soup.find(attrs={"data-testid": testid})
        return el.get_text(strip=True) if el else None

    # Confirmed selectors from live Zoopla page
    low_text = get_testid("low-estimate-blurred")
    mid_text = get_testid("estimate-blurred")
    high_text = get_testid("high-estimate-blurred")

    logger.debug(
        "Estimate DOM: low=%s mid=%s high=%s",
        low_text, mid_text, high_text
    )

    result = {
        "estimate_price": _parse_price_text(mid_text),
        "estimate_low": _parse_price_text(low_text),
        "estimate_high": _parse_price_text(high_text),
        "estimate_url": f"https://www.zoopla.co.uk/property/uprn/{uprn}/",
    }

    if not any(v for v in (result["estimate_price"], result["estimate_low"], result["estimate_high"])):
        logger.warning("No estimate values found for UPRN %s", uprn)

    return result

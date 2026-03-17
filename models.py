from dataclasses import dataclass, field, asdict
from typing import Optional
import json


@dataclass
class Property:
    # --- From search page ---
    property_id: str = ""
    listing_price: Optional[int] = None
    beds: Optional[int] = None
    baths: Optional[int] = None
    address: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    detail_url: str = ""

    # --- From detail page ---
    uprn: str = ""
    property_name: str = ""
    description: str = ""
    tenure: str = ""
    floor_size: str = ""
    listing_update_date: str = ""
    postcode: str = ""

    # --- From estimate page ---
    estimate_price: Optional[int] = None
    estimate_low: Optional[int] = None
    estimate_high: Optional[int] = None
    estimate_url: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

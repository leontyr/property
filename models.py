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
    features: str = ""        # comma-separated key feature bullets
    tenure: str = ""
    floor_size: str = ""
    listing_update_date: str = ""
    postcode: str = ""
    chain_free: Optional[bool] = None
    epc_rating: str = ""
    council_tax_band: str = ""

    # --- From estimate page ---
    estimate_price: Optional[int] = None
    estimate_low: Optional[int] = None
    estimate_high: Optional[int] = None
    estimate_url: str = ""

    # --- LLM-extracted metadata ---
    garden_facing: str = ""
    dev_types: str = ""           # comma-separated: "Loft,Rear"
    dev_planning: str = ""        # "Granted" | "Lapsed" | "Potential" | "None"
    dev_unmodernized: Optional[bool] = None
    parking_type: str = ""
    parking_spaces: Optional[int] = None
    parking_ev: Optional[bool] = None
    outdoor_space: str = ""
    period_features: Optional[bool] = None
    double_glazing: Optional[bool] = None
    quiet_rating: str = ""
    river_proximity: str = ""

    # --- Derived ---
    price_delta: Optional[int] = None  # listing_price - estimate_price

    # --- Commute: kids school (51.41188, -0.29607) arriving 08:30 ---
    school_commute_seconds: Optional[int] = None
    school_commute_text: str = ""
    school_distance_km: Optional[float] = None
    school_commute_url: str = ""

    # --- Commute: office Hylo, 103-105 Bunhill Row EC1Y 8LZ (51.52281, -0.09015) arriving 10:00 ---
    office_commute_seconds: Optional[int] = None
    office_commute_text: str = ""
    office_distance_km: Optional[float] = None
    office_commute_url: str = ""

    def compute_derived(self):
        """Recompute fields derived from other fields."""
        if self.listing_price is not None and self.estimate_price is not None:
            self.price_delta = self.listing_price - self.estimate_price
        else:
            self.price_delta = None

        if self.latitude and self.longitude:
            origin = f"{self.latitude},{self.longitude}"
            base = "https://www.google.com/maps/dir/?api=1&travelmode=transit"
            self.school_commute_url = f"{base}&origin={origin}&destination=51.41188,-0.29607"
            self.office_commute_url = f"{base}&origin={origin}&destination=51.52281,-0.09015"

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

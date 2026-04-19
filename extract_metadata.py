"""
Property metadata extraction using Ollama + Gemma 4:26b.

Extracts structured metadata from estate agent descriptions using LLM inference.
Uses ollama.chat with format="json" and validates output with Pydantic.
"""

import json
import logging
from typing import Any, List, Literal, Optional

import ollama
from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger("extract_metadata")

MODEL = "gemma4:e4b"

_DEV_TYPES = {"Loft", "Rear", "Side", "Basement", "None"}
_DEV_PLANNING = {"Granted", "Lapsed", "Potential", "None"}
_PARKING = {"Driveway", "Garage", "On-Street", "None"}
_OUTDOOR = {"Balcony", "Terrace", "Small Garden", "Large Garden", "Communal", "None"}
_QUIET = {"Cul-de-sac", "Private Road", "Main Road", "Busy Junction", "Unknown"}
_GARDEN = {"North", "North-East", "East", "South-East", "South", "South-West", "West", "North-West", "Unknown"}


def _str_or_default(v: Any, valid: set, default: str) -> str:
    if isinstance(v, dict):
        # model wrapped scalar in a dict — take first string value that matches
        for val in v.values():
            if isinstance(val, str) and val in valid:
                return val
        return default
    if isinstance(v, str) and v in valid:
        return v
    return default


# ── Pydantic schema ──────────────────────────────────────────────────────────

class DevelopmentPotential(BaseModel):
    type: List[Literal["Loft", "Rear", "Side", "Basement", "None"]] = ["None"]
    planning_status: Literal["Granted", "Lapsed", "Potential", "None"] = "None"
    unmodernized: bool = False

    @field_validator("type", mode="before")
    @classmethod
    def coerce_type_list(cls, v):
        if v is None:
            return ["None"]
        if isinstance(v, str):
            return [v] if v in _DEV_TYPES else ["None"]
        if isinstance(v, list):
            cleaned = [x for x in v if isinstance(x, str) and x in _DEV_TYPES]
            return cleaned if cleaned else ["None"]
        return ["None"]

    @field_validator("planning_status", mode="before")
    @classmethod
    def coerce_planning(cls, v):
        return _str_or_default(v, _DEV_PLANNING, "None")

    @field_validator("unmodernized", mode="before")
    @classmethod
    def coerce_unmod(cls, v):
        if isinstance(v, dict):
            return any(bool(x) for x in v.values())
        return bool(v) if v is not None else False


class ParkingEV(BaseModel):
    parking_type: Literal["Driveway", "Garage", "On-Street", "None"] = "None"
    spaces: int = 0
    ev_charger: bool = False

    @field_validator("parking_type", mode="before")
    @classmethod
    def coerce_parking(cls, v):
        return _str_or_default(v, _PARKING, "None")

    @field_validator("spaces", mode="before")
    @classmethod
    def coerce_spaces(cls, v):
        if isinstance(v, dict):
            for val in v.values():
                if isinstance(val, int):
                    return val
            return 0
        try:
            return int(v) if v is not None else 0
        except (TypeError, ValueError):
            return 0

    @field_validator("ev_charger", mode="before")
    @classmethod
    def coerce_ev(cls, v):
        if isinstance(v, dict):
            return any(bool(x) for x in v.values())
        return bool(v) if v is not None else False


class PropertyMetadata(BaseModel):
    garden_facing: Literal[
        "North", "North-East", "East", "South-East",
        "South", "South-West", "West", "North-West", "Unknown"
    ] = "Unknown"
    development_potential: DevelopmentPotential = Field(default_factory=DevelopmentPotential)
    parking_ev: ParkingEV = Field(default_factory=ParkingEV)
    outdoor_space: Literal[
        "Balcony", "Terrace", "Small Garden", "Large Garden", "Communal", "None"
    ] = "None"
    period_features: bool = False
    double_glazing: bool = False
    quiet_rating: Literal[
        "Cul-de-sac", "Private Road", "Main Road", "Busy Junction", "Unknown"
    ] = "Unknown"
    river_proximity: str = "None"

    @model_validator(mode="before")
    @classmethod
    def normalize_keys(cls, data):
        if not isinstance(data, dict):
            return data
        # model sometimes uses garden_aspect instead of garden_facing
        if "garden_aspect" in data and "garden_facing" not in data:
            data["garden_facing"] = data.pop("garden_aspect")
        # model sometimes uses parking instead of parking_ev
        if "parking" in data and "parking_ev" not in data:
            p = data.pop("parking")
            if isinstance(p, dict):
                data["parking_ev"] = {
                    "parking_type": p.get("parking_type") or "None",
                    "spaces": p.get("spaces") or 0,
                    "ev_charger": p.get("ev_charger") or False,
                }
        # model sometimes uses development instead of development_potential
        if "development" in data and "development_potential" not in data:
            data["development_potential"] = data.pop("development")
        return data

    @field_validator("garden_facing", mode="before")
    @classmethod
    def coerce_garden(cls, v):
        return _str_or_default(v, _GARDEN, "Unknown")

    @field_validator("outdoor_space", mode="before")
    @classmethod
    def coerce_outdoor(cls, v):
        return _str_or_default(v, _OUTDOOR, "None")

    @field_validator("period_features", "double_glazing", mode="before")
    @classmethod
    def coerce_bool(cls, v):
        if isinstance(v, dict):
            return any(bool(x) for x in v.values())
        return bool(v) if v is not None else False

    @field_validator("quiet_rating", mode="before")
    @classmethod
    def coerce_quiet(cls, v):
        return _str_or_default(v, _QUIET, "Unknown")

    @field_validator("river_proximity", mode="before")
    @classmethod
    def coerce_river(cls, v):
        if v is None:
            return "None"
        if isinstance(v, dict):
            for val in v.values():
                if isinstance(val, str):
                    return val or "None"
            return "None"
        return str(v) if v else "None"


# ── System prompt ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a UK property analyst. Extract structured metadata from estate agent descriptions.
Be aware of common "agent-speak":

Garden facing:
- "sunny aspect", "sunny rear garden", "south-facing garden" → South
- "easterly aspect" → East; "westerly aspect" → West; "northerly aspect" → North
- compass directions stated explicitly always take priority

Development potential:
- "scope to extend", "potential to develop", "extend STPP", "subject to planning" → type: Rear or Loft (infer from context)
- "loft room", "loft conversion potential" → type: Loft
- "rear extension", "kitchen extension" → type: Rear
- "side return", "side extension" → type: Side
- "basement", "lower ground floor potential" → type: Basement
- "planning granted", "planning approved", "planning permission obtained" → planning_status: Granted
- "planning lapsed", "expired planning" → planning_status: Lapsed
- "scope for development", "further potential", "STPP", "subject to planning" → planning_status: Potential
- "in need of modernisation", "requires updating", "original condition", "great project", "blank canvas", "dated" → unmodernized: true

Parking:
- "off-street parking", "private driveway", "driveway for N cars" → parking_type: Driveway
- "garage", "single garage", "double garage" → parking_type: Garage
- "residents parking", "on-street permit" → parking_type: On-Street
- "EV charger", "electric vehicle charging point", "EV charging" → ev_charger: true
- Extract number of spaces from context (e.g. "driveway for 2 cars" → spaces: 2)

Outdoor space:
- "balcony" → Balcony
- "roof terrace", "raised terrace" → Terrace
- "small garden", "courtyard garden", "patio garden", "manageable garden" → Small Garden
- "large garden", "extensive garden", "generous garden", "mature garden", "wrap-around garden" → Large Garden
- "communal garden", "shared garden" → Communal

Period features:
- "Victorian", "Edwardian", "Georgian", "Regency", "original features", "period property",
  "bay windows", "sash windows", "cornicing", "ceiling rose", "parquet flooring" → period_features: true

Double glazing:
- "double glazed", "double glazing", "UPVC windows", "replacement windows" → double_glazing: true

Quiet rating:
- "cul-de-sac", "no through road", "close" (as in residential close) → Cul-de-sac
- "quiet road", "private road", "sought-after road", "tree-lined road" → Private Road
- "main road", "high street", "A-road" → Main Road
- "busy junction", "dual carriageway" → Busy Junction

River proximity:
- Extract any mention of Thames, river, canal proximity with distance (e.g. "10 minute walk to the Thames")
- If none mentioned, use "None"

Return ONLY valid JSON. No commentary, no markdown, no explanation."""


# ── Extraction function ──────────────────────────────────────────────────────

def extract_property_metadata(description: str, features: str = "") -> PropertyMetadata:
    """
    Extract structured metadata from a property description.
    Returns a validated PropertyMetadata object.
    On failure, retries once with a stricter prompt, then returns safe defaults.
    """
    if not description or not description.strip():
        return PropertyMetadata()

    user_content = f"Property description:\n\n{description}"
    if features and features.strip():
        user_content += f"\n\nKey features:\n{features}"

    for attempt in range(2):
        try:
            messages = [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": user_content},
            ]
            if attempt == 1:
                messages.append({
                    "role": "user",
                    "content": (
                        "Your previous response was not valid JSON. "
                        "Return ONLY a raw JSON object with no surrounding text."
                    ),
                })

            response = ollama.chat(
                model=MODEL,
                format="json",
                options={"temperature": 0},
                messages=messages,
            )
            raw = response.message.content
            return PropertyMetadata.model_validate_json(raw)

        except Exception as e:
            if attempt == 0:
                logger.debug("Attempt 1 failed (%s), retrying...", e)
            else:
                logger.warning("Both attempts failed for description (len=%d): %s", len(description), e)

    return PropertyMetadata()


def metadata_to_fields(meta: PropertyMetadata) -> dict:
    """Flatten PropertyMetadata into Property-compatible dict fields."""
    return {
        "garden_facing":     meta.garden_facing,
        "dev_types":         ",".join(meta.development_potential.type),
        "dev_planning":      meta.development_potential.planning_status,
        "dev_unmodernized":  meta.development_potential.unmodernized,
        "parking_type":      meta.parking_ev.parking_type,
        "parking_spaces":    meta.parking_ev.spaces,
        "parking_ev":        meta.parking_ev.ev_charger,
        "outdoor_space":     meta.outdoor_space,
        "period_features":   meta.period_features,
        "double_glazing":    meta.double_glazing,
        "quiet_rating":      meta.quiet_rating,
        "river_proximity":   meta.river_proximity,
    }

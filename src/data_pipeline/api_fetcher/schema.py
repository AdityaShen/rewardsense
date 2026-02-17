from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class CardOffer(BaseModel):
    """
    Normalized internal schema for all credit card offers,
    regardless of source (scraper or API).

    This schema is intentionally:
    - Normalized for downstream use (card_name, annual_fee, welcome_bonus, etc.)
    - Enriched with optional upstream metadata when available (currency, network, etc.)
    - Able to preserve the full raw upstream record for debugging/auditing via `raw`
    """

    # --- Required core metadata ---
    source: str = Field(..., description="Data source name")
    card_name: str = Field(..., description="Official name of the credit card")
    issuer: str = Field(..., description="Bank issuer")

    # --- Financial attributes ---
    annual_fee: Optional[float] = Field(None, description="Annual fee in USD")
    welcome_bonus: Optional[str] = Field(
        None, description="Welcome bonus text (human-readable)"
    )
    bonus_value_usd: Optional[float] = Field(
        None, description="Estimated bonus value in USD (if computed)"
    )

    # Reward multipliers / baseline rates (schema is flexible):
    # e.g. {"universal_base_rate": 1.0}
    reward_rates: Dict[str, float] = Field(
        default_factory=dict, description="Reward multipliers or baseline rates"
    )

    categories: List[str] = Field(
        default_factory=list, description="Card categories (if available)"
    )
    apr: Optional[str] = Field(None, description="APR information (if available)")
    offer_url: Optional[str] = Field(None, description="Offer URL")

    # --- Optional upstream metadata (from credit-card-bonuses-api export) ---
    card_id: Optional[str] = Field(None, description="Upstream cardId (if available)")
    network: Optional[str] = Field(
        None, description="Card network (e.g., VISA/MASTERCARD/AMEX)"
    )
    currency: Optional[str] = Field(
        None, description="Reward currency/program (e.g., DELTA, UR, MR)"
    )
    is_business: Optional[bool] = Field(
        None, description="Whether this is a business card"
    )
    is_annual_fee_waived: Optional[bool] = Field(
        None, description="Whether annual fee is waived (if provided)"
    )
    universal_cashback_percent: Optional[float] = Field(
        None,
        description="Upstream universalCashbackPercent baseline rate (if provided)",
    )
    image_url: Optional[str] = Field(
        None, description="Upstream imageUrl (if provided)"
    )
    discontinued: Optional[bool] = Field(
        None, description="Upstream discontinued flag (if provided)"
    )

    # --- Preserve raw bonus structures for 'seeing everything' ---
    # These match the upstream export shapes closely.
    offers: List[Dict[str, Any]] = Field(
        default_factory=list, description="Upstream offers list (structured)"
    )
    historical_offers: List[Dict[str, Any]] = Field(
        default_factory=list, description="Upstream historicalOffers list"
    )
    credits: List[Dict[str, Any]] = Field(
        default_factory=list, description="Upstream credits list"
    )

    # --- Full raw payload (optional but useful for debugging/auditing) ---
    raw: Optional[Dict[str, Any]] = Field(
        default=None, description="Full raw upstream record (for debugging/auditing)"
    )

    last_updated: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(),
        description="ISO timestamp when record was created",
    )

    # ------------------------------
    # Field Validators (Pydantic v2)
    # ------------------------------

    @field_validator(
        "annual_fee",
        "bonus_value_usd",
        "universal_cashback_percent",
        mode="before",
    )
    @classmethod
    def validate_numeric_fields(cls, v):
        if v is None:
            return None
        try:
            if isinstance(v, str):
                v = v.replace("$", "").replace(",", "").strip()
            return float(v)
        except (ValueError, TypeError):
            return None

    @field_validator("reward_rates", mode="before")
    @classmethod
    def validate_reward_rates(cls, v):
        if v is None:
            return {}
        if isinstance(v, dict):
            cleaned = {}
            for k, val in v.items():
                try:
                    cleaned[str(k).lower()] = float(val)
                except (ValueError, TypeError):
                    continue
            return cleaned
        return {}

    @field_validator("categories", mode="before")
    @classmethod
    def validate_categories(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return [str(cat).lower() for cat in v]
        return []

    @field_validator("offers", "historical_offers", "credits", mode="before")
    @classmethod
    def validate_list_of_dicts(cls, v):
        if v is None:
            return []
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
        return []

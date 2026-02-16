from typing import Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class CardOffer(BaseModel):
    """
    Normalized internal schema for all credit card offers,
    regardless of source (scraper or API).
    """

    # --- Required core metadata ---
    source: str = Field(..., description="Data source name")
    card_name: str = Field(..., description="Official name of the credit card")
    issuer: str = Field(..., description="Bank issuer")

    # --- Financial attributes ---
    annual_fee: Optional[float] = Field(None, description="Annual fee in USD")
    welcome_bonus: Optional[str] = Field(None, description="Welcome bonus text")
    bonus_value_usd: Optional[float] = Field(None, description="Estimated bonus value")

    reward_rates: Dict[str, float] = Field(
        default_factory=dict, description="Reward multipliers by category"
    )

    categories: List[str] = Field(default_factory=list, description="Card categories")

    apr: Optional[str] = Field(None, description="APR information")
    offer_url: Optional[str] = Field(None, description="Offer URL")

    last_updated: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(),
        description="ISO timestamp when record was created",
    )

    # ------------------------------
    # Field Validators (Pydantic v2)
    # ------------------------------

    @field_validator("annual_fee", "bonus_value_usd", mode="before")
    @classmethod
    def validate_numeric_fields(cls, v):
        if v is None:
            return None
        try:
            # remove $ and commas if present
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

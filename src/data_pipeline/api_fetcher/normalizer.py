from typing import Dict, Any, Optional

from .schema import CardOffer


def normalize_creditcardbonuses_offer(raw_offer: Dict[str, Any]) -> Optional[CardOffer]:
    """
    Convert a raw credit-card-bonuses export record into a normalized CardOffer.

    Returns:
        CardOffer if valid
        None if record is unusable
    """

    if not isinstance(raw_offer, dict):
        return None

    # -----------------------------
    # Extract core required fields
    # -----------------------------

    card_name = raw_offer.get("name") or raw_offer.get("card_name")
    issuer = raw_offer.get("issuer")

    if not card_name or not issuer:
        # Required fields missing → skip record
        return None

    # -----------------------------
    # Annual fee parsing
    # -----------------------------

    annual_fee = raw_offer.get("annual_fee")

    # Let CardOffer schema handle numeric parsing ($95 → 95.0)

    # -----------------------------
    # Welcome bonus
    # -----------------------------

    welcome_bonus = raw_offer.get("bonus") or raw_offer.get("welcome_bonus")

    # Try to extract numeric bonus value if available
    bonus_value_usd = raw_offer.get("bonus_value") or raw_offer.get("bonus_value_usd")

    # -----------------------------
    # Reward rates
    # -----------------------------

    reward_rates = {}

    raw_rewards = raw_offer.get("rewards") or raw_offer.get("reward_rates")

    if isinstance(raw_rewards, dict):
        reward_rates = raw_rewards
    elif isinstance(raw_rewards, list):
        # Sometimes rewards come as list of category dicts
        for entry in raw_rewards:
            if isinstance(entry, dict):
                category = entry.get("category")
                multiplier = entry.get("multiplier") or entry.get("rate")
                if category and multiplier:
                    reward_rates[category] = multiplier

    # -----------------------------
    # Categories
    # -----------------------------

    categories = raw_offer.get("categories")
    if not isinstance(categories, list):
        categories = []

    # -----------------------------
    # APR
    # -----------------------------

    apr = raw_offer.get("apr")

    # -----------------------------
    # Offer URL
    # -----------------------------

    offer_url = raw_offer.get("url") or raw_offer.get("offer_url")

    # -----------------------------
    # Build normalized object
    # -----------------------------

    try:
        return CardOffer(
            source="creditcardbonuses",
            card_name=card_name,
            issuer=issuer,
            annual_fee=annual_fee,
            welcome_bonus=welcome_bonus,
            bonus_value_usd=bonus_value_usd,
            reward_rates=reward_rates,
            categories=categories,
            apr=apr,
            offer_url=offer_url,
        )
    except Exception:
        # If schema validation fails, skip record
        return None
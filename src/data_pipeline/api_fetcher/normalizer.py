from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from .schema import CardOffer


def _extract_offer_amount(offer: Dict[str, Any]) -> float:
    """
    Export typically uses: offer["amount"] = [{"amount": 10000}]
    Return first numeric amount if found.
    """
    amt = offer.get("amount")
    if isinstance(amt, list) and amt:
        first = amt[0]
        if isinstance(first, dict) and isinstance(first.get("amount"), (int, float)):
            return float(first["amount"])
    if isinstance(amt, (int, float)):
        return float(amt)
    return 0.0


def _extract_offer_spend(offer: Dict[str, Any]) -> float:
    spend = offer.get("spend")
    return float(spend) if isinstance(spend, (int, float)) else 0.0


def _extract_offer_days(offer: Dict[str, Any]) -> float:
    days = offer.get("days")
    return float(days) if isinstance(days, (int, float)) else 0.0


def _pick_best_offer(raw_offer: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Choose a 'best' offer from raw_offer["offers"] (fallback to historicalOffers).
    Heuristic:
      - Prefer highest amount/spend ratio
      - Tie-break by highest amount
      - Tie-break by shortest days
    """
    offers = raw_offer.get("offers")
    if not isinstance(offers, list) or not offers:
        offers = raw_offer.get("historicalOffers")
        if not isinstance(offers, list) or not offers:
            return None

    best = None
    best_key: Tuple[float, float, float] = (-1.0, -1.0, -1.0)

    for o in offers:
        if not isinstance(o, dict):
            continue
        amt = _extract_offer_amount(o)
        spend = _extract_offer_spend(o)
        days = _extract_offer_days(o)
        ratio = (amt / spend) if spend > 0 else 0.0

        # We want higher ratio, higher amt, shorter days:
        key = (ratio, amt, -days)
        if key > best_key:
            best_key = key
            best = o

    return best


def _build_welcome_bonus_text(best_offer: Dict[str, Any]) -> Optional[str]:
    """
    Build a readable string:
      "70,000 bonus after $5,000 spend in 180 days"
    Units are unknown (points/miles/cash), so we keep it unitless.
    """
    if not isinstance(best_offer, dict):
        return None

    amt = _extract_offer_amount(best_offer)
    spend = _extract_offer_spend(best_offer)
    days = _extract_offer_days(best_offer)

    parts = []
    if amt > 0:
        parts.append(f"{int(amt):,} bonus")
    if spend > 0:
        parts.append(f"after ${int(spend):,} spend")
    if days > 0:
        parts.append(f"in {int(days)} days")

    return " ".join(parts) if parts else None


def normalize_creditcardbonuses_offer(raw_offer: Dict[str, Any]) -> Optional[CardOffer]:
    """
    Normalize a record from andenacitelli/credit-card-bonuses-api export JSON.

    Raw keys include:
      cardId, name, issuer, network, currency, isBusiness,
      annualFee, isAnnualFeeWaived, universalCashbackPercent,
      url, imageUrl, credits, offers, historicalOffers, discontinued

    We preserve "everything" by:
      - copying structured lists (offers, historicalOffers, credits)
      - copying metadata (card_id, network, currency, etc.)
      - storing full raw payload in `raw`
    """
    if not isinstance(raw_offer, dict):
        return None

    card_name = raw_offer.get("name")
    issuer = raw_offer.get("issuer")
    if not card_name or not issuer:
        return None

    # Core mappings
    annual_fee = raw_offer.get("annualFee")
    offer_url = raw_offer.get("url")

    # Upstream metadata
    card_id = raw_offer.get("cardId")
    network = raw_offer.get("network")
    currency = raw_offer.get("currency")
    is_business = raw_offer.get("isBusiness")
    is_annual_fee_waived = raw_offer.get("isAnnualFeeWaived")
    ucb = raw_offer.get("universalCashbackPercent")
    image_url = raw_offer.get("imageUrl")
    discontinued = raw_offer.get("discontinued")

    # Offers lists
    offers = (
        raw_offer.get("offers") if isinstance(raw_offer.get("offers"), list) else []
    )
    historical_offers = (
        raw_offer.get("historicalOffers")
        if isinstance(raw_offer.get("historicalOffers"), list)
        else []
    )
    credits = (
        raw_offer.get("credits") if isinstance(raw_offer.get("credits"), list) else []
    )

    # Reward rates: store baseline rate in a neutral key
    reward_rates = {}
    if isinstance(ucb, (int, float)) and ucb > 0:
        reward_rates["universal_base_rate"] = float(ucb)

    # Welcome bonus: pick best offer and format
    best_offer = _pick_best_offer(raw_offer)
    welcome_bonus = _build_welcome_bonus_text(best_offer) if best_offer else None

    # categories / apr / bonus_value_usd are not present in this export
    # (keep them empty/None unless you later enrich from scrapers)
    try:
        return CardOffer(
            source="creditcardbonuses",
            card_name=str(card_name),
            issuer=str(issuer),
            annual_fee=annual_fee,
            welcome_bonus=welcome_bonus,
            bonus_value_usd=None,
            reward_rates=reward_rates,
            categories=[],
            apr=None,
            offer_url=offer_url,
            # metadata
            card_id=str(card_id) if card_id else None,
            network=str(network) if network else None,
            currency=str(currency) if currency else None,
            is_business=bool(is_business) if isinstance(is_business, bool) else None,
            is_annual_fee_waived=(
                bool(is_annual_fee_waived)
                if isinstance(is_annual_fee_waived, bool)
                else None
            ),
            universal_cashback_percent=ucb,
            image_url=str(image_url) if image_url else None,
            discontinued=bool(discontinued) if isinstance(discontinued, bool) else None,
            # preserve upstream structures
            offers=offers,
            historical_offers=historical_offers,
            credits=credits,
            # preserve raw payload
            raw=raw_offer,
        )
    except Exception:
        return None

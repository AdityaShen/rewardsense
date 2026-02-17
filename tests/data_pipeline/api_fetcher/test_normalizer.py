import pytest

from src.data_pipeline.api_fetcher.normalizer import normalize_creditcardbonuses_offer
from src.data_pipeline.api_fetcher.schema import CardOffer


@pytest.mark.unit
def test_normalizer_minimal_record_export_shape():
    raw = {
        "name": "Delta SkyMiles Blue",
        "issuer": "AMERICAN_EXPRESS",
        "url": "https://example.com/card",
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert isinstance(offer, CardOffer)

    d = offer.model_dump()

    assert d["source"] == "creditcardbonuses"
    assert d["card_name"] == "Delta SkyMiles Blue"
    assert d["issuer"] == "AMERICAN_EXPRESS"
    assert d["offer_url"] == "https://example.com/card"

    # Export doesn't provide categories or apr
    assert d["categories"] == []
    assert d.get("apr") is None

    # No universalCashbackPercent provided -> reward_rates should be empty
    assert d["reward_rates"] == {}

    # New fields should exist in schema (may be None/empty depending on input)
    assert d.get("card_id") is None
    assert d.get("network") is None
    assert d.get("currency") is None
    assert d.get("offers") == []
    assert d.get("historical_offers") == []
    assert d.get("credits") == []


@pytest.mark.unit
def test_normalizer_missing_required_fields_returns_none():
    assert normalize_creditcardbonuses_offer({"issuer": "AMERICAN_EXPRESS"}) is None
    assert normalize_creditcardbonuses_offer({"name": "Test"}) is None
    assert normalize_creditcardbonuses_offer({}) is None
    assert normalize_creditcardbonuses_offer("not a dict") is None


@pytest.mark.unit
def test_normalizer_parses_annual_fee_from_export_annualFee():
    raw = {
        "name": "Test Card",
        "issuer": "Chase",
        "annualFee": 95,
        "url": "https://example.com/card",
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None
    assert offer.annual_fee == 95.0


@pytest.mark.unit
def test_normalizer_sets_universal_base_rate_from_universalCashbackPercent():
    raw = {
        "name": "Test Card",
        "issuer": "Chase",
        "universalCashbackPercent": 1,
        "url": "https://example.com/card",
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None
    # stored under neutral key in reward_rates
    assert offer.reward_rates == {"universal_base_rate": 1.0}
    # also preserved in dedicated field
    assert offer.universal_cashback_percent == 1.0


@pytest.mark.unit
def test_normalizer_extracts_welcome_bonus_from_offers_list():
    raw = {
        "name": "Test Card",
        "issuer": "AMERICAN_EXPRESS",
        "url": "https://example.com/card",
        "offers": [
            {
                "spend": 1000,
                "amount": [{"amount": 10000}],
                "days": 180,
                "credits": [],
            }
        ],
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None

    assert offer.welcome_bonus == "10,000 bonus after $1,000 spend in 180 days"
    assert offer.offers and isinstance(offer.offers, list)
    assert offer.offers[0]["spend"] == 1000
    assert offer.offers[0]["amount"][0]["amount"] == 10000
    assert offer.offers[0]["days"] == 180


@pytest.mark.unit
def test_normalizer_falls_back_to_historical_offers_if_offers_missing():
    raw = {
        "name": "Test Card",
        "issuer": "AMERICAN_EXPRESS",
        "url": "https://example.com/card",
        "historicalOffers": [
            {
                "spend": 2000,
                "amount": [{"amount": 50000}],
                "days": 90,
                "credits": [],
            }
        ],
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None

    assert offer.welcome_bonus == "50,000 bonus after $2,000 spend in 90 days"
    assert offer.historical_offers and isinstance(offer.historical_offers, list)
    assert offer.historical_offers[0]["spend"] == 2000


@pytest.mark.unit
def test_normalizer_preserves_metadata_fields():
    raw = {
        "cardId": "abc123",
        "name": "Delta SkyMiles Gold",
        "issuer": "AMERICAN_EXPRESS",
        "network": "AMERICAN_EXPRESS",
        "currency": "DELTA",
        "isBusiness": False,
        "annualFee": 150,
        "isAnnualFeeWaived": True,
        "universalCashbackPercent": 1,
        "url": "https://example.com/card",
        "imageUrl": "/images/some.jpg",
        "credits": [{"type": "statement", "amount": 100}],
        "discontinued": False,
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None

    assert offer.card_id == "abc123"
    assert offer.network == "AMERICAN_EXPRESS"
    assert offer.currency == "DELTA"
    assert offer.is_business is False
    assert offer.is_annual_fee_waived is True
    assert offer.image_url == "/images/some.jpg"
    assert offer.discontinued is False
    assert offer.credits == [{"type": "statement", "amount": 100}]


@pytest.mark.unit
def test_normalizer_includes_raw_payload_by_default():
    # Your normalizer currently sets raw=raw_offer.
    raw = {
        "name": "Test Card",
        "issuer": "Chase",
        "url": "https://example.com/card",
        "offers": [{"spend": 1, "amount": [{"amount": 2}], "days": 3, "credits": []}],
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None
    assert offer.raw is not None
    assert offer.raw["name"] == "Test Card"

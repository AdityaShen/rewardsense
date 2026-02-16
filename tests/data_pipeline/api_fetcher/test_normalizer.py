import pytest

from data_pipeline.api_fetcher.normalizer import normalize_creditcardbonuses_offer
from data_pipeline.api_fetcher.schema import CardOffer


@pytest.mark.unit
def test_normalizer_minimal_record():
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
    assert d["reward_rates"] == {}
    assert d["categories"] == []


@pytest.mark.unit
def test_normalizer_missing_required_fields_returns_none():
    assert normalize_creditcardbonuses_offer({"issuer": "Chase"}) is None
    assert normalize_creditcardbonuses_offer({"name": "Test"}) is None


@pytest.mark.unit
def test_normalizer_parses_annual_fee_string():
    raw = {
        "name": "Test Card",
        "issuer": "Chase",
        "annual_fee": "$95",
        "url": "https://example.com/card",
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None
    assert offer.annual_fee == 95.0


@pytest.mark.unit
def test_normalizer_reward_rates_from_dict():
    raw = {
        "name": "Test Card",
        "issuer": "Chase",
        "rewards": {"Dining": "3", "Travel": 2},
        "url": "https://example.com/card",
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None
    assert offer.reward_rates == {"dining": 3.0, "travel": 2.0}


@pytest.mark.unit
def test_normalizer_reward_rates_from_list():
    raw = {
        "name": "Test Card",
        "issuer": "Chase",
        "rewards": [
            {"category": "Dining", "multiplier": "3"},
            {"category": "Travel", "rate": 2},
            {"category": None, "multiplier": 5},  # ignored
        ],
        "url": "https://example.com/card",
    }
    offer = normalize_creditcardbonuses_offer(raw)
    assert offer is not None
    assert offer.reward_rates == {"dining": 3.0, "travel": 2.0}

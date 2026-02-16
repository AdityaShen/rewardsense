import pytest
from unittest.mock import patch

from data_pipeline.api_fetcher.credit_card_bonuses_api import (
    CreditCardBonusesClient,
    CreditCardBonusesConfigError,
    CreditCardBonusesUpstreamError,
)
from data_pipeline.api_fetcher.client_base import APIClientHTTPError


@pytest.mark.unit
def test_public_export_mode_selected_when_no_api_key(monkeypatch):
    monkeypatch.delenv("CREDITCARDBONUSES_API_KEY", raising=False)
    monkeypatch.delenv("CREDITCARDBONUSES_BASE_URL", raising=False)
    monkeypatch.setenv(
        "CREDITCARDBONUSES_EXPORT_URL",
        "https://raw.githubusercontent.com/andenacitelli/credit-card-bonuses-api/master/exports/data.json",
    )
    monkeypatch.setenv("CREDITCARDBONUSES_TIMEOUT_SEC", "10")

    # Avoid real HTTP by patching BaseAPIClient.get_json
    with patch.object(CreditCardBonusesClient, "get_json", return_value=[]):
        client = CreditCardBonusesClient()
        assert client.mode == "public_export"
        offers = client.fetch_current_offers()
        assert offers == []


@pytest.mark.unit
def test_keyed_api_mode_selected_when_api_key_present(monkeypatch):
    monkeypatch.setenv("CREDITCARDBONUSES_API_KEY", "dummy_key")
    monkeypatch.setenv("CREDITCARDBONUSES_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("CREDITCARDBONUSES_TIMEOUT_SEC", "10")

    with patch.object(
        CreditCardBonusesClient, "get_json", return_value={"offers": []}
    ) as mock_get:
        client = CreditCardBonusesClient()
        assert client.mode == "keyed_api"
        client.fetch_current_offers()
        mock_get.assert_called()  # ensures it tried to fetch


@pytest.mark.unit
def test_keyed_api_requires_base_url(monkeypatch):
    monkeypatch.setenv("CREDITCARDBONUSES_API_KEY", "dummy_key")
    monkeypatch.delenv("CREDITCARDBONUSES_BASE_URL", raising=False)

    with pytest.raises(CreditCardBonusesConfigError):
        CreditCardBonusesClient()


@pytest.mark.unit
def test_invalid_timeout_env_raises(monkeypatch):
    monkeypatch.delenv("CREDITCARDBONUSES_API_KEY", raising=False)
    monkeypatch.delenv("CREDITCARDBONUSES_BASE_URL", raising=False)
    monkeypatch.setenv("CREDITCARDBONUSES_EXPORT_URL", "https://example.com/data.json")
    monkeypatch.setenv("CREDITCARDBONUSES_TIMEOUT_SEC", "not-a-number")

    with pytest.raises(CreditCardBonusesConfigError):
        CreditCardBonusesClient()


@pytest.mark.unit
def test_coerce_offers_list_accepts_list(monkeypatch):
    monkeypatch.delenv("CREDITCARDBONUSES_API_KEY", raising=False)
    monkeypatch.delenv("CREDITCARDBONUSES_BASE_URL", raising=False)
    monkeypatch.setenv("CREDITCARDBONUSES_EXPORT_URL", "https://example.com/data.json")

    with patch.object(
        CreditCardBonusesClient, "get_json", return_value=[{"a": 1}, "x", {"b": 2}]
    ):
        client = CreditCardBonusesClient()
        offers = client.fetch_current_offers()
        assert offers == [{"a": 1}, {"b": 2}]  # non-dicts filtered out


@pytest.mark.unit
def test_coerce_offers_list_accepts_wrapped_offers(monkeypatch):
    monkeypatch.delenv("CREDITCARDBONUSES_API_KEY", raising=False)
    monkeypatch.delenv("CREDITCARDBONUSES_BASE_URL", raising=False)
    monkeypatch.setenv("CREDITCARDBONUSES_EXPORT_URL", "https://example.com/data.json")

    with patch.object(
        CreditCardBonusesClient, "get_json", return_value={"offers": [{"x": 1}]}
    ):
        client = CreditCardBonusesClient()
        offers = client.fetch_current_offers()
        assert offers == [{"x": 1}]


@pytest.mark.unit
def test_upstream_error_wrapping(monkeypatch):
    # Force keyed_api mode
    monkeypatch.setenv("CREDITCARDBONUSES_API_KEY", "dummy_key")
    monkeypatch.setenv("CREDITCARDBONUSES_BASE_URL", "https://api.example.com")

    # Simulate BaseAPIClient raising an HTTP error
    with patch.object(
        CreditCardBonusesClient,
        "get_json",
        side_effect=APIClientHTTPError("HTTP 500 returned"),
    ):
        client = CreditCardBonusesClient()
        with pytest.raises(CreditCardBonusesUpstreamError) as e:
            client.fetch_current_offers()

        assert "Keyed API fetch failed" in str(e.value)

import pytest
from unittest.mock import MagicMock

from data_pipeline.api_fetcher.client_base import (
    BaseAPIClient,
    APIClientError,
    APIClientHTTPError,
    APIClientTimeout,
)


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, json_raises=False):
        self.status_code = status_code
        self._json_data = json_data
        self._json_raises = json_raises

    def json(self):
        if self._json_raises:
            raise ValueError("Invalid JSON")
        return self._json_data


@pytest.mark.unit
def test_base_client_sets_default_headers():
    client = BaseAPIClient(base_url="https://example.com")
    assert client.session.headers["Accept"] == "application/json"
    assert "RewardSense" in client.session.headers["User-Agent"]


@pytest.mark.unit
def test_get_json_success():
    client = BaseAPIClient(base_url="https://example.com")
    client.session.get = MagicMock(return_value=FakeResponse(200, {"ok": True}))

    data = client.get_json("/offers")
    assert data == {"ok": True}
    client.session.get.assert_called_once()


@pytest.mark.unit
def test_get_json_http_error_raises():
    client = BaseAPIClient(base_url="https://example.com")
    client.session.get = MagicMock(return_value=FakeResponse(404, {"err": "nope"}))

    with pytest.raises(APIClientHTTPError) as e:
        client.get_json("/offers")

    assert "HTTP 404" in str(e.value)


@pytest.mark.unit
def test_get_json_invalid_json_raises():
    client = BaseAPIClient(base_url="https://example.com")
    client.session.get = MagicMock(return_value=FakeResponse(200, json_raises=True))

    with pytest.raises(APIClientError) as e:
        client.get_json("/offers")

    assert "Invalid JSON" in str(e.value)


@pytest.mark.unit
def test_get_json_timeout_raises():
    import requests

    client = BaseAPIClient(base_url="https://example.com")

    def raise_timeout(*args, **kwargs):
        raise requests.Timeout("timeout")

    client.session.get = MagicMock(side_effect=raise_timeout)

    with pytest.raises(APIClientTimeout) as e:
        client.get_json("/offers")

    assert "timed out" in str(e.value).lower()

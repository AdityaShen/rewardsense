"""
RewardSense - Unit Tests for BaseScraper

Tests for the abstract base scraper class functionality including:
- Initialization and configuration
- Rate limiting
- Session management
- Request handling
- Statistics tracking

Run with: pytest tests/test_base_scraper.py -v
"""

import pytest
import time
from unittest.mock import MagicMock, Mock, patch
from bs4 import BeautifulSoup

import sys

sys.path.insert(0, "src")

from data_pipeline.scrapers.base_scraper import BaseScraper


class ConcreteScraper(BaseScraper):
    """Concrete implementation of BaseScraper for testing purposes."""

    def get_source_name(self):
        return "TestSource"

    def get_card_list_urls(self):
        return ["https://example.com/cards"]

    def parse_card_listing(self, soup):
        return []

    def parse_card_details(self, url):
        return None


@pytest.fixture
def scraper():
    """Provides a basic scraper instance with default settings."""
    return ConcreteScraper()


@pytest.fixture
def scraper_no_rate_limit():
    """Provides a scraper with rate limiting disabled."""
    return ConcreteScraper(rate_limit=0)


@pytest.fixture
def mock_successful_response():
    """Provides a mock successful HTTP response."""
    mock = Mock()
    mock.status_code = 200
    mock.content = b"<html><body><h1>Test Page</h1></body></html>"
    mock.raise_for_status = Mock()
    return mock


@pytest.fixture
def mock_failed_response():
    """Provides a mock failed HTTP response."""
    mock = Mock()
    mock.status_code = 500
    mock.raise_for_status = Mock(side_effect=Exception("Server Error"))
    return mock


class TestBaseScraperInitialization:
    """Tests for BaseScraper initialization."""

    def test_init_with_default_values(self):
        """
        Given: No custom configuration
        When: BaseScraper is initialized
        Then: Default values should be set correctly
        """
        # Given
        # (no custom config)

        # When
        scraper = ConcreteScraper()

        # Then
        assert scraper.rate_limit == 1.0
        assert scraper.max_retries == 3
        assert scraper.timeout == 30
        assert scraper.respect_robots is True
        assert "RewardSense" in scraper.user_agent

    def test_init_with_custom_rate_limit(self):
        """
        Given: A custom rate limit of 2.5 seconds
        When: BaseScraper is initialized
        Then: Rate limit should be set to 2.5
        """
        # Given
        custom_rate_limit = 2.5

        # When
        scraper = ConcreteScraper(rate_limit=custom_rate_limit)

        # Then
        assert scraper.rate_limit == 2.5

    def test_init_with_custom_max_retries(self):
        """
        Given: A custom max_retries of 5
        When: BaseScraper is initialized
        Then: max_retries should be set to 5
        """
        # Given
        custom_retries = 5

        # When
        scraper = ConcreteScraper(max_retries=custom_retries)

        # Then
        assert scraper.max_retries == 5

    def test_init_with_custom_timeout(self):
        """
        Given: A custom timeout of 60 seconds
        When: BaseScraper is initialized
        Then: Timeout should be set to 60
        """
        # Given
        custom_timeout = 60

        # When
        scraper = ConcreteScraper(timeout=custom_timeout)

        # Then
        assert scraper.timeout == 60

    def test_init_with_custom_user_agent(self):
        """
        Given: A custom user agent string
        When: BaseScraper is initialized
        Then: User agent should match the custom value
        """
        # Given
        custom_ua = "CustomBot/2.0"

        # When
        scraper = ConcreteScraper(user_agent=custom_ua)

        # Then
        assert scraper.user_agent == "CustomBot/2.0"

    def test_init_creates_session(self):
        """
        Given: Default configuration
        When: BaseScraper is initialized
        Then: A requests session should be created
        """
        # Given / When
        scraper = ConcreteScraper()

        # Then
        assert scraper.session is not None


class TestBaseScraperStatistics:
    """Tests for scraping statistics tracking."""

    def test_stats_initialized_to_zero(self, scraper):
        """
        Given: A newly created scraper
        When: Checking initial statistics
        Then: All counters should be zero
        """
        # Given
        # (scraper fixture)

        # When
        stats = scraper.stats

        # Then
        assert stats["requests_made"] == 0
        assert stats["requests_failed"] == 0
        assert stats["cards_scraped"] == 0

    def test_stats_start_time_initially_none(self, scraper):
        """
        Given: A newly created scraper
        When: Checking start_time
        Then: It should be None
        """
        # Given / When
        stats = scraper.stats

        # Then
        assert stats["start_time"] is None
        assert stats["end_time"] is None

    @patch("requests.Session.get")
    def test_stats_increment_on_successful_request(
        self, mock_get, scraper_no_rate_limit, mock_successful_response
    ):
        """
        Given: A scraper with zero requests made
        When: A successful page fetch is performed
        Then: requests_made should increment by 1
        """
        # Given
        mock_get.return_value = mock_successful_response
        initial_count = scraper_no_rate_limit.stats["requests_made"]

        # When
        scraper_no_rate_limit.fetch_page("https://example.com")

        # Then
        assert scraper_no_rate_limit.stats["requests_made"] == initial_count + 1
        assert scraper_no_rate_limit.stats["requests_failed"] == 0

    @patch("requests.Session.get")
    def test_stats_increment_on_failed_request(self, mock_get, scraper_no_rate_limit):
        """
        Given: A scraper with zero failed requests
        When: A failed page fetch is performed
        Then: Both requests_made and requests_failed should increment
        """
        # Given
        from requests.exceptions import RequestException

        scraper_no_rate_limit.session.get = MagicMock(
            side_effect=RequestException("Connection failed")
        )

        # When
        scraper_no_rate_limit.fetch_page("https://example.com")

        # Then
        assert scraper_no_rate_limit.stats["requests_made"] == 1
        assert scraper_no_rate_limit.stats["requests_failed"] == 1

    def test_get_stats_returns_copy(self, scraper):
        """
        Given: A scraper with statistics
        When: get_stats() is called
        Then: It should return a copy of the stats dict
        """
        # Given / When
        stats = scraper.get_stats()
        stats["requests_made"] = 999

        # Then
        assert scraper.stats["requests_made"] == 0  # Original unchanged


class TestBaseScraperRateLimiting:
    """Tests for rate limiting functionality."""

    def test_rate_limit_delays_requests(self):
        """
        Given: A scraper with 0.5 second rate limit
        When: Rate limit wait is triggered after a recent request
        Then: It should wait at least 0.4 seconds
        """
        # Given
        scraper = ConcreteScraper(rate_limit=0.5)
        scraper.last_request_time = time.time()

        # When
        start = time.time()
        scraper._wait_for_rate_limit()
        elapsed = time.time() - start

        # Then
        assert elapsed >= 0.4  # Allow small tolerance

    def test_no_delay_when_rate_limit_elapsed(self):
        """
        Given: A scraper where rate limit time has already passed
        When: Rate limit wait is triggered
        Then: It should not add significant delay
        """
        # Given
        scraper = ConcreteScraper(rate_limit=0.5)
        scraper.last_request_time = time.time() - 10  # 10 seconds ago

        # When
        start = time.time()
        scraper._wait_for_rate_limit()
        elapsed = time.time() - start

        # Then
        assert elapsed < 0.2  # Should be nearly instant

    def test_zero_rate_limit_no_delay(self):
        """
        Given: A scraper with zero rate limit
        When: Rate limit wait is triggered
        Then: It should not add any significant delay
        """
        # Given
        scraper = ConcreteScraper(rate_limit=0)
        scraper.last_request_time = time.time()

        # When
        start = time.time()
        scraper._wait_for_rate_limit()
        elapsed = time.time() - start
        # Then
        assert elapsed < 0.2


class TestBaseScraperFetchPage:
    """Tests for page fetching functionality."""

    @patch("requests.Session.get")
    def test_fetch_page_returns_beautifulsoup(
        self, mock_get, scraper_no_rate_limit, mock_successful_response
    ):
        """
        Given: A valid URL and successful response
        When: fetch_page is called
        Then: It should return a BeautifulSoup object
        """
        # Given
        mock_get.return_value = mock_successful_response

        # When
        result = scraper_no_rate_limit.fetch_page("https://example.com")

        # Then
        assert result is not None
        assert isinstance(result, BeautifulSoup)

    @patch("requests.Session.get")
    def test_fetch_page_returns_none_on_failure(self, mock_get, scraper_no_rate_limit):
        """
        Given: A URL that causes a connection error
        When: fetch_page is called
        Then: It should return None
        """
        # Given
        from requests.exceptions import RequestException

        scraper_no_rate_limit.session.get = MagicMock(
            side_effect=RequestException("Connection refused")
        )

        # When
        result = scraper_no_rate_limit.fetch_page("https://example.com")

        # Then
        assert result is None

    @patch("requests.Session.get")
    def test_fetch_page_parses_html_content(
        self, mock_get, scraper_no_rate_limit, mock_successful_response
    ):
        """
        Given: A response with HTML content
        When: fetch_page is called
        Then: The HTML should be parsed correctly
        """
        # Given
        mock_get.return_value = mock_successful_response

        # When
        result = scraper_no_rate_limit.fetch_page("https://example.com")

        # Then
        assert result.find("h1").text == "Test Page"

    @patch("requests.Session.get")
    def test_fetch_page_updates_last_request_time(
        self, mock_get, scraper_no_rate_limit, mock_successful_response
    ):
        """
        Given: A scraper with old last_request_time
        When: fetch_page is called
        Then: last_request_time should be updated
        """
        # Given
        mock_get.return_value = mock_successful_response
        old_time = scraper_no_rate_limit.last_request_time

        # When
        scraper_no_rate_limit.fetch_page("https://example.com")

        # Then
        assert scraper_no_rate_limit.last_request_time > old_time


class TestBaseScraperContextManager:
    """Tests for context manager functionality."""

    def test_context_manager_returns_scraper(self):
        """
        Given: A scraper class
        When: Used as a context manager
        Then: It should return the scraper instance
        """
        # Given / When
        with ConcreteScraper() as scraper:
            # Then
            assert scraper is not None
            assert isinstance(scraper, ConcreteScraper)

    def test_context_manager_session_available(self):
        """
        Given: A scraper used as context manager
        When: Inside the context
        Then: Session should be available
        """
        # Given / When
        with ConcreteScraper() as scraper:
            # Then
            assert scraper.session is not None

    def test_context_manager_closes_session_on_exit(self):
        """
        Given: A scraper used as context manager
        When: Exiting the context
        Then: Session.close() should be called
        """
        # Given
        scraper = ConcreteScraper()
        scraper.session.close = Mock()

        # When
        with scraper:
            pass

        # Then
        scraper.session.close.assert_called_once()


class TestBaseScraperSession:
    """Tests for session configuration."""

    def test_session_has_user_agent_header(self, scraper):
        """
        Given: A scraper with default user agent
        When: Checking session headers
        Then: User-Agent header should be set
        """
        # Given / When
        headers = scraper.session.headers

        # Then
        assert "User-Agent" in headers
        assert "RewardSense" in headers["User-Agent"]

    def test_session_has_accept_header(self, scraper):
        """
        Given: A scraper
        When: Checking session headers
        Then: Accept header should be set for HTML
        """
        # Given / When
        headers = scraper.session.headers

        # Then
        assert "Accept" in headers
        assert "text/html" in headers["Accept"]


class TestBaseScraperScrapeAllCards:
    """Tests for scrape_all_cards method."""

    def test_scrape_all_cards_returns_list(self, scraper):
        """
        Given: A scraper with mocked methods
        When: scrape_all_cards is called
        Then: It should return a list
        """
        # Given
        scraper.get_card_list_urls = MagicMock(return_value=[])

        # When
        result = scraper.scrape_all_cards()

        # Then
        assert isinstance(result, list)

    def test_scrape_all_cards_sets_start_and_end_time(self, scraper):
        """
        Given: A scraper with mocked methods
        When: scrape_all_cards is called
        Then: start_time and end_time should be set
        """
        # Given
        scraper.get_card_list_urls = MagicMock(return_value=[])

        # When
        scraper.scrape_all_cards()

        # Then
        assert scraper.stats["start_time"] is not None
        assert scraper.stats["end_time"] is not None

    def test_scrape_all_cards_updates_cards_scraped_stat(self, scraper):
        """
        Given: A scraper that returns cards
        When: scrape_all_cards is called
        Then: cards_scraped stat should be updated
        """
        # Given
        scraper.get_card_list_urls = MagicMock(return_value=["https://example.com"])
        scraper.fetch_page = MagicMock(return_value=MagicMock())
        scraper.parse_card_listing = MagicMock(
            return_value=[{"name": "Card 1"}, {"name": "Card 2"}]
        )

        # When
        scraper.scrape_all_cards()

        # Then
        assert scraper.stats["cards_scraped"] == 2

    def test_scrape_all_cards_handles_failed_fetch(self, scraper):
        """
        Given: A scraper where fetch_page returns None
        When: scrape_all_cards is called
        Then: It should continue without crashing
        """
        # Given
        scraper.get_card_list_urls = MagicMock(return_value=["https://example.com"])
        scraper.fetch_page = MagicMock(return_value=None)

        # When
        result = scraper.scrape_all_cards()

        # Then
        assert result == []


class TestBaseScraperGetStats:
    """Tests for get_stats method."""

    def test_get_stats_includes_duration_when_complete(self, scraper):
        """
        Given: A scraper that has completed scraping
        When: get_stats is called
        Then: duration_seconds should be included
        """
        # Given
        from datetime import datetime, timedelta

        scraper.stats["start_time"] = datetime.now() - timedelta(seconds=10)
        scraper.stats["end_time"] = datetime.now()

        # When
        stats = scraper.get_stats()

        # Then
        assert "duration_seconds" in stats
        assert stats["duration_seconds"] >= 10


class TestBaseScraperFetchPageWithHeaders:
    """Tests for fetch_page_with_headers method."""

    def test_fetch_page_with_headers_success(self, scraper_no_rate_limit):
        """
        Given: Custom headers and a successful response
        When: fetch_page_with_headers is called
        Then: It should return BeautifulSoup object
        """
        # Given
        from bs4 import BeautifulSoup

        mock_response = MagicMock()
        mock_response.content = b"<html><body>Test</body></html>"
        mock_response.raise_for_status = MagicMock()
        scraper_no_rate_limit.session.get = MagicMock(return_value=mock_response)

        # When
        result = scraper_no_rate_limit.fetch_page_with_headers(
            "https://example.com", headers={"X-Custom": "value"}
        )

        # Then
        assert result is not None
        assert isinstance(result, BeautifulSoup)

    def test_fetch_page_with_headers_failure(self, scraper_no_rate_limit):
        """
        Given: A request that fails
        When: fetch_page_with_headers is called
        Then: It should return None and increment failed stats
        """
        # Given
        from requests.exceptions import RequestException

        scraper_no_rate_limit.session.get = MagicMock(
            side_effect=RequestException("Failed")
        )

        # When
        result = scraper_no_rate_limit.fetch_page_with_headers("https://example.com")

        # Then
        assert result is None
        assert scraper_no_rate_limit.stats["requests_failed"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

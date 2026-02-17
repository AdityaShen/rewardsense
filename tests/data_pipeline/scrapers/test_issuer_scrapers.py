"""
RewardSense - Unit Tests for Issuer Scrapers

Tests for issuer-specific scrapers:
    ✅ ChaseScraper - Full tests (working)
    ✅ DiscoverScraper - Full tests (working)
    ⏭️ AmexScraper - Skipped (TODO: needs Selenium)
    ⏭️ CitiScraper - Skipped (TODO: needs Selenium)
    ⏭️ CapitalOneScraper - Skipped (TODO: needs Selenium)

Run with: pytest tests/test_issuer_scrapers.py -v
"""

import pytest
from bs4 import BeautifulSoup

import sys

sys.path.insert(0, "src")

from data_pipeline.scrapers.issuer_scrapers import (
    ChaseScraper,
    AmexScraper,
    CitiScraper,
    CapitalOneScraper,
    DiscoverScraper,
)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def chase_scraper():
    """Provides a ChaseScraper instance."""
    return ChaseScraper()


@pytest.fixture
def discover_scraper():
    """Provides a DiscoverScraper instance."""
    return DiscoverScraper()


@pytest.fixture
def chase_html():
    """Provides sample Chase card listing HTML matching real site structure."""
    return """
    <html>
    <body>
        <div class="cardsummarylist">
            <div class="cmp-cardsummary-container">
                <div class="cmp-cardsummary--list-view selected">
                    <div class="cmp-cardsummary--list-view--personal">
                        <div class="cmp-cardsummary__inner-container">
                            <div class="cmp-cardsummary__inner-container__title">
                                <h2>Chase Freedom Unlimited® Credit Card Links to product page</h2>
                            </div>
                            <div class="cmp-cardsummary__inner-container--annual-fee">
                                <p>$0 Annual Fee</p>
                            </div>
                            <div class="cmp-cardsummary__inner-container--card-member-offer">
                                <p>NEW CARDMEMBER OFFER Earn a $200 bonus after spending $500</p>
                            </div>
                            <a href="/freedom-unlimited">Apply Now</a>
                            <div class="cmp-cardsummary__inner-container__image">
                                <img src="https://chase.com/freedom.png" alt="Card">
                            </div>
                        </div>
                    </div>
                </div>
                <div class="cmp-cardsummary--list-view selected">
                    <div class="cmp-cardsummary--list-view--personal">
                        <div class="cmp-cardsummary__inner-container">
                            <div class="cmp-cardsummary__inner-container__title">
                                <h2>Chase Sapphire Reserve® Credit Card Links to product page</h2>
                            </div>
                            <div class="cmp-cardsummary__inner-container--annual-fee">
                                <p>$550 Annual Fee</p>
                            </div>
                            <div class="cmp-cardsummary__inner-container--card-member-offer">
                                <p>NEW CARDMEMBER OFFER Earn 75,000 bonus points after spending $4,000</p>
                            </div>
                            <a href="/sapphire-reserve">Learn More</a>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def discover_html():
    """Provides sample Discover card listing HTML."""
    return """
    <html>
    <body>
        <div class="card-container">
            <h2>Discover it® Cash Back</h2>
            <p>No annual fee</p>
            <p>Earn 5% cash back on rotating categories</p>
            <a href="/credit-cards/cash-back">Learn More</a>
        </div>
        <div class="card-container">
            <h2>Discover it® Cash Back</h2>
            <p>No annual fee - duplicate entry</p>
            <a href="/credit-cards/cash-back">Learn More</a>
        </div>
        <div class="card-container">
            <h2>Discover it® Miles</h2>
            <p>No annual fee</p>
            <p>Earn 1.5X miles on every purchase</p>
            <a href="/credit-cards/miles">Apply</a>
        </div>
        <div class="card-container">
            <h3>Not A Card - Just Content</h3>
            <p>Some random content</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def empty_js_html():
    """Provides HTML that simulates a JS-rendered page (empty body)."""
    return """
    <html>
    <head><title>Credit Cards</title></head>
    <body>
        <noscript>Please enable JavaScript</noscript>
    </body>
    </html>
    """


# =============================================================================
# ChaseScraper Tests
# =============================================================================


class TestChaseScraper:
    """Tests for ChaseScraper."""

    def test_get_source_name(self, chase_scraper):
        """
        Given: A ChaseScraper instance
        When: get_source_name is called
        Then: It should return "Chase"
        """
        # Given / When
        name = chase_scraper.get_source_name()

        # Then
        assert name == "Chase"

    def test_get_card_list_urls_returns_chase_urls(self, chase_scraper):
        """
        Given: A ChaseScraper instance
        When: get_card_list_urls is called
        Then: All URLs should contain "creditcards.chase.com"
        """
        # Given / When
        urls = chase_scraper.get_card_list_urls()

        # Then
        assert len(urls) > 0
        assert all("creditcards.chase.com" in url for url in urls)

    def test_parse_card_listing_extracts_cards(self, chase_scraper, chase_html):
        """
        Given: HTML with Chase card elements
        When: parse_card_listing is called
        Then: Cards should be extracted
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)

        # Then
        assert len(cards) == 2

    def test_parse_card_listing_extracts_card_name(self, chase_scraper, chase_html):
        """
        Given: HTML with Chase cards
        When: parse_card_listing is called
        Then: Card names should be cleaned (no "Credit Card Links to..." suffix)
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)
        freedom = next((c for c in cards if "Freedom" in c.get("name", "")), None)

        # Then
        assert freedom is not None
        assert "Links to" not in freedom["name"]
        assert "Credit Card" not in freedom["name"]
        assert "Chase Freedom Unlimited" in freedom["name"]

    def test_parse_card_listing_sets_issuer_to_chase(self, chase_scraper, chase_html):
        """
        Given: HTML with Chase cards
        When: parse_card_listing is called
        Then: All cards should have issuer set to "Chase"
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)

        # Then
        for card in cards:
            assert card.get("issuer") == "Chase"

    def test_parse_card_listing_sets_source_to_chase(self, chase_scraper, chase_html):
        """
        Given: HTML with Chase cards
        When: parse_card_listing is called
        Then: All cards should have source set to "Chase"
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)

        # Then
        for card in cards:
            assert card.get("source") == "Chase"

    def test_parse_card_listing_extracts_zero_annual_fee(
        self, chase_scraper, chase_html
    ):
        """
        Given: HTML with a $0 annual fee card
        When: parse_card_listing is called
        Then: Annual fee should be 0
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)
        freedom = next((c for c in cards if "Freedom" in c.get("name", "")), None)

        # Then
        assert freedom is not None
        assert freedom.get("annual_fee") == 0

    def test_parse_card_listing_extracts_numeric_annual_fee(
        self, chase_scraper, chase_html
    ):
        """
        Given: HTML with a $550 annual fee card
        When: parse_card_listing is called
        Then: Annual fee should be 550
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)
        reserve = next((c for c in cards if "Reserve" in c.get("name", "")), None)

        # Then
        assert reserve is not None
        assert reserve.get("annual_fee") == 550

    def test_parse_card_listing_extracts_cash_bonus(self, chase_scraper, chase_html):
        """
        Given: HTML with "$200 bonus" offer
        When: parse_card_listing is called
        Then: Welcome bonus should contain "$200"
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)
        freedom = next((c for c in cards if "Freedom" in c.get("name", "")), None)

        # Then
        assert freedom is not None
        assert freedom.get("welcome_bonus") is not None
        assert "$200" in freedom["welcome_bonus"]

    def test_parse_card_listing_extracts_points_bonus(self, chase_scraper, chase_html):
        """
        Given: HTML with "75,000 bonus points" offer
        When: parse_card_listing is called
        Then: Welcome bonus should contain points info
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)
        reserve = next((c for c in cards if "Reserve" in c.get("name", "")), None)

        # Then
        assert reserve is not None
        assert reserve.get("welcome_bonus") is not None
        assert "75,000" in reserve["welcome_bonus"]

    def test_parse_card_listing_extracts_detail_url(self, chase_scraper, chase_html):
        """
        Given: HTML with card links
        When: parse_card_listing is called
        Then: Detail URLs should be absolute URLs
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)

        # Then
        cards_with_urls = [c for c in cards if c.get("detail_url")]
        assert len(cards_with_urls) > 0
        for card in cards_with_urls:
            assert card["detail_url"].startswith("http")

    def test_parse_card_listing_sets_scraped_at(self, chase_scraper, chase_html):
        """
        Given: HTML with Chase cards
        When: parse_card_listing is called
        Then: All cards should have scraped_at timestamp
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)

        # Then
        for card in cards:
            assert card.get("scraped_at") is not None

    def test_parse_card_listing_empty_html_returns_empty_list(self, chase_scraper):
        """
        Given: Empty HTML
        When: parse_card_listing is called
        Then: Should return empty list
        """
        # Given
        soup = BeautifulSoup("<html><body></body></html>", "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)

        # Then
        assert cards == []


# =============================================================================
# DiscoverScraper Tests
# =============================================================================


class TestDiscoverScraper:
    """Tests for DiscoverScraper."""

    def test_get_source_name(self, discover_scraper):
        """
        Given: A DiscoverScraper instance
        When: get_source_name is called
        Then: It should return "Discover"
        """
        # Given / When
        name = discover_scraper.get_source_name()

        # Then
        assert name == "Discover"

    def test_get_card_list_urls_returns_discover_urls(self, discover_scraper):
        """
        Given: A DiscoverScraper instance
        When: get_card_list_urls is called
        Then: All URLs should contain "discover.com"
        """
        # Given / When
        urls = discover_scraper.get_card_list_urls()

        # Then
        assert len(urls) > 0
        assert all("discover.com" in url for url in urls)

    def test_parse_card_listing_deduplicates_cards(
        self, discover_scraper, discover_html
    ):
        """
        Given: HTML with duplicate card entries
        When: parse_card_listing is called
        Then: Duplicates should be removed
        """
        # Given
        soup = BeautifulSoup(discover_html, "lxml")

        # When
        cards = discover_scraper.parse_card_listing(soup)
        card_names = [c.get("name") for c in cards]

        # Then
        # Should have 2 unique cards (Cash Back and Miles), not 3
        assert len(cards) == 2
        assert len(card_names) == len(set(card_names))  # No duplicates

    def test_parse_card_listing_filters_non_card_content(
        self, discover_scraper, discover_html
    ):
        """
        Given: HTML with non-card content
        When: parse_card_listing is called
        Then: Non-card elements should be filtered out
        """
        # Given
        soup = BeautifulSoup(discover_html, "lxml")

        # When
        cards = discover_scraper.parse_card_listing(soup)
        card_names = [c.get("name", "").lower() for c in cards]

        # Then
        assert not any("not a card" in name for name in card_names)

    def test_parse_card_listing_sets_issuer_to_discover(
        self, discover_scraper, discover_html
    ):
        """
        Given: HTML with Discover cards
        When: parse_card_listing is called
        Then: All cards should have issuer set to "Discover"
        """
        # Given
        soup = BeautifulSoup(discover_html, "lxml")

        # When
        cards = discover_scraper.parse_card_listing(soup)

        # Then
        for card in cards:
            assert card.get("issuer") == "Discover"

    def test_parse_card_listing_sets_zero_annual_fee(
        self, discover_scraper, discover_html
    ):
        """
        Given: HTML with "no annual fee" text
        When: parse_card_listing is called
        Then: Annual fee should be 0
        """
        # Given
        soup = BeautifulSoup(discover_html, "lxml")

        # When
        cards = discover_scraper.parse_card_listing(soup)

        # Then
        for card in cards:
            assert card.get("annual_fee") == 0

    def test_normalize_name_removes_special_chars(self, discover_scraper):
        """
        Given: A card name with trademark symbols
        When: _normalize_name is called
        Then: Special characters should be removed
        """
        # Given
        name = "Discover it® Cash Back™"

        # When
        normalized = discover_scraper._normalize_name(name)

        # Then
        assert "®" not in normalized
        assert "™" not in normalized
        assert "discover it cash back" == normalized


# =============================================================================
# TODO Scraper Tests (Skipped)
# =============================================================================


class TestAmexScraper:
    """Tests for AmexScraper (TODO: Needs Selenium)."""

    def test_get_source_name(self):
        """
        Given: An AmexScraper instance
        When: get_source_name is called
        Then: It should return "American Express"
        """
        # Given
        scraper = AmexScraper()

        # When
        name = scraper.get_source_name()

        # Then
        assert name == "American Express"

    def test_get_card_list_urls_returns_amex_urls(self):
        """
        Given: An AmexScraper instance
        When: get_card_list_urls is called
        Then: All URLs should contain "americanexpress.com"
        """
        # Given
        scraper = AmexScraper()

        # When
        urls = scraper.get_card_list_urls()

        # Then
        assert len(urls) > 0
        assert all("americanexpress.com" in url for url in urls)

    @pytest.mark.skip(reason="TODO: AmexScraper requires Selenium implementation")
    def test_parse_card_listing_extracts_cards(self):
        """Skipped: Requires Selenium implementation."""
        pass

    def test_parse_card_listing_returns_empty_for_js_rendered_page(self, empty_js_html):
        """
        Given: A JS-rendered page with empty body
        When: parse_card_listing is called
        Then: Should return empty list with warning
        """
        # Given
        scraper = AmexScraper()
        soup = BeautifulSoup(empty_js_html, "lxml")

        # When
        cards = scraper.parse_card_listing(soup)

        # Then
        assert cards == []


class TestCitiScraper:
    """Tests for CitiScraper (TODO: Needs Selenium)."""

    def test_get_source_name(self):
        """
        Given: A CitiScraper instance
        When: get_source_name is called
        Then: It should return "Citi"
        """
        # Given
        scraper = CitiScraper()

        # When
        name = scraper.get_source_name()

        # Then
        assert name == "Citi"

    @pytest.mark.skip(reason="TODO: CitiScraper requires Selenium implementation")
    def test_parse_card_listing_extracts_cards(self):
        """Skipped: Requires Selenium implementation."""
        pass

    def test_parse_card_listing_returns_empty_for_js_rendered_page(self, empty_js_html):
        """
        Given: A JS-rendered page with empty body
        When: parse_card_listing is called
        Then: Should return empty list with warning
        """
        # Given
        scraper = CitiScraper()
        soup = BeautifulSoup(empty_js_html, "lxml")

        # When
        cards = scraper.parse_card_listing(soup)

        # Then
        assert cards == []


class TestCapitalOneScraper:
    """Tests for CapitalOneScraper (TODO: Needs Selenium)."""

    def test_get_source_name(self):
        """
        Given: A CapitalOneScraper instance
        When: get_source_name is called
        Then: It should return "Capital One"
        """
        # Given
        scraper = CapitalOneScraper()

        # When
        name = scraper.get_source_name()

        # Then
        assert name == "Capital One"

    @pytest.mark.skip(reason="TODO: CapitalOneScraper requires Selenium implementation")
    def test_parse_card_listing_extracts_cards(self):
        """Skipped: Requires Selenium implementation."""
        pass


# =============================================================================
# Common Behavior Tests
# =============================================================================


class TestAllIssuerScrapersCommonBehavior:
    """Tests for behavior common to all issuer scrapers."""

    def test_all_scrapers_return_list_from_parse_card_listing(self):
        """
        Given: All issuer scrapers
        When: parse_card_listing is called with empty HTML
        Then: All should return a list (possibly empty)
        """
        # Given
        scrapers = [
            ChaseScraper(),
            AmexScraper(),
            CitiScraper(),
            CapitalOneScraper(),
            DiscoverScraper(),
        ]
        soup = BeautifulSoup("<html><body></body></html>", "lxml")

        # When / Then
        for scraper in scrapers:
            result = scraper.parse_card_listing(soup)
            assert isinstance(result, list), f"{scraper.get_source_name()} failed"

    def test_all_scrapers_have_non_empty_urls(self):
        """
        Given: All issuer scrapers
        When: get_card_list_urls is called
        Then: All should return at least one URL
        """
        # Given
        scrapers = [
            ChaseScraper(),
            AmexScraper(),
            CitiScraper(),
            CapitalOneScraper(),
            DiscoverScraper(),
        ]

        # When / Then
        for scraper in scrapers:
            urls = scraper.get_card_list_urls()
            assert len(urls) > 0, f"{scraper.get_source_name()} has no URLs"

    def test_all_scrapers_urls_are_https(self):
        """
        Given: All issuer scrapers
        When: get_card_list_urls is called
        Then: All URLs should use HTTPS
        """
        # Given
        scrapers = [
            ChaseScraper(),
            AmexScraper(),
            CitiScraper(),
            CapitalOneScraper(),
            DiscoverScraper(),
        ]

        # When / Then
        for scraper in scrapers:
            urls = scraper.get_card_list_urls()
            for url in urls:
                assert url.startswith(
                    "https://"
                ), f"{scraper.get_source_name()} has non-HTTPS URL: {url}"


# =============================================================================
# Data Validation Tests
# =============================================================================


class TestIssuerScrapersDataValidation:
    """Tests for data validation across issuer scrapers."""

    def test_chase_cards_have_required_fields(self, chase_scraper, chase_html):
        """
        Given: Parsed Chase cards
        When: Checking card fields
        Then: Required fields should be present
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)

        # Then
        required_fields = ["source", "issuer", "scraped_at", "name"]
        for card in cards:
            for field in required_fields:
                assert field in card, f"Missing field: {field}"

    def test_annual_fee_is_non_negative(self, chase_scraper, chase_html):
        """
        Given: Parsed cards with annual fees
        When: Checking annual fee values
        Then: All fees should be >= 0
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")

        # When
        cards = chase_scraper.parse_card_listing(soup)

        # Then
        for card in cards:
            if card.get("annual_fee") is not None:
                assert card["annual_fee"] >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

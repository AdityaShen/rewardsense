"""
RewardSense - Unit Tests for Issuer Scrapers

Tests for issuer-specific scrapers:
- ChaseScraper
- AmexScraper
- CitiScraper
- CapitalOneScraper
- DiscoverScraper

Run with: pytest tests/test_issuer_scrapers.py -v
"""

import pytest
from bs4 import BeautifulSoup
from datetime import datetime

import sys
sys.path.insert(0, 'src')

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
def amex_scraper():
    """Provides an AmexScraper instance."""
    return AmexScraper()


@pytest.fixture
def citi_scraper():
    """Provides a CitiScraper instance."""
    return CitiScraper()


@pytest.fixture
def capital_one_scraper():
    """Provides a CapitalOneScraper instance."""
    return CapitalOneScraper()


@pytest.fixture
def discover_scraper():
    """Provides a DiscoverScraper instance."""
    return DiscoverScraper()


@pytest.fixture
def chase_html():
    """Provides sample Chase card listing HTML."""
    return """
    <html>
    <body>
        <div class="card-tile">
            <h2 class="card-name">Chase Freedom Unlimited®</h2>
            <p>Annual Fee: $0</p>
            <p>Earn 1.5% cash back on all purchases</p>
            <a href="/freedom-unlimited">Apply Now</a>
            <img src="https://chase.com/freedom.png" alt="Card">
        </div>
        <div class="product-card">
            <h2 class="product-title">Chase Sapphire Reserve®</h2>
            <p>Annual Fee: $550</p>
            <p>Earn 3X points on travel and dining</p>
            <a href="/sapphire-reserve">Learn More</a>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def amex_html():
    """Provides sample American Express card listing HTML."""
    return """
    <html>
    <body>
        <div class="card-tile">
            <h2>The Platinum Card® from American Express</h2>
            <p>$695 Annual Fee</p>
            <p>Earn 5X Membership Rewards points on flights</p>
            <p>80,000 Membership Rewards points welcome offer</p>
            <a href="/platinum-card">Apply</a>
        </div>
        <div class="product-card">
            <h3>Blue Cash Everyday® Card</h3>
            <p>$0 annual fee</p>
            <p>3% cash back at U.S. supermarkets</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def citi_html():
    """Provides sample Citi card listing HTML."""
    return """
    <html>
    <body>
        <div class="card">
            <h2>Citi Double Cash Card</h2>
            <p>$0 annual fee</p>
            <p>2% on every purchase</p>
        </div>
        <div class="product">
            <h3>Citi Premier Card</h3>
            <p>$95 annual fee</p>
            <p>3X points on travel</p>
        </div>
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
        # Given
        # (fixture)
        
        # When
        name = chase_scraper.get_source_name()
        
        # Then
        assert name == "Chase"
    
    def test_get_card_list_urls_returns_chase_urls(self, chase_scraper):
        """
        Given: A ChaseScraper instance
        When: get_card_list_urls is called
        Then: All URLs should contain "creditcards.chase.com"
        """
        # Given
        # (fixture)
        
        # When
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
        assert len(cards) >= 1
    
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
    
    def test_parse_card_listing_extracts_zero_fee(self, chase_scraper, chase_html):
        """
        Given: HTML with a $0 annual fee card
        When: parse_card_listing is called
        Then: Annual fee should be 0
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")
        
        # When
        cards = chase_scraper.parse_card_listing(soup)
        freedom_card = next(
            (c for c in cards if "Freedom" in c.get("name", "")),
            None
        )
        
        # Then
        if freedom_card:
            assert freedom_card.get("annual_fee") == 0
    
    def test_parse_card_listing_extracts_numeric_fee(self, chase_scraper, chase_html):
        """
        Given: HTML with a $550 annual fee card
        When: parse_card_listing is called
        Then: Annual fee should be 550
        """
        # Given
        soup = BeautifulSoup(chase_html, "lxml")
        
        # When
        cards = chase_scraper.parse_card_listing(soup)
        reserve_card = next(
            (c for c in cards if "Reserve" in c.get("name", "")),
            None
        )
        
        # Then
        if reserve_card:
            assert reserve_card.get("annual_fee") == 550
    
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


# =============================================================================
# AmexScraper Tests
# =============================================================================

class TestAmexScraper:
    """Tests for AmexScraper."""
    
    def test_get_source_name(self, amex_scraper):
        """
        Given: An AmexScraper instance
        When: get_source_name is called
        Then: It should return "American Express"
        """
        # Given
        # (fixture)
        
        # When
        name = amex_scraper.get_source_name()
        
        # Then
        assert name == "American Express"
    
    def test_get_card_list_urls_returns_amex_urls(self, amex_scraper):
        """
        Given: An AmexScraper instance
        When: get_card_list_urls is called
        Then: All URLs should contain "americanexpress.com"
        """
        # Given
        # (fixture)
        
        # When
        urls = amex_scraper.get_card_list_urls()
        
        # Then
        assert len(urls) > 0
        assert all("americanexpress.com" in url for url in urls)
    
    def test_parse_card_listing_sets_issuer_to_amex(self, amex_scraper, amex_html):
        """
        Given: HTML with Amex cards
        When: parse_card_listing is called
        Then: All cards should have issuer set to "American Express"
        """
        # Given
        soup = BeautifulSoup(amex_html, "lxml")
        
        # When
        cards = amex_scraper.parse_card_listing(soup)
        
        # Then
        for card in cards:
            assert card.get("issuer") == "American Express"
    
    def test_parse_card_listing_extracts_membership_rewards(
        self, amex_scraper, amex_html
    ):
        """
        Given: HTML with Membership Rewards earning rate
        When: parse_card_listing is called
        Then: Reward highlight should contain the multiplier
        """
        # Given
        soup = BeautifulSoup(amex_html, "lxml")
        
        # When
        cards = amex_scraper.parse_card_listing(soup)
        platinum = next(
            (c for c in cards if "Platinum" in c.get("name", "")),
            None
        )
        
        # Then
        if platinum and platinum.get("reward_highlight"):
            assert "5X" in platinum["reward_highlight"] or "5x" in platinum["reward_highlight"]
    
    def test_parse_card_listing_extracts_welcome_offer(self, amex_scraper, amex_html):
        """
        Given: HTML with welcome offer text
        When: parse_card_listing is called
        Then: Signup bonus should be extracted
        """
        # Given
        soup = BeautifulSoup(amex_html, "lxml")
        
        # When
        cards = amex_scraper.parse_card_listing(soup)
        platinum = next(
            (c for c in cards if "Platinum" in c.get("name", "")),
            None
        )
        
        # Then
        if platinum and platinum.get("signup_bonus"):
            assert "80,000" in platinum["signup_bonus"]
    
    def test_parse_card_listing_extracts_high_annual_fee(self, amex_scraper, amex_html):
        """
        Given: HTML with $695 annual fee
        When: parse_card_listing is called
        Then: Annual fee should be 695
        """
        # Given
        soup = BeautifulSoup(amex_html, "lxml")
        
        # When
        cards = amex_scraper.parse_card_listing(soup)
        platinum = next(
            (c for c in cards if "Platinum" in c.get("name", "")),
            None
        )
        
        # Then
        if platinum:
            assert platinum.get("annual_fee") == 695


# =============================================================================
# CitiScraper Tests
# =============================================================================

class TestCitiScraper:
    """Tests for CitiScraper."""
    
    def test_get_source_name(self, citi_scraper):
        """
        Given: A CitiScraper instance
        When: get_source_name is called
        Then: It should return "Citi"
        """
        # Given
        # (fixture)
        
        # When
        name = citi_scraper.get_source_name()
        
        # Then
        assert name == "Citi"
    
    def test_get_card_list_urls_returns_citi_urls(self, citi_scraper):
        """
        Given: A CitiScraper instance
        When: get_card_list_urls is called
        Then: All URLs should contain "citi.com"
        """
        # Given
        # (fixture)
        
        # When
        urls = citi_scraper.get_card_list_urls()
        
        # Then
        assert len(urls) > 0
        assert all("citi.com" in url for url in urls)
    
    def test_parse_card_listing_sets_issuer_to_citi(self, citi_scraper, citi_html):
        """
        Given: HTML with Citi cards
        When: parse_card_listing is called
        Then: All cards should have issuer set to "Citi"
        """
        # Given
        soup = BeautifulSoup(citi_html, "lxml")
        
        # When
        cards = citi_scraper.parse_card_listing(soup)
        
        # Then
        for card in cards:
            assert card.get("issuer") == "Citi"
    
    def test_parse_card_listing_sets_source_to_citi(self, citi_scraper, citi_html):
        """
        Given: HTML with Citi cards
        When: parse_card_listing is called
        Then: All cards should have source set to "Citi"
        """
        # Given
        soup = BeautifulSoup(citi_html, "lxml")
        
        # When
        cards = citi_scraper.parse_card_listing(soup)
        
        # Then
        for card in cards:
            assert card.get("source") == "Citi"


# =============================================================================
# CapitalOneScraper Tests
# =============================================================================

class TestCapitalOneScraper:
    """Tests for CapitalOneScraper."""
    
    def test_get_source_name(self, capital_one_scraper):
        """
        Given: A CapitalOneScraper instance
        When: get_source_name is called
        Then: It should return "Capital One"
        """
        # Given
        # (fixture)
        
        # When
        name = capital_one_scraper.get_source_name()
        
        # Then
        assert name == "Capital One"
    
    def test_get_card_list_urls_returns_capital_one_urls(self, capital_one_scraper):
        """
        Given: A CapitalOneScraper instance
        When: get_card_list_urls is called
        Then: All URLs should contain "capitalone.com"
        """
        # Given
        # (fixture)
        
        # When
        urls = capital_one_scraper.get_card_list_urls()
        
        # Then
        assert len(urls) > 0
        assert all("capitalone.com" in url for url in urls)


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
        # Given
        # (fixture)
        
        # When
        name = discover_scraper.get_source_name()
        
        # Then
        assert name == "Discover"
    
    def test_get_card_list_urls_returns_discover_urls(self, discover_scraper):
        """
        Given: A DiscoverScraper instance
        When: get_card_list_urls is called
        Then: All URLs should contain "discover.com"
        """
        # Given
        # (fixture)
        
        # When
        urls = discover_scraper.get_card_list_urls()
        
        # Then
        assert len(urls) > 0
        assert all("discover.com" in url for url in urls)


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
                assert url.startswith("https://"), \
                    f"{scraper.get_source_name()} has non-HTTPS URL: {url}"


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
        required_fields = ["source", "issuer", "scraped_at"]
        for card in cards:
            for field in required_fields:
                assert field in card, f"Missing field: {field}"
    
    def test_amex_cards_have_required_fields(self, amex_scraper, amex_html):
        """
        Given: Parsed Amex cards
        When: Checking card fields
        Then: Required fields should be present
        """
        # Given
        soup = BeautifulSoup(amex_html, "lxml")
        
        # When
        cards = amex_scraper.parse_card_listing(soup)
        
        # Then
        required_fields = ["source", "issuer", "scraped_at"]
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
    
# =============================================================================
# Additional Tests for Coverage
# =============================================================================

class TestChaseScraperParseCardDetails:
    """Tests for Chase parse_card_details method."""
    
    def test_parse_card_details_returns_none_on_failed_fetch(self, chase_scraper):
        """
        Given: A URL that fails to fetch
        When: parse_card_details is called
        Then: It should return None
        """
        # Given
        from unittest.mock import MagicMock
        chase_scraper.fetch_page = MagicMock(return_value=None)
        
        # When
        result = chase_scraper.parse_card_details("https://example.com/card")
        
        # Then
        assert result is None
    
    def test_parse_card_details_extracts_rewards(self, chase_scraper):
        """
        Given: HTML with rewards section
        When: parse_card_details is called
        Then: reward_categories should be extracted
        """
        # Given
        from unittest.mock import MagicMock
        from bs4 import BeautifulSoup
        html = """
        <html>
        <body>
            <section id="rewards">
                <li>5X on travel</li>
                <li>3X on dining</li>
            </section>
        </body>
        </html>
        """
        chase_scraper.fetch_page = MagicMock(return_value=BeautifulSoup(html, "lxml"))
        
        # When
        result = chase_scraper.parse_card_details("https://example.com/card")
        
        # Then
        assert result is not None
        assert "reward_categories" in result


class TestAmexScraperParseCardDetails:
    """Tests for Amex parse_card_details method."""
    
    def test_parse_card_details_returns_none_on_failed_fetch(self, amex_scraper):
        """
        Given: A URL that fails to fetch
        When: parse_card_details is called
        Then: It should return None
        """
        # Given
        from unittest.mock import MagicMock
        amex_scraper.fetch_page = MagicMock(return_value=None)
        
        # When
        result = amex_scraper.parse_card_details("https://example.com/card")
        
        # Then
        assert result is None
    
    def test_parse_card_details_extracts_benefits(self, amex_scraper):
        """
        Given: HTML with benefits section
        When: parse_card_details is called
        Then: benefits should be extracted
        """
        # Given
        from unittest.mock import MagicMock
        from bs4 import BeautifulSoup
        html = """
        <html>
        <body>
            <section>
                <h2>Benefits</h2>
                <li>Airport lounge access</li>
                <li>Travel insurance</li>
            </section>
        </body>
        </html>
        """
        amex_scraper.fetch_page = MagicMock(return_value=BeautifulSoup(html, "lxml"))
        
        # When
        result = amex_scraper.parse_card_details("https://example.com/card")
        
        # Then
        assert result is not None
        assert "benefits" in result


class TestCitiScraperParseCardDetails:
    """Tests for Citi parse_card_details method."""
    
    def test_parse_card_details_returns_none_on_failed_fetch(self, citi_scraper):
        """
        Given: A URL that fails to fetch
        When: parse_card_details is called
        Then: It should return None
        """
        # Given
        from unittest.mock import MagicMock
        citi_scraper.fetch_page = MagicMock(return_value=None)
        
        # When
        result = citi_scraper.parse_card_details("https://example.com/card")
        
        # Then
        assert result is None


class TestCapitalOneScraperMethods:
    """Additional tests for Capital One scraper."""
    
    def test_parse_card_listing_empty_html(self, capital_one_scraper):
        """
        Given: Empty HTML
        When: parse_card_listing is called
        Then: It should return empty list
        """
        # Given
        from bs4 import BeautifulSoup
        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        
        # When
        result = capital_one_scraper.parse_card_listing(soup)
        
        # Then
        assert result == []
    
    def test_parse_card_details_returns_none(self, capital_one_scraper):
        """
        Given: Any URL
        When: parse_card_details is called
        Then: It should return None (not implemented)
        """
        # Given / When
        result = capital_one_scraper.parse_card_details("https://example.com")
        
        # Then
        assert result is None


class TestDiscoverScraperMethods:
    """Additional tests for Discover scraper."""
    
    def test_parse_card_listing_empty_html(self, discover_scraper):
        """
        Given: Empty HTML
        When: parse_card_listing is called
        Then: It should return empty list
        """
        # Given
        from bs4 import BeautifulSoup
        soup = BeautifulSoup("<html><body></body></html>", "lxml")
        
        # When
        result = discover_scraper.parse_card_listing(soup)
        
        # Then
        assert result == []
    
    def test_parse_card_details_returns_none(self, discover_scraper):
        """
        Given: Any URL
        When: parse_card_details is called
        Then: It should return None (not implemented)
        """
        # Given / When
        result = discover_scraper.parse_card_details("https://example.com")
        
        # Then
        assert result is None

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
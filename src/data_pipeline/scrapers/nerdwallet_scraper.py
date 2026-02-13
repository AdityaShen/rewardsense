"""
RewardSense - NerdWallet Scraper

Scrapes credit card data from NerdWallet's credit card listings.
NerdWallet uses JavaScript rendering, so this scraper includes
both static and dynamic content handling.

Note: For JavaScript-heavy pages, consider using SeleniumScraper instead.
"""

import re
import json
import logging
from typing import List, Dict, Any, Optional

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class NerdWalletScraper(BaseScraper):
    """
    Scraper for NerdWallet credit card data.
    
    NerdWallet is one of the most comprehensive credit card aggregators,
    providing detailed reward structures, APRs, and fees.
    """
    
    BASE_URL = "https://www.nerdwallet.com"
    
    # Category URLs to scrape
    CATEGORY_URLS = {
        "best_overall": "/best/credit-cards/best-credit-cards",
        "cash_back": "/best/credit-cards/cash-back-credit-cards",
        "travel": "/best/credit-cards/travel-credit-cards",
        "balance_transfer": "/best/credit-cards/balance-transfer-credit-cards",
        "business": "/best/credit-cards/small-business-credit-cards",
        "rewards": "/best/credit-cards/rewards-credit-cards",
        "dining": "/best/credit-cards/restaurant-credit-cards",
        "groceries": "/best/credit-cards/groceries-credit-cards",
        "gas": "/best/credit-cards/gas-credit-cards",
        "hotel": "/best/credit-cards/hotel-credit-cards",
        "airline": "/best/credit-cards/airline-credit-cards",
        "no_annual_fee": "/best/credit-cards/no-annual-fee-credit-cards",
    }
    
    def __init__(self, categories: Optional[List[str]] = None, **kwargs):
        """
        Initialize NerdWallet scraper.
        
        Args:
            categories: List of category keys to scrape (default: all)
            **kwargs: Arguments passed to BaseScraper
        """
        super().__init__(**kwargs)
        
        # Select which categories to scrape
        if categories:
            self.categories = {
                k: v for k, v in self.CATEGORY_URLS.items() 
                if k in categories
            }
        else:
            self.categories = self.CATEGORY_URLS
    
    def get_source_name(self) -> str:
        """Return the data source name."""
        return "NerdWallet"
    
    def get_card_list_urls(self) -> List[str]:
        """Return list of category URLs to scrape."""
        return [f"{self.BASE_URL}{path}" for path in self.categories.values()]
    
    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Parse a NerdWallet category page for credit card listings.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of card dictionaries with basic info
        """
        cards = []
        
        # Try to find JSON-LD structured data first (most reliable)
        json_ld = self._extract_json_ld(soup)
        if json_ld:
            cards.extend(json_ld)
            logger.info(f"Extracted {len(json_ld)} cards from JSON-LD")
        
        # Also parse HTML for additional cards
        html_cards = self._parse_html_cards(soup)
        
        # Merge, avoiding duplicates by card name
        existing_names = {c.get("name", "").lower() for c in cards}
        for card in html_cards:
            if card.get("name", "").lower() not in existing_names:
                cards.append(card)
        
        return cards
    
    def _extract_json_ld(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract card data from JSON-LD structured data."""
        cards = []
        
        # Find all JSON-LD script tags
        scripts = soup.find_all("script", type="application/ld+json")
        
        for script in scripts:
            try:
                data = json.loads(script.string)
                
                # Handle different JSON-LD formats
                if isinstance(data, list):
                    for item in data:
                        card = self._parse_json_ld_item(item)
                        if card:
                            cards.append(card)
                elif isinstance(data, dict):
                    card = self._parse_json_ld_item(data)
                    if card:
                        cards.append(card)
                        
            except json.JSONDecodeError as e:
                logger.debug(f"Failed to parse JSON-LD: {e}")
                continue
        
        return cards
    
    def _parse_json_ld_item(self, item: Dict) -> Optional[Dict[str, Any]]:
        """Parse a single JSON-LD item into card data."""
        # Check if this is a credit card product
        item_type = item.get("@type", "")
        if "Product" not in item_type and "CreditCard" not in item_type:
            return None
        
        card = {
            "source": "NerdWallet",
            "name": item.get("name"),
            "description": item.get("description"),
            "issuer": self._extract_issuer(item.get("name", "")),
            "url": item.get("url"),
            "image_url": item.get("image"),
            "scraped_at": None,  # Will be set by caller
        }
        
        # Extract offers/pricing info
        offers = item.get("offers", {})
        if isinstance(offers, dict):
            card["annual_fee"] = self._parse_price(offers.get("price"))
        
        # Extract ratings
        rating = item.get("aggregateRating", {})
        if rating:
            card["rating"] = rating.get("ratingValue")
            card["review_count"] = rating.get("reviewCount")
        
        return card if card.get("name") else None
    
    def _parse_html_cards(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse card data from HTML elements."""
        cards = []
        
        # NerdWallet uses various class patterns for card containers
        # These selectors may need updates if NerdWallet changes their HTML
        card_selectors = [
            {"class_": re.compile(r"CardProduct|card-product", re.I)},
            {"class_": re.compile(r"ProductCard|product-card", re.I)},
            {"data-testid": re.compile(r"card", re.I)},
        ]
        
        card_elements = []
        for selector in card_selectors:
            card_elements.extend(soup.find_all("div", **selector))
        
        # Remove duplicates while preserving order
        seen = set()
        unique_elements = []
        for elem in card_elements:
            elem_id = id(elem)
            if elem_id not in seen:
                seen.add(elem_id)
                unique_elements.append(elem)
        
        for element in unique_elements:
            card = self._parse_card_element(element)
            if card and card.get("name"):
                cards.append(card)
        
        return cards
    
    def _parse_card_element(self, element) -> Dict[str, Any]:
        """Parse a single card HTML element."""
        card = {
            "source": "NerdWallet",
            "scraped_at": None,
        }
        
        # Extract card name
        name_elem = (
            element.find("h2") or 
            element.find("h3") or 
            element.find(class_=re.compile(r"card-?name|product-?name", re.I))
        )
        if name_elem:
            card["name"] = name_elem.get_text(strip=True)
            card["issuer"] = self._extract_issuer(card["name"])
        
        # Extract detail URL
        link = element.find("a", href=True)
        if link:
            href = link["href"]
            if href.startswith("/"):
                card["detail_url"] = f"{self.BASE_URL}{href}"
            elif href.startswith("http"):
                card["detail_url"] = href
        
        # Extract annual fee
        fee_patterns = [
            re.compile(r"\$(\d+)\s*annual\s*fee", re.I),
            re.compile(r"annual\s*fee[:\s]*\$(\d+)", re.I),
            re.compile(r"no\s*annual\s*fee", re.I),
        ]
        
        text = element.get_text()
        for pattern in fee_patterns:
            match = pattern.search(text)
            if match:
                if "no annual fee" in match.group(0).lower():
                    card["annual_fee"] = 0
                else:
                    card["annual_fee"] = int(match.group(1))
                break
        
        # Extract reward rate (e.g., "2% cash back", "3X points")
        reward_patterns = [
            re.compile(r"(\d+(?:\.\d+)?)[%x]\s*(cash\s*back|points?|miles?)", re.I),
            re.compile(r"earn\s*(\d+(?:\.\d+)?)[%x]", re.I),
        ]
        
        for pattern in reward_patterns:
            match = pattern.search(text)
            if match:
                card["base_reward_rate"] = match.group(1)
                break
        
        # Extract sign-up bonus
        bonus_patterns = [
            re.compile(r"(\$[\d,]+|\d+[,\d]*\s*(?:points?|miles?))\s*(?:sign.?up|welcome|bonus)", re.I),
            re.compile(r"(?:sign.?up|welcome|bonus)[:\s]*(\$[\d,]+|\d+[,\d]*\s*(?:points?|miles?))", re.I),
        ]
        
        for pattern in bonus_patterns:
            match = pattern.search(text)
            if match:
                card["signup_bonus"] = match.group(1).strip()
                break
        
        return card
    
    def _extract_issuer(self, card_name: str) -> Optional[str]:
        """Extract the card issuer from the card name."""
        issuers = [
            "Chase", "American Express", "Amex", "Citi", "Capital One",
            "Discover", "Bank of America", "Wells Fargo", "Barclays",
            "U.S. Bank", "HSBC", "PNC", "TD Bank", "Navy Federal"
        ]
        
        card_name_lower = card_name.lower()
        for issuer in issuers:
            if issuer.lower() in card_name_lower:
                # Normalize "Amex" to "American Express"
                if issuer.lower() == "amex":
                    return "American Express"
                return issuer
        
        return None
    
    def _parse_price(self, price_str: Any) -> Optional[float]:
        """Parse a price string into a float."""
        if price_str is None:
            return None
        
        if isinstance(price_str, (int, float)):
            return float(price_str)
        
        # Extract numeric value from string
        match = re.search(r"[\d,]+(?:\.\d+)?", str(price_str))
        if match:
            return float(match.group().replace(",", ""))
        
        return None
    
    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        """
        Parse detailed information from a card's individual page.
        
        Args:
            card_url: URL to the card's detail page
            
        Returns:
            Dictionary with detailed card information
        """
        soup = self.fetch_page(card_url)
        if not soup:
            return None
        
        details = {
            "detail_url": card_url,
            "reward_categories": [],
            "benefits": [],
        }
        
        # Extract reward categories
        # This is highly dependent on NerdWallet's page structure
        reward_section = soup.find(
            class_=re.compile(r"reward|earning", re.I)
        )
        if reward_section:
            # Parse reward tiers
            items = reward_section.find_all("li")
            for item in items:
                text = item.get_text(strip=True)
                if text:
                    details["reward_categories"].append(text)
        
        # Extract benefits
        benefits_section = soup.find(
            class_=re.compile(r"benefit|perk", re.I)
        )
        if benefits_section:
            items = benefits_section.find_all("li")
            for item in items:
                text = item.get_text(strip=True)
                if text:
                    details["benefits"].append(text)
        
        # Extract APR
        apr_pattern = re.compile(
            r"(\d+\.?\d*)\s*%\s*[-–]\s*(\d+\.?\d*)\s*%.*?APR|"
            r"APR[:\s]*(\d+\.?\d*)\s*%",
            re.I
        )
        text = soup.get_text()
        apr_match = apr_pattern.search(text)
        if apr_match:
            if apr_match.group(1) and apr_match.group(2):
                details["apr_min"] = float(apr_match.group(1))
                details["apr_max"] = float(apr_match.group(2))
            elif apr_match.group(3):
                details["apr"] = float(apr_match.group(3))
        
        return details


class NerdWalletSeleniumScraper(BaseScraper):
    """
    Selenium-based scraper for NerdWallet when JavaScript rendering is required.
    
    Use this when the basic requests-based scraper misses dynamically loaded content.
    """
    
    def __init__(self, headless: bool = True, **kwargs):
        """
        Initialize Selenium-based scraper.
        
        Args:
            headless: Run browser in headless mode (default: True)
            **kwargs: Arguments passed to BaseScraper
        """
        super().__init__(**kwargs)
        self.headless = headless
        self.driver = None
    
    def _init_driver(self):
        """Initialize Selenium WebDriver."""
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        options = Options()
        if self.headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={self.user_agent}")
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
    
    def fetch_page_dynamic(self, url: str, wait_time: int = 3) -> Optional[BeautifulSoup]:
        """
        Fetch a page using Selenium and return parsed BeautifulSoup.
        
        Args:
            url: URL to fetch
            wait_time: Seconds to wait for JavaScript to render
            
        Returns:
            BeautifulSoup object or None
        """
        import time
        
        if not self.driver:
            self._init_driver()
        
        self._wait_for_rate_limit()
        self.stats["requests_made"] += 1
        
        try:
            logger.info(f"Fetching (Selenium): {url}")
            self.driver.get(url)
            time.sleep(wait_time)  # Wait for JS to render
            
            # Scroll to load lazy content
            self.driver.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);"
            )
            time.sleep(1)
            
            html = self.driver.page_source
            return BeautifulSoup(html, "lxml")
            
        except Exception as e:
            self.stats["requests_failed"] += 1
            logger.error(f"Selenium fetch failed for {url}: {e}")
            return None
    
    def get_source_name(self) -> str:
        return "NerdWallet (Selenium)"
    
    def get_card_list_urls(self) -> List[str]:
        # Reuse from parent class
        return NerdWalletScraper.get_card_list_urls(self)
    
    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        # Reuse parsing logic from NerdWalletScraper
        static_scraper = NerdWalletScraper()
        return static_scraper.parse_card_listing(soup)
    
    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        soup = self.fetch_page_dynamic(card_url)
        if not soup:
            return None
        
        static_scraper = NerdWalletScraper()
        return static_scraper.parse_card_details(card_url)
    
    def close(self):
        """Close the Selenium driver."""
        if self.driver:
            self.driver.quit()
            self.driver = None
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close driver and session."""
        self.close()
        super().__exit__(exc_type, exc_val, exc_tb)
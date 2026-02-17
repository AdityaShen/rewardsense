"""
RewardSense - NerdWallet Scraper (Fixed)

Updated 2026-02 with correct URLs after NerdWallet site restructure.
Old: /best/credit-cards/...
New: /credit-cards/...
"""

import re
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class NerdWalletScraper(BaseScraper):
    """
    Scraper for NerdWallet credit card data.

    NerdWallet is a comprehensive credit card aggregator.
    URLs updated 2026-02 to match current site structure.
    """

    BASE_URL = "https://www.nerdwallet.com"

    # Updated category URLs (old /best/credit-cards/ is now /credit-cards/)
    CATEGORY_URLS = {
        "all_cards": "/credit-cards",
        "best_cards": "/credit-cards/best",
        "cash_back": "/credit-cards/cash-back",
        "travel": "/credit-cards/travel",
        "balance_transfer": "/credit-cards/balance-transfer",
        "business": "/credit-cards/business",
        "rewards": "/credit-cards/rewards",
        "no_annual_fee": "/credit-cards/no-annual-fee",
        "compare": "/credit-cards/compare",
    }

    def __init__(self, categories: Optional[List[str]] = None, **kwargs):
        """
        Initialize NerdWallet scraper.

        Args:
            categories: List of category keys to scrape (default: main pages)
            **kwargs: Arguments passed to BaseScraper
        """
        super().__init__(**kwargs)

        # Default to just the main pages to avoid duplicates
        default_categories = ["all_cards", "best_cards"]

        if categories:
            self.categories = {
                k: v for k, v in self.CATEGORY_URLS.items() if k in categories
            }
        else:
            self.categories = {
                k: v for k, v in self.CATEGORY_URLS.items() if k in default_categories
            }

    def get_source_name(self) -> str:
        return "NerdWallet"

    def get_card_list_urls(self) -> List[str]:
        return [f"{self.BASE_URL}{path}" for path in self.categories.values()]

    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Parse a NerdWallet category page for credit card listings.
        """
        cards = []

        # Try JSON-LD structured data first (most reliable)
        json_ld_cards = self._extract_json_ld(soup)
        if json_ld_cards:
            cards.extend(json_ld_cards)
            logger.info(f"Extracted {len(json_ld_cards)} cards from JSON-LD")

        # Also parse HTML for additional cards
        html_cards = self._parse_html_cards(soup)

        # Merge avoiding duplicates by card name
        existing_names = {c.get("name", "").lower() for c in cards}
        for card in html_cards:
            name_lower = card.get("name", "").lower()
            if name_lower and name_lower not in existing_names:
                cards.append(card)
                existing_names.add(name_lower)

        return cards

    def _extract_json_ld(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract card data from JSON-LD structured data."""
        cards = []

        scripts = soup.find_all("script", type="application/ld+json")

        for script in scripts:
            try:
                data = json.loads(script.string)

                if isinstance(data, list):
                    for item in data:
                        card = self._parse_json_ld_item(item)
                        if card:
                            cards.append(card)
                elif isinstance(data, dict):
                    # Check if it's a product or list of products
                    if data.get("@type") in ["Product", "CreditCard"]:
                        card = self._parse_json_ld_item(data)
                        if card:
                            cards.append(card)
                    elif "itemListElement" in data:
                        for item in data.get("itemListElement", []):
                            if "item" in item:
                                item_data = item["item"]
                                # item can be a URL string or a dict
                                if isinstance(item_data, dict):
                                    card = self._parse_json_ld_item(item_data)
                                    if card:
                                        cards.append(card)

            except (json.JSONDecodeError, TypeError) as e:
                logger.debug(f"Failed to parse JSON-LD: {e}")
                continue

        return cards

    def _parse_json_ld_item(self, item: Dict) -> Optional[Dict[str, Any]]:
        """Parse a single JSON-LD item into card data."""
        item_type = item.get("@type", "")
        if not any(
            t in str(item_type) for t in ["Product", "CreditCard", "FinancialProduct"]
        ):
            return None

        name = item.get("name")
        if not name:
            return None

        card: Dict[str, Any] = {
            "source": "NerdWallet",
            "name": name,
            "description": item.get("description"),
            "issuer": self._extract_issuer(name),
            "detail_url": item.get("url"),
            "image_url": item.get("image"),
            "scraped_at": datetime.now().isoformat(),
        }

        # Extract offers/pricing info
        offers = item.get("offers", {})
        if isinstance(offers, dict):
            price = offers.get("price")
            if price is not None:
                card["annual_fee"] = self._parse_price(price)

        # Extract ratings
        rating = item.get("aggregateRating", {})
        if rating:
            card["rating"] = rating.get("ratingValue")
            card["review_count"] = rating.get("reviewCount")

        return card

    def _parse_html_cards(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse card data from HTML elements."""
        cards = []

        # Look for common card container patterns
        # NerdWallet may use various class patterns
        card_selectors = [
            ("div", {"class": re.compile(r"CardProduct|card-product", re.I)}),
            ("div", {"class": re.compile(r"ProductCard|product-card", re.I)}),
            ("div", {"data-testid": re.compile(r"card", re.I)}),
            ("article", {"class": re.compile(r"card", re.I)}),
        ]

        card_elements = []
        for tag, attrs in card_selectors:
            found = soup.find_all(tag, attrs)
            card_elements.extend(found)

        # Deduplicate elements
        seen_ids = set()
        unique_elements = []
        for elem in card_elements:
            elem_id = id(elem)
            if elem_id not in seen_ids:
                seen_ids.add(elem_id)
                unique_elements.append(elem)

        for element in unique_elements:
            card = self._parse_card_element(element)
            if card and card.get("name"):
                cards.append(card)

        return cards

    def _parse_card_element(self, element) -> Dict[str, Any]:
        """Parse a single card HTML element."""
        card: Dict[str, Any] = {
            "source": "NerdWallet",
            "scraped_at": datetime.now().isoformat(),
        }

        # Extract card name from heading
        name_elem = (
            element.find("h2")
            or element.find("h3")
            or element.find("h4")
            or element.find(class_=re.compile(r"card-?name|product-?name|title", re.I))
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

        text = element.get_text()

        # Extract annual fee
        if "no annual fee" in text.lower() or "$0 annual fee" in text.lower():
            card["annual_fee"] = 0
        else:
            fee_match = re.search(r"\$(\d+)\s*(?:annual\s*fee)?", text, re.I)
            if fee_match:
                card["annual_fee"] = int(fee_match.group(1))

        # Extract reward rate
        reward_match = re.search(
            r"(\d+(?:\.\d+)?)\s*[%xX]\s*(cash\s*back|points?|miles?)?", text, re.I
        )
        if reward_match:
            card["base_reward_rate"] = reward_match.group(0).strip()

        # Extract sign-up bonus
        bonus_patterns = [
            re.compile(
                r"(\$[\d,]+|\d+[,\d]*\s*(?:points?|miles?))\s*(?:sign.?up|welcome|bonus)",
                re.I,
            ),
            re.compile(
                r"(?:earn|get)\s+(\$[\d,]+|\d+[,\d]*)\s*(?:points?|miles?|bonus)?", re.I
            ),
        ]

        for pattern in bonus_patterns:
            match = pattern.search(text)
            if match:
                card["welcome_bonus"] = match.group(1).strip()
                break

        return card

    def _extract_issuer(self, card_name: str) -> Optional[str]:
        """Extract the card issuer from the card name."""
        issuers = [
            ("Chase", ["chase"]),
            ("American Express", ["american express", "amex"]),
            ("Citi", ["citi"]),
            ("Capital One", ["capital one"]),
            ("Discover", ["discover"]),
            ("Bank of America", ["bank of america"]),
            ("Wells Fargo", ["wells fargo"]),
            ("Barclays", ["barclays"]),
            ("U.S. Bank", ["u.s. bank", "us bank"]),
        ]

        card_name_lower = card_name.lower()
        for issuer_name, patterns in issuers:
            for pattern in patterns:
                if pattern in card_name_lower:
                    return issuer_name

        return None

    def _parse_price(self, price_str: Any) -> Optional[float]:
        """Parse a price string into a float."""
        if price_str is None:
            return None

        if isinstance(price_str, (int, float)):
            return float(price_str)

        match = re.search(r"[\d,]+(?:\.\d+)?", str(price_str))
        if match:
            return float(match.group().replace(",", ""))

        return None

    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        """Parse detailed information from a card's individual page."""
        soup = self.fetch_page(card_url)
        if not soup:
            return None

        details = {
            "detail_url": card_url,
            "reward_categories": [],
            "benefits": [],
        }

        return details


class NerdWalletSeleniumScraper(BaseScraper):
    """
    Selenium-based scraper for NerdWallet when JavaScript rendering is required.
    """

    BASE_URL = "https://www.nerdwallet.com"

    CATEGORY_URLS = NerdWalletScraper.CATEGORY_URLS

    def __init__(
        self, headless: bool = True, categories: Optional[List[str]] = None, **kwargs
    ):
        super().__init__(**kwargs)
        self.headless = headless
        self.driver = None

        default_categories = ["all_cards", "best_cards"]
        if categories:
            self.categories = {
                k: v for k, v in self.CATEGORY_URLS.items() if k in categories
            }
        else:
            self.categories = {
                k: v for k, v in self.CATEGORY_URLS.items() if k in default_categories
            }

    def _init_driver(self):
        """Initialize Selenium WebDriver."""
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options

        options = Options()
        if self.headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={self.user_agent}")

        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)

    def get_source_name(self) -> str:
        return "NerdWallet (Selenium)"

    def get_card_list_urls(self) -> List[str]:
        return [f"{self.BASE_URL}{path}" for path in self.categories.values()]

    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        # Reuse parsing logic from NerdWalletScraper
        static_scraper = NerdWalletScraper()
        return static_scraper.parse_card_listing(soup)

    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        return None

    def close(self):
        if self.driver:
            self.driver.quit()
            self.driver = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        super().__exit__(exc_type, exc_val, exc_tb)

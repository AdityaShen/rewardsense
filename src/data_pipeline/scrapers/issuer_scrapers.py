"""
RewardSense - Credit Card Issuer Scrapers

Direct scrapers for major credit card issuers:
- Chase
- American Express
- Citi
- Capital One
- Discover

These scrape directly from issuer websites for the most accurate,
up-to-date reward structures and card details.
"""

import re
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)


class ChaseScraper(BaseScraper):
    """
    Scraper for Chase credit cards.
    
    Chase provides a comprehensive card comparison page that lists
    all their consumer credit cards with key details.
    """
    
    BASE_URL = "https://creditcards.chase.com"
    
    CARD_URLS = {
        "all_cards": "/all-credit-cards",
        "cash_back": "/cash-back-credit-cards",
        "travel": "/travel-credit-cards",
        "business": "/business-credit-cards",
    }
    
    def get_source_name(self) -> str:
        return "Chase"
    
    def get_card_list_urls(self) -> List[str]:
        return [f"{self.BASE_URL}{path}" for path in self.CARD_URLS.values()]
    
    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse Chase credit card listing page."""
        cards = []
        
        # Chase uses specific data attributes for card containers
        card_elements = soup.find_all(
            "div", 
            class_=re.compile(r"card-tile|product-card|card-compare", re.I)
        )
        
        for element in card_elements:
            card = self._parse_chase_card(element)
            if card and card.get("name"):
                cards.append(card)
        
        return cards
    
    def _parse_chase_card(self, element) -> Dict[str, Any]:
        """Parse a single Chase card element."""
        card = {
            "source": "Chase",
            "issuer": "Chase",
            "scraped_at": datetime.now().isoformat(),
        }
        
        # Card name
        name_elem = element.find(class_=re.compile(r"card-?name|product-?title", re.I))
        if name_elem:
            card["name"] = name_elem.get_text(strip=True)
        
        # Annual fee
        fee_elem = element.find(string=re.compile(r"annual\s*fee", re.I))
        if fee_elem:
            parent = fee_elem.find_parent()
            if parent:
                text = parent.get_text()
                if "$0" in text or "no annual fee" in text.lower():
                    card["annual_fee"] = 0
                else:
                    match = re.search(r"\$(\d+)", text)
                    if match:
                        card["annual_fee"] = int(match.group(1))
        
        # Reward rate
        reward_elem = element.find(string=re.compile(r"earn|points?|%", re.I))
        if reward_elem:
            text = reward_elem.get_text() if hasattr(reward_elem, 'get_text') else str(reward_elem)
            # Look for patterns like "5% cash back" or "3X points"
            match = re.search(r"(\d+(?:\.\d+)?)[%xX]\s*(cash\s*back|points?|miles?)?", text)
            if match:
                card["reward_highlight"] = match.group(0)
        
        # Card URL
        link = element.find("a", href=True)
        if link:
            href = link["href"]
            if not href.startswith("http"):
                href = f"{self.BASE_URL}{href}"
            card["detail_url"] = href
        
        # Image URL
        img = element.find("img")
        if img and img.get("src"):
            card["image_url"] = img["src"]
        
        return card
    
    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        """Parse detailed Chase card page."""
        soup = self.fetch_page(card_url)
        if not soup:
            return None
        
        details = {
            "detail_url": card_url,
            "reward_categories": [],
            "benefits": [],
        }
        
        # Chase typically lists rewards in structured sections
        rewards_section = soup.find(id=re.compile(r"reward|earning", re.I))
        if rewards_section:
            items = rewards_section.find_all(["li", "p"])
            for item in items:
                text = item.get_text(strip=True)
                if text and len(text) > 10:
                    details["reward_categories"].append(text)
        
        return details


class AmexScraper(BaseScraper):
    """
    Scraper for American Express credit cards.
    
    Amex has a structured card comparison tool that provides
    detailed reward and benefit information.
    """
    
    BASE_URL = "https://www.americanexpress.com"
    
    CARD_URLS = {
        "all_cards": "/us/credit-cards/all-cards/",
        "cash_back": "/us/credit-cards/category/cash-back/",
        "travel": "/us/credit-cards/category/travel-rewards/",
        "no_fee": "/us/credit-cards/category/no-annual-fee/",
    }
    
    def get_source_name(self) -> str:
        return "American Express"
    
    def get_card_list_urls(self) -> List[str]:
        return [f"{self.BASE_URL}{path}" for path in self.CARD_URLS.values()]
    
    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse Amex credit card listing page."""
        cards = []
        
        # Amex uses specific card tile components
        card_elements = soup.find_all(
            "div",
            class_=re.compile(r"card-?tile|product-?card|compare-?card", re.I)
        )
        
        for element in card_elements:
            card = self._parse_amex_card(element)
            if card and card.get("name"):
                cards.append(card)
        
        # Also try JSON-LD extraction
        json_cards = self._extract_json_data(soup)
        
        # Merge avoiding duplicates
        existing = {c.get("name", "").lower() for c in cards}
        for jc in json_cards:
            if jc.get("name", "").lower() not in existing:
                cards.append(jc)
        
        return cards
    
    def _parse_amex_card(self, element) -> Dict[str, Any]:
        """Parse a single Amex card element."""
        card = {
            "source": "American Express",
            "issuer": "American Express",
            "scraped_at": datetime.now().isoformat(),
        }
        
        # Card name - Amex often uses heading tags
        name_elem = element.find(["h2", "h3", "h4"])
        if name_elem:
            card["name"] = name_elem.get_text(strip=True)
        
        # Annual fee
        text = element.get_text()
        if "$0 annual fee" in text.lower() or "no annual fee" in text.lower():
            card["annual_fee"] = 0
        else:
            fee_match = re.search(r"\$(\d+)\s*(?:annual\s*fee)?", text, re.I)
            if fee_match:
                card["annual_fee"] = int(fee_match.group(1))
        
        # Membership Rewards / Cash Back rate
        reward_match = re.search(
            r"(\d+)[xX]\s*(membership\s*rewards?|points?)|"
            r"(\d+(?:\.\d+)?)\s*%\s*cash\s*back",
            text, re.I
        )
        if reward_match:
            card["reward_highlight"] = reward_match.group(0)
        
        # Welcome offer
        bonus_match = re.search(
            r"(\d{2,3},?\d{3})\s*(?:membership\s*rewards?|points?|bonus)",
            text, re.I
        )
        if bonus_match:
            card["signup_bonus"] = bonus_match.group(0)
        
        # Detail URL
        link = element.find("a", href=True)
        if link:
            href = link["href"]
            if not href.startswith("http"):
                href = f"{self.BASE_URL}{href}"
            card["detail_url"] = href
        
        return card
    
    def _extract_json_data(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract card data from embedded JSON."""
        cards = []
        
        # Look for script tags with card data
        scripts = soup.find_all("script", type="application/json")
        scripts.extend(soup.find_all("script", type="application/ld+json"))
        
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and "cards" in data:
                    for item in data["cards"]:
                        card = {
                            "source": "American Express",
                            "issuer": "American Express",
                            "name": item.get("name"),
                            "annual_fee": item.get("annualFee"),
                            "scraped_at": datetime.now().isoformat(),
                        }
                        if card["name"]:
                            cards.append(card)
            except (json.JSONDecodeError, TypeError):
                continue
        
        return cards
    
    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        """Parse detailed Amex card page."""
        soup = self.fetch_page(card_url)
        if not soup:
            return None
        
        details = {
            "detail_url": card_url,
            "reward_categories": [],
            "benefits": [],
        }
        
        # Parse reward earning structure
        earning_section = soup.find(
            string=re.compile(r"earn|reward", re.I)
        )
        if earning_section:
            parent = earning_section.find_parent("section")
            if parent:
                items = parent.find_all("li")
                for item in items:
                    details["reward_categories"].append(item.get_text(strip=True))
        
        # Parse card benefits
        benefits_section = soup.find(
            string=re.compile(r"benefit|perk|feature", re.I)
        )
        if benefits_section:
            parent = benefits_section.find_parent("section")
            if parent:
                items = parent.find_all("li")
                for item in items:
                    details["benefits"].append(item.get_text(strip=True))
        
        return details


class CitiScraper(BaseScraper):
    """
    Scraper for Citi credit cards.
    """
    
    BASE_URL = "https://www.citi.com"
    
    CARD_URLS = {
        "all_cards": "/credit-cards/compare/all-credit-cards",
        "cash_back": "/credit-cards/compare/cash-back-credit-cards",
        "travel": "/credit-cards/compare/travel-credit-cards",
    }
    
    def get_source_name(self) -> str:
        return "Citi"
    
    def get_card_list_urls(self) -> List[str]:
        return [f"{self.BASE_URL}{path}" for path in self.CARD_URLS.values()]
    
    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse Citi credit card listing page."""
        cards = []
        
        card_elements = soup.find_all(
            "div",
            class_=re.compile(r"card|product", re.I)
        )
        
        for element in card_elements:
            card = self._parse_citi_card(element)
            if card and card.get("name"):
                cards.append(card)
        
        return cards
    
    def _parse_citi_card(self, element) -> Dict[str, Any]:
        """Parse a single Citi card element."""
        card = {
            "source": "Citi",
            "issuer": "Citi",
            "scraped_at": datetime.now().isoformat(),
        }
        
        # Card name
        name_elem = element.find(["h2", "h3", "h4"])
        if name_elem:
            card["name"] = name_elem.get_text(strip=True)
        
        # Annual fee
        text = element.get_text()
        if "$0" in text:
            card["annual_fee"] = 0
        else:
            fee_match = re.search(r"\$(\d+)", text)
            if fee_match:
                card["annual_fee"] = int(fee_match.group(1))
        
        # Rewards
        reward_match = re.search(r"(\d+)[%xX]", text)
        if reward_match:
            card["reward_highlight"] = reward_match.group(0)
        
        return card
    
    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        """Parse detailed Citi card page."""
        soup = self.fetch_page(card_url)
        if not soup:
            return None
        
        return {
            "detail_url": card_url,
            "reward_categories": [],
            "benefits": [],
        }


class CapitalOneScraper(BaseScraper):
    """Scraper for Capital One credit cards."""
    
    BASE_URL = "https://www.capitalone.com"
    
    CARD_URLS = {
        "all_cards": "/credit-cards/",
    }
    
    def get_source_name(self) -> str:
        return "Capital One"
    
    def get_card_list_urls(self) -> List[str]:
        return [f"{self.BASE_URL}{path}" for path in self.CARD_URLS.values()]
    
    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse Capital One card listing."""
        cards = []
        
        card_elements = soup.find_all(
            "div",
            class_=re.compile(r"card|product", re.I)
        )
        
        for element in card_elements:
            card = {
                "source": "Capital One",
                "issuer": "Capital One",
                "scraped_at": datetime.now().isoformat(),
            }
            
            name_elem = element.find(["h2", "h3"])
            if name_elem:
                card["name"] = name_elem.get_text(strip=True)
            
            if card.get("name"):
                cards.append(card)
        
        return cards
    
    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        return None


class DiscoverScraper(BaseScraper):
    """Scraper for Discover credit cards."""
    
    BASE_URL = "https://www.discover.com"
    
    CARD_URLS = {
        "all_cards": "/credit-cards/",
    }
    
    def get_source_name(self) -> str:
        return "Discover"
    
    def get_card_list_urls(self) -> List[str]:
        return [f"{self.BASE_URL}{path}" for path in self.CARD_URLS.values()]
    
    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse Discover card listing."""
        cards = []
        
        card_elements = soup.find_all(
            "div",
            class_=re.compile(r"card|product", re.I)
        )
        
        for element in card_elements:
            card = {
                "source": "Discover",
                "issuer": "Discover",
                "scraped_at": datetime.now().isoformat(),
            }
            
            name_elem = element.find(["h2", "h3"])
            if name_elem:
                card["name"] = name_elem.get_text(strip=True)
            
            if card.get("name"):
                cards.append(card)
        
        return cards
    
    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        return None
"""
RewardSense - Base Scraper Module

Abstract base class for all credit card scrapers.
Provides common functionality like rate limiting, retries, and logging.
"""

import time
import random
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    Abstract base class for credit card data scrapers.

    All scrapers should inherit from this class and implement
    the abstract methods for their specific data source.
    """

    def __init__(
        self,
        rate_limit: float = 1.0,
        max_retries: int = 3,
        timeout: int = 30,
        user_agent: Optional[str] = None,
        respect_robots: bool = True,
    ) -> None:
        """
        Initialize the base scraper.

        Args:
            rate_limit: Minimum seconds between requests (default: 1.0)
            max_retries: Maximum retry attempts for failed requests (default: 3)
            timeout: Request timeout in seconds (default: 30)
            user_agent: Custom user agent string (default: RewardSense bot)
            respect_robots: Whether to respect robots.txt (default: True)
        """
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.timeout = timeout
        self.respect_robots = respect_robots
        self.last_request_time: float = 0.0

        # Default user agent
        self.user_agent = user_agent or (
            "RewardSense/1.0 (Educational Project; "
            "https://github.com/avadharj/rewardsense)"
        )

        # Set up session with retry logic
        self.session = self._create_session()

        # Track scraping statistics
        # Explicit type annotation to avoid mypy inference issues
        self.stats: Dict[str, Union[int, Optional[datetime]]] = {
            "requests_made": 0,
            "requests_failed": 0,
            "cards_scraped": 0,
            "start_time": None,
            "end_time": None,
        }

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": (
                    "text/html,application/xhtml+xml," "application/xml;q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }
        )

        return session

    def _wait_for_rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            # Add small random jitter to avoid patterns
            sleep_time = self.rate_limit - elapsed + random.uniform(0.1, 0.5)
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """
        Fetch a page and return parsed BeautifulSoup object.

        Args:
            url: URL to fetch

        Returns:
            BeautifulSoup object or None if request failed
        """
        self._wait_for_rate_limit()
        requests_made = self.stats["requests_made"]
        if isinstance(requests_made, int):
            self.stats["requests_made"] = requests_made + 1

        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()

            return BeautifulSoup(response.content, "lxml")

        except requests.exceptions.RequestException as e:
            requests_failed = self.stats["requests_failed"]
            if isinstance(requests_failed, int):
                self.stats["requests_failed"] = requests_failed + 1
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    def fetch_page_with_headers(
        self, url: str, headers: Optional[Dict[str, str]] = None
    ) -> Optional[BeautifulSoup]:
        """
        Fetch a page with custom headers.

        Args:
            url: URL to fetch
            headers: Additional headers to include

        Returns:
            BeautifulSoup object or None if request failed
        """
        self._wait_for_rate_limit()
        requests_made = self.stats["requests_made"]
        if isinstance(requests_made, int):
            self.stats["requests_made"] = requests_made + 1

        try:
            logger.info(f"Fetching (custom headers): {url}")
            response = self.session.get(
                url, timeout=self.timeout, headers=headers or {}
            )
            response.raise_for_status()

            return BeautifulSoup(response.content, "lxml")

        except requests.exceptions.RequestException as e:
            requests_failed = self.stats["requests_failed"]
            if isinstance(requests_failed, int):
                self.stats["requests_failed"] = requests_failed + 1
            logger.error(f"Failed to fetch {url}: {e}")
            return None

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the name of the data source (e.g., 'NerdWallet')."""
        pass

    @abstractmethod
    def get_card_list_urls(self) -> List[str]:
        """Return list of URLs to scrape for card listings."""
        pass

    @abstractmethod
    def parse_card_listing(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Parse a card listing page and extract basic card info.

        Args:
            soup: BeautifulSoup object of the listing page

        Returns:
            List of dictionaries with card info
        """
        pass

    @abstractmethod
    def parse_card_details(self, card_url: str) -> Optional[Dict[str, Any]]:
        """
        Parse detailed information for a specific card.

        Args:
            card_url: URL to the card's detail page

        Returns:
            Dictionary with detailed card information or None
        """
        pass

    def scrape_all_cards(self) -> List[Dict[str, Any]]:
        """
        Main method to scrape all credit cards from this source.

        Returns:
            List of dictionaries containing card data
        """
        self.stats["start_time"] = datetime.now()
        all_cards: List[Dict[str, Any]] = []

        logger.info(f"Starting scrape for {self.get_source_name()}")

        # Get all listing URLs
        listing_urls = self.get_card_list_urls()
        logger.info(f"Found {len(listing_urls)} listing pages to scrape")

        # Scrape each listing page
        for url in listing_urls:
            soup = self.fetch_page(url)
            if soup:
                cards = self.parse_card_listing(soup)
                logger.info(f"Found {len(cards)} cards on {url}")
                all_cards.extend(cards)

        self.stats["cards_scraped"] = len(all_cards)
        self.stats["end_time"] = datetime.now()

        logger.info(
            f"Scrape complete for {self.get_source_name()}: "
            f"{len(all_cards)} cards scraped"
        )

        return all_cards

    def get_stats(self) -> Dict[str, Any]:
        """Return scraping statistics."""
        stats: Dict[str, Any] = dict(self.stats)
        start_time = stats.get("start_time")
        end_time = stats.get("end_time")
        if isinstance(start_time, datetime) and isinstance(end_time, datetime):
            stats["duration_seconds"] = (end_time - start_time).total_seconds()
        return stats

    def __enter__(self) -> "BaseScraper":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Context manager exit - close session."""
        self.session.close()

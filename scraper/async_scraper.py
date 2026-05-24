"""
Async HTTP scraper using aiohttp with BeautifulSoup parsing.
Suitable for static HTML pages with high throughput requirements.
"""
import asyncio
from typing import Optional, Callable
from datetime import datetime

import aiohttp
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, ScrapeResult


class AsyncScraper(BaseScraper):
    """
    High-throughput async scraper using aiohttp.

    Best for: Static HTML pages, APIs, large-scale scraping jobs.

    Example:
        scraper = AsyncScraper(rate_limit=5, max_retries=3)
        results = await scraper.scrape_urls(urls, concurrency=20)
    """

    def __init__(
        self,
        rate_limit: float = 2.0,
        max_retries: int = 3,
        timeout: int = 30,
        headers: Optional[dict] = None,
        proxy: Optional[str] = None,
    ):
        super().__init__(rate_limit=rate_limit, max_retries=max_retries, timeout=timeout)
        self.headers = headers or {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        self.proxy = proxy
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            connector=aiohttp.TCPConnector(limit=100, ssl=False),
        )
        return self

    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session:
            self._session = aiohttp.ClientSession(
                headers=self.headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            )
        return self._session

    async def _fetch(self, url: str, **kwargs) -> ScrapeResult:
        session = await self._get_session()
        async with session.get(url, proxy=self.proxy) as response:
            content = await response.text()
            return ScrapeResult(
                url=url,
                status_code=response.status,
                content=content,
                metadata={"headers": dict(response.headers)},
            )

    def parse_html(
        self,
        result: ScrapeResult,
        parser: str = "html.parser",
    ) -> Optional[BeautifulSoup]:
        """Parse scraped HTML content into BeautifulSoup object."""
        if not result.success or not result.content:
            return None
        return BeautifulSoup(result.content, parser)

    def extract_links(self, result: ScrapeResult, base_url: str = "") -> list[str]:
        """Extract all hyperlinks from a scraped page."""
        soup = self.parse_html(result)
        if not soup:
            return []

        links = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            if href.startswith("http"):
                links.append(href)
            elif base_url and href.startswith("/"):
                links.append(f"{base_url.rstrip('/')}{href}")
        return links

    def extract_structured_data(
        self,
        result: ScrapeResult,
        selectors: dict[str, str],
    ) -> dict:
        """
        Extract structured data using CSS selectors.

        Args:
            result: ScrapeResult from scraping
            selectors: dict mapping field names to CSS selectors
                       e.g. {"title": "h1.product-title", "price": "span.price"}

        Returns:
            dict with extracted values
        """
        soup = self.parse_html(result)
        if not soup:
            return {}

        data = {"url": result.url, "scraped_at": result.scraped_at.isoformat()}
        for field_name, selector in selectors.items():
            element = soup.select_one(selector)
            data[field_name] = element.get_text(strip=True) if element else None

        return data

    async def scrape_with_transform(
        self,
        urls: list[str],
        transform_fn: Callable[[ScrapeResult], dict],
        concurrency: int = 10,
    ) -> list[dict]:
        """
        Scrape URLs and apply a transformation function to each result.

        Args:
            urls: List of URLs to scrape
            transform_fn: Function to transform ScrapeResult into dict
            concurrency: Max concurrent requests

        Returns:
            List of transformed results
        """
        results = await self.scrape_urls(urls, concurrency=concurrency)
        return [transform_fn(r) for r in results if r.success]

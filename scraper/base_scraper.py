"""
Base scraper with retry logic, rate limiting, and structured logging.
"""

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ScrapeResult:
    url: str
    status_code: int
    content: Optional[str]
    scraped_at: datetime = field(default_factory=datetime.utcnow)
    duration_ms: float = 0.0
    error: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.error is None and self.status_code == 200

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "status_code": self.status_code,
            "content": self.content,
            "scraped_at": self.scraped_at.isoformat(),
            "duration_ms": self.duration_ms,
            "error": self.error,
            "metadata": self.metadata,
        }


class RateLimiter:
    """Token bucket rate limiter for controlling request frequency."""

    def __init__(self, rate: float):
        self.rate = rate  # requests per second
        self.tokens = rate
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now

            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class BaseScraper(ABC):
    """
    Abstract base scraper with retry logic, rate limiting, and logging.

    All scrapers inherit from this class to ensure consistent behavior
    across different scraping strategies (async HTTP, Playwright, etc.)
    """

    def __init__(
        self,
        rate_limit: float = 2.0,
        max_retries: int = 3,
        timeout: int = 30,
        backoff_factor: float = 2.0,
    ):
        self.rate_limiter = RateLimiter(rate_limit)
        self.max_retries = max_retries
        self.timeout = timeout
        self.backoff_factor = backoff_factor
        self.logger = structlog.get_logger(self.__class__.__name__)

    @abstractmethod
    async def _fetch(self, url: str, **kwargs) -> ScrapeResult:
        """Implement the actual HTTP fetch logic."""
        pass

    async def scrape(self, url: str, **kwargs) -> ScrapeResult:
        """
        Scrape a single URL with retry logic and rate limiting.
        """
        await self.rate_limiter.acquire()

        for attempt in range(1, self.max_retries + 1):
            start_time = time.monotonic()
            try:
                self.logger.info("scraping_url", url=url, attempt=attempt)
                result = await self._fetch(url, **kwargs)
                result.duration_ms = (time.monotonic() - start_time) * 1000

                self.logger.info(
                    "scrape_success",
                    url=url,
                    status_code=result.status_code,
                    duration_ms=result.duration_ms,
                )
                return result

            except Exception as e:
                duration_ms = (time.monotonic() - start_time) * 1000
                self.logger.warning(
                    "scrape_failed",
                    url=url,
                    attempt=attempt,
                    error=str(e),
                    duration_ms=duration_ms,
                )

                if attempt == self.max_retries:
                    return ScrapeResult(
                        url=url,
                        status_code=0,
                        content=None,
                        duration_ms=duration_ms,
                        error=str(e),
                    )

                backoff = self.backoff_factor**attempt
                self.logger.info("retrying", url=url, backoff_seconds=backoff)
                await asyncio.sleep(backoff)

    async def scrape_urls(
        self, urls: list[str], concurrency: int = 10, **kwargs
    ) -> list[ScrapeResult]:
        """
        Scrape multiple URLs concurrently with a concurrency limit.
        """
        semaphore = asyncio.Semaphore(concurrency)

        async def bounded_scrape(url: str) -> ScrapeResult:
            async with semaphore:
                return await self.scrape(url, **kwargs)

        self.logger.info("starting_batch_scrape", total_urls=len(urls))
        results = await asyncio.gather(*[bounded_scrape(url) for url in urls])

        successful = sum(1 for r in results if r.success)
        self.logger.info(
            "batch_scrape_complete",
            total=len(urls),
            successful=successful,
            failed=len(urls) - successful,
        )
        return list(results)

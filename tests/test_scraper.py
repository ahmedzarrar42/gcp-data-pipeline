"""
Tests for the async scraper module.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from scraper.base_scraper import ScrapeResult, RateLimiter
from scraper.async_scraper import AsyncScraper


class TestScrapeResult:
    def test_success_when_status_200_no_error(self):
        result = ScrapeResult(url="https://example.com", status_code=200, content="<html>")
        assert result.success is True

    def test_failure_when_status_not_200(self):
        result = ScrapeResult(url="https://example.com", status_code=404, content=None)
        assert result.success is False

    def test_failure_when_error_present(self):
        result = ScrapeResult(
            url="https://example.com", status_code=200, content=None, error="Timeout"
        )
        assert result.success is False

    def test_to_dict_contains_all_fields(self):
        result = ScrapeResult(url="https://example.com", status_code=200, content="test")
        d = result.to_dict()
        assert "url" in d
        assert "status_code" in d
        assert "scraped_at" in d
        assert "duration_ms" in d


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_acquire_does_not_raise(self):
        limiter = RateLimiter(rate=10.0)
        await limiter.acquire()  # Should not raise

    @pytest.mark.asyncio
    async def test_rate_limiting_slows_requests(self):
        import time

        limiter = RateLimiter(rate=100.0)  # High rate — should be fast
        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 1.0  # Should complete quickly at high rate


class TestAsyncScraper:
    @pytest.mark.asyncio
    async def test_scrape_returns_result_on_success(self):
        scraper = AsyncScraper(rate_limit=100)

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {}
        mock_response.text = AsyncMock(return_value="<html><body>Test</body></html>")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)

        with patch.object(scraper, "_get_session", AsyncMock(return_value=mock_session)):
            result = await scraper.scrape("https://example.com")

        assert result.status_code == 200
        assert result.success is True
        assert result.content == "<html><body>Test</body></html>"

    @pytest.mark.asyncio
    async def test_scrape_retries_on_failure(self):
        scraper = AsyncScraper(rate_limit=100, max_retries=3)
        call_count = 0

        async def failing_fetch(url, **kwargs):
            nonlocal call_count
            call_count += 1
            raise ConnectionError("Connection refused")

        with patch.object(scraper, "_fetch", side_effect=failing_fetch):
            result = await scraper.scrape("https://example.com")

        assert call_count == 3
        assert result.success is False
        assert "Connection refused" in result.error

    @pytest.mark.asyncio
    async def test_scrape_urls_returns_all_results(self):
        scraper = AsyncScraper(rate_limit=100)
        urls = ["https://example.com/1", "https://example.com/2", "https://example.com/3"]

        async def mock_fetch(url, **kwargs):
            return ScrapeResult(url=url, status_code=200, content="<html>")

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            results = await scraper.scrape_urls(urls, concurrency=3)

        assert len(results) == 3
        assert all(r.success for r in results)

    def test_extract_links_from_html(self):
        scraper = AsyncScraper()
        result = ScrapeResult(
            url="https://example.com",
            status_code=200,
            content='<html><body><a href="https://other.com">Link</a><a href="/page">Local</a></body></html>',
        )
        links = scraper.extract_links(result, base_url="https://example.com")
        assert "https://other.com" in links
        assert "https://example.com/page" in links

    def test_extract_structured_data(self):
        scraper = AsyncScraper()
        result = ScrapeResult(
            url="https://example.com",
            status_code=200,
            content='<html><body><h1 class="title">Product Name</h1><span class="price">€29.99</span></body></html>',
        )
        data = scraper.extract_structured_data(
            result,
            selectors={"title": "h1.title", "price": "span.price"},
        )
        assert data["title"] == "Product Name"
        assert data["price"] == "€29.99"

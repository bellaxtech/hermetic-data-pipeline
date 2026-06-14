"""
Asynchronous HTTP Collector

An async HTTP collector that uses httpx.AsyncClient to fetch data from
internal API endpoints, collecting data in batches with pagination
and rate limiting support.

Features:
    - Concurrent async HTTP requests via httpx
    - Automatic pagination handling (page-based and cursor-based)
    - Rate limiting with configurable requests per second
    - Retry with exponential backoff on transient failures
    - Batch processing with configurable batch sizes
    - Structured logging with correlation IDs

Usage:
    from ingestion.async_collector import AsyncDataCollector, CollectorConfig

    config = CollectorConfig(
        base_url="https://api.example.com",
        endpoint="/v1/orders",
        api_key="sk-...",
        page_size=100,
        max_concurrent=10,
    )

    collector = AsyncDataCollector(config)
    results = await collector.collect_all()

Requirements:
    httpx >= 0.25
    pydantic >= 2.0
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Callable

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class CollectorConfig:
    """Configuration for the async data collector.

    Attributes:
        base_url: Base URL for the API (e.g., https://api.example.com).
        endpoint: API endpoint path (e.g., /v1/orders).
        api_key: API key for authentication.
        page_size: Number of items per page (default: 100).
        max_concurrent: Maximum concurrent requests (default: 10).
        rate_limit: Maximum requests per second (default: 50).
        max_retries: Maximum retry attempts per request (default: 3).
        timeout: Request timeout in seconds (default: 30).
        pagination_type: 'page' or 'cursor' (default: 'page').
        extra_headers: Additional headers to include in requests.
        extract_items_fn: Custom function to extract items from response JSON.
            Defaults to response['data'].
        extract_next_token_fn: Custom function to extract next page/cursor token.
            Defaults to response['next_page'] or response.get('next_cursor').
    """

    base_url: str
    endpoint: str
    api_key: str
    page_size: int = 100
    max_concurrent: int = 10
    rate_limit: float = 50.0  # requests per second
    max_retries: int = 3
    timeout: float = 30.0
    pagination_type: str = "page"  # 'page' or 'cursor'
    extra_headers: dict[str, str] = field(default_factory=dict)
    extract_items_fn: Callable[[dict[str, Any]], list[dict[str, Any]]] | None = None
    extract_next_token_fn: Callable[[dict[str, Any]], str | None] | None = None


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------
class RateLimiter:
    """Simple token-bucket rate limiter for controlling request frequency."""

    def __init__(self, max_rate: float, time_period: float = 1.0) -> None:
        """
        Args:
            max_rate: Maximum number of requests per time_period.
            time_period: Time period in seconds (default: 1.0).
        """
        self.max_rate = max_rate
        self.time_period = time_period
        self.tokens = max_rate
        self.updated_at = time.monotonic()

    async def acquire(self) -> None:
        """Wait for a token to become available."""
        while True:
            now = time.monotonic()
            elapsed = now - self.updated_at
            self.tokens = min(self.max_rate, self.tokens + elapsed * (self.max_rate / self.time_period))
            self.updated_at = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return

            sleep_time = (1.0 - self.tokens) * (self.time_period / self.max_rate)
            await asyncio.sleep(max(sleep_time, 0.001))


# ---------------------------------------------------------------------------
# Abstract Collector
# ---------------------------------------------------------------------------
class BaseCollector(ABC):
    """Abstract base for async data collectors."""

    @abstractmethod
    async def collect_all(self) -> list[dict[str, Any]]:
        """Collect all data from the API endpoint.

        Returns:
            List of all collected items.
        """
        ...

    @abstractmethod
    async def collect_batch(self, batch_size: int) -> list[dict[str, Any]]:
        """Collect a specific number of items.

        Args:
            batch_size: Maximum number of items to collect.

        Returns:
            List of collected items up to batch_size.
        """
        ...


# ---------------------------------------------------------------------------
# Async HTTP Collector
# ---------------------------------------------------------------------------
class AsyncDataCollector(BaseCollector):
    """Async HTTP collector with pagination and rate limiting."""

    def __init__(self, config: CollectorConfig) -> None:
        """
        Args:
            config: Collector configuration.
        """
        self.config = config
        self.correlation_id = uuid.uuid4().hex[:12]
        self.rate_limiter = RateLimiter(config.rate_limit)
        self._session: httpx.AsyncClient | None = None
        self._stats: dict[str, Any] = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_items": 0,
            "start_time": None,
            "end_time": None,
        }

        # Set up the logger adapter with correlation ID
        self.logger = logging.LoggerAdapter(
            logger,
            {"correlation_id": self.correlation_id},
        )

    async def __aenter__(self) -> "AsyncDataCollector":
        """Enter async context manager."""
        await self._ensure_session()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context manager and close session."""
        await self.close()

    async def _ensure_session(self) -> httpx.AsyncClient:
        """Get or create the underlying httpx session."""
        if self._session is None or self._session.is_closed:
            headers = {
                "Authorization": f"Bearer {self.config.api_key}",
                "Accept": "application/json",
                "User-Agent": "AsyncDataCollector/1.0",
                **self.config.extra_headers,
            }
            limits = httpx.Limits(
                max_keepalive_connections=self.config.max_concurrent,
                max_connections=self.config.max_concurrent * 2,
            )
            self._session = httpx.AsyncClient(
                base_url=self.config.base_url,
                headers=headers,
                timeout=self.config.timeout,
                limits=limits,
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.is_closed:
            await self._session.aclose()
            self._session = None

    def _extract_items(self, response_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract items from response JSON.

        Uses custom extract function if provided, otherwise defaults
        to response['data'].
        """
        if self.config.extract_items_fn:
            return self.config.extract_items_fn(response_data)
        return response_data.get("data", [])

    def _extract_next_token(self, response_data: dict[str, Any]) -> str | None:
        """Extract the next page/cursor token from response.

        Uses custom extract function if provided, otherwise defaults
        to response['next_page'] (page-based) or response.get('next_cursor') (cursor).
        """
        if self.config.extract_next_token_fn:
            return self.config.extract_next_token_fn(response_data)

        if self.config.pagination_type == "cursor":
            return response_data.get("next_cursor")
        return response_data.get("next_page")

    async def _fetch_page(
        self,
        params: dict[str, Any],
        retry_count: int = 0,
    ) -> dict[str, Any] | None:
        """Fetch a single page from the API with retry logic.

        Args:
            params: Query parameters for the request.
            retry_count: Current attempt number (starts at 0).

        Returns:
            Response JSON dict, or None on failure after max retries.
        """
        await self.rate_limiter.acquire()
        session = await self._ensure_session()
        self._stats["total_requests"] += 1

        try:
            response = await session.get(self.config.endpoint, params=params)
            response.raise_for_status()
            self._stats["successful_requests"] += 1
            return response.json()
        except (httpx.HTTPStatusError, httpx.RequestError, httpx.TimeoutException) as exc:
            self._stats["failed_requests"] += 1
            self.logger.warning(
                "Request failed (attempt %d/%d): %s - %s",
                retry_count + 1,
                self.config.max_retries,
                self.config.endpoint,
                exc,
            )

            if retry_count < self.config.max_retries - 1:
                backoff = 2 ** retry_count + (retry_count * 0.5)
                self.logger.info("Retrying in %.2f seconds...", backoff)
                await asyncio.sleep(backoff)
                return await self._fetch_page(params, retry_count + 1)

            self.logger.error(
                "Max retries reached for %s: %s",
                self.config.endpoint,
                exc,
            )
            return None

    async def collect_all(self) -> list[dict[str, Any]]:
        """Collect all data from the paginated API endpoint.

        Returns:
            Complete list of collected items.
        """
        self._stats["start_time"] = datetime.now(timezone.utc)
        self.logger.info(
            "Starting collection from %s%s (page_size=%d, max_concurrent=%d)",
            self.config.base_url,
            self.config.endpoint,
            self.config.page_size,
            self.config.max_concurrent,
        )

        semaphore = asyncio.Semaphore(self.config.max_concurrent)
        all_items: list[dict[str, Any]] = []
        seen_page_ids: set[str] = set()
        pending_pages: asyncio.Queue = asyncio.Queue()
        initial_params = self._build_initial_params()
        await pending_pages.put(initial_params)

        workers = [
            asyncio.create_task(
                self._page_worker(semaphore, pending_pages, all_items, seen_page_ids)
            )
            for _ in range(self.config.max_concurrent)
        ]

        # Wait for all workers to finish
        await pending_pages.join()

        # Cancel workers
        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

        self._stats["end_time"] = datetime.now(timezone.utc)
        self._stats["total_items"] = len(all_items)
        self.logger.info(
            "Collection completed: %d items in %.2f seconds (%d requests, %d failed)",
            len(all_items),
            (self._stats["end_time"] - self._stats["start_time"]).total_seconds(),
            self._stats["total_requests"],
            self._stats["failed_requests"],
        )

        return all_items

    async def collect_batch(self, batch_size: int) -> list[dict[str, Any]]:
        """Collect a specific number of items.

        Args:
            batch_size: Maximum number of items to return.

        Returns:
            List of collected items up to batch_size.
        """
        self._stats["start_time"] = datetime.now(timezone.utc)

        params = self._build_initial_params()
        items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        while len(items) < batch_size:
            response = await self._fetch_page(params)
            if response is None:
                break

            page_items = self._extract_items(response)
            if not page_items:
                break

            for item in page_items:
                item_id = str(item.get("id", item.get("order_id", "")))
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)
                    items.append(item)
                    if len(items) >= batch_size:
                        break

            next_token = self._extract_next_token(response)
            if not next_token:
                break

            params = self._build_next_params(next_token)

        self._stats["end_time"] = datetime.now(timezone.utc)
        self._stats["total_items"] = len(items)
        self.logger.info(
            "Batch collection completed: %d items (batch_size=%d)",
            len(items),
            batch_size,
        )

        return items

    def _build_initial_params(self) -> dict[str, Any]:
        """Build the initial query parameters."""
        if self.config.pagination_type == "cursor":
            return {
                "limit": self.config.page_size,
            }
        return {
            "page": 1,
            "per_page": self.config.page_size,
        }

    def _build_next_params(self, next_token: str) -> dict[str, Any]:
        """Build query parameters for the next page."""
        if self.config.pagination_type == "cursor":
            return {
                "limit": self.config.page_size,
                "cursor": next_token,
            }
        return {
            "page": next_token,
            "per_page": self.config.page_size,
        }

    async def _page_worker(
        self,
        semaphore: asyncio.Semaphore,
        queue: asyncio.Queue,
        all_items: list[dict[str, Any]],
        seen_ids: set[str],
    ) -> None:
        """Worker coroutine that fetches pages from the queue.

        Args:
            semaphore: Semaphore to limit concurrent requests.
            queue: Queue of page parameters to fetch.
            all_items: Shared list to append collected items.
            seen_ids: Shared set of seen item IDs for dedup.
        """
        while True:
            params = await queue.get()
            async with semaphore:
                response = await self._fetch_page(params)

            if response is None:
                queue.task_done()
                continue

            items = self._extract_items(response)
            for item in items:
                item_id = str(item.get("id", item.get("order_id", "")))
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)
                    all_items.append(item)

            next_token = self._extract_next_token(response)
            if next_token:
                next_params = self._build_next_params(next_token)
                await queue.put(next_params)

            queue.task_done()

    @property
    def stats(self) -> dict[str, Any]:
        """Get collection statistics."""
        return dict(self._stats)

    async def collect_stream(self) -> AsyncGenerator[dict[str, Any], None]:
        """Stream items as they arrive (generator-based).

        Yields:
            Individual items as they are collected.
        """
        params = self._build_initial_params()
        seen_ids: set[str] = set()

        while True:
            response = await self._fetch_page(params)
            if response is None:
                break

            items = self._extract_items(response)
            if not items:
                break

            for item in items:
                item_id = str(item.get("id", item.get("order_id", "")))
                if item_id and item_id not in seen_ids:
                    seen_ids.add(item_id)
                    yield item

            next_token = self._extract_next_token(response)
            if not next_token:
                break

            params = self._build_next_params(next_token)

"""Scrapy middleware with retry logic, user-agent rotation, and error handling."""

import logging
import random
from typing import Optional

from scrapy import signals
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# User-Agent rotation
# ---------------------------------------------------------------------------

USER_AGENTS = [
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) "
    "Gecko/20100101 Firefox/121.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.2 Safari/605.1.15",
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
]


class UserAgentRotationMiddleware:
    """Rotate User-Agent header on every request."""

    def __init__(self, ua_list: list[str]):
        self.ua_list = ua_list

    @classmethod
    def from_crawler(cls, crawler):
        ua_list = crawler.settings.getlist(
            "USER_AGENT_LIST", USER_AGENTS
        )
        middleware = cls(ua_list)
        crawler.signals.connect(
            middleware.spider_opened, signal=signals.spider_opened
        )
        return middleware

    def spider_opened(self, spider):
        spider.logger.info(
            "UserAgentRotationMiddleware loaded with %d agents",
            len(self.ua_list),
        )

    def process_request(self, request, spider):
        request.headers["User-Agent"] = random.choice(self.ua_list)


# ---------------------------------------------------------------------------
# Custom retry middleware with exponential backoff and jitter
# ---------------------------------------------------------------------------

class CustomRetryMiddleware(RetryMiddleware):
    """Enhanced retry with exponential backoff + jitter and per-domain tracking."""

    def __init__(self, settings):
        super().__init__(settings)
        self.max_retry_delay = settings.getint("RETRY_MAX_DELAY", 60)
        self.retry_counts: dict[str, int] = {}

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def _retry(self, request, reason, spider):
        retries = request.meta.get("retry_times", 0) + 1
        request.meta["retry_times"] = retries

        # Exponential backoff with full jitter
        base = self.priority_adjust
        cap = min(base * (2 ** retries), self.max_retry_delay)
        delay = random.uniform(0, cap)
        request.meta["retry_delay"] = delay

        # Track retries per domain
        domain = request.url.split("/")[2] if "//" in request.url else "unknown"
        self.retry_counts[domain] = self.retry_counts.get(domain, 0) + 1

        spider.crawler.stats.inc_value("retry/count")
        spider.crawler.stats.inc_value(f"retry/domain/{domain}")

        logger.warning(
            "Retry %d/%d for %s (delay=%.2fs, reason=%s)",
            retries,
            self.max_retry_times,
            request.url,
            delay,
            reason,
        )

        return super()._retry(request, reason, spider)


# ---------------------------------------------------------------------------
# Error handling / logging middleware
# ---------------------------------------------------------------------------

class ErrorLoggingMiddleware:
    """Log and classify errors for monitoring."""

    @staticmethod
    def process_exception(request, exception, spider):
        error_type = type(exception).__name__
        spider.crawler.stats.inc_value(f"error/{error_type}")

        logger.error(
            "Exception on %s: [%s] %s",
            request.url,
            error_type,
            str(exception),
        )
        return None  # let other middlewares / retry handle it

"""Scrapy Spider that crawls a mock internal wiki site."""

import logging
from typing import Optional
from urllib.parse import urljoin, urlparse

import scrapy
from scrapy import Request
from scrapy.linkextractors import LinkExtractor

from crawler.scrapy_spider.items import WikiPageItem


logger = logging.getLogger(__name__)


class WikiSpider(scrapy.Spider):
    """Crawl an internal wiki, extract pages, and follow links up to a depth limit.

    Configuration
    -------------
    *allowed_domains* – restrict crawling to these domains (set via settings or CLI).
    *start_urls* – entry-point page(s) for the crawl.
    *depth_limit* – maximum link depth to follow (default: 3).
    *max_pages* – hard cap on the number of pages to crawl (default: 1000).
    """

    name = "wiki_spider"

    allowed_domains: list[str] = []
    start_urls: list[str] = []

    # Limits
    depth_limit: int = 3
    max_pages: int = 1000
    _pages_crawled: int = 0

    # Custom settings
    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0.5,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "COOKIES_ENABLED": False,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "DEPTH_LIMIT": 3,
        "LOG_LEVEL": "INFO",
    }

    def __init__(
        self,
        *args,
        domain: Optional[str] = None,
        start_url: Optional[str] = None,
        depth_limit: Optional[int] = None,
        max_pages: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if domain:
            self.allowed_domains = [domain]
        if start_url:
            self.start_urls = [start_url]
        if depth_limit is not None:
            self.depth_limit = depth_limit
        if max_pages is not None:
            self.max_pages = max_pages

        self._link_extractor = LinkExtractor(
            allow=(),  # accept all links on allowed domains
            deny=(r"\.(pdf|zip|gz|png|jpg|jpeg|gif|ico|svg|css|js)$",),
            unique=True,
        )

        if not self.start_urls:
            self.start_urls = ["http://wiki.example.com/"]

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, meta={"depth": 0, "source": "wiki"})

    def parse(self, response, **cb_kwargs):
        # Enforce hard page cap
        if self._pages_crawled >= self.max_pages:
            return
        self._pages_crawled += 1

        current_depth = response.meta.get("depth", 0)

        # --- Extract page content ---
        item = WikiPageItem()
        item["url"] = response.url
        item["title"] = self._extract_title(response)
        item["content"] = self._extract_body(response)
        item["source"] = response.meta.get("source", "wiki")
        item["content_type"] = "text/html"
        item["depth"] = current_depth

        # Wiki-specific metadata
        item["page_id"] = self._extract_page_id(response)
        item["revision"] = self._extract_revision(response)
        item["author"] = self._extract_author(response)
        item["category"] = self._extract_category(response)
        item["tags"] = self._extract_tags(response)

        # Last-Modified header or meta
        last_mod = response.headers.get("Last-Modified")
        if last_mod:
            item["last_modified"] = last_mod.decode("utf-8", errors="replace")
        else:
            item["last_modified"] = None

        # Count outgoing links (for stats)
        links = self._link_extractor.extract_links(response)
        item["links_out"] = len(links)

        yield item

        # --- Follow links if within depth limit ---
        if current_depth < self.depth_limit:
            for link in links:
                # Ensure we don't exceed max pages
                if self._pages_crawled >= self.max_pages:
                    break
                yield Request(
                    url=urljoin(response.url, link.url),
                    meta={
                        "depth": current_depth + 1,
                        "source": "wiki",
                    },
                )

    # ------------------------------------------------------------------
    #  Extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_title(response) -> Optional[str]:
        title = response.css("title::text").get()
        if title:
            return title.strip()
        h1 = response.css("h1::text").get()
        return h1.strip() if h1 else None

    @staticmethod
    def _extract_body(response) -> Optional[str]:
        """Return the HTML of the main content area if identifiable, else full body."""
        main = response.css(
            "main, article, #content, .wiki-content, .page-content"
        ).get()
        if main:
            return main
        return response.css("body").get()

    @staticmethod
    def _extract_page_id(response) -> Optional[str]:
        return response.css(
            "meta[name='page-id']::attr(content), "
            "#page-id::text"
        ).get(default=None)

    @staticmethod
    def _extract_revision(response) -> Optional[str]:
        return response.css(
            "meta[name='revision']::attr(content), "
            ".revision::text"
        ).get(default=None)

    @staticmethod
    def _extract_author(response) -> Optional[str]:
        return response.css(
            "meta[name='author']::attr(content), "
            ".author::text"
        ).get(default=None)

    @staticmethod
    def _extract_category(response) -> Optional[str]:
        return response.css(
            "meta[name='category']::attr(content), "
            ".category::text"
        ).get(default=None)

    @staticmethod
    def _extract_tags(response) -> list[str]:
        raw = response.css(
            "meta[name='keywords']::attr(content), "
            ".tags a::text"
        ).getall()
        tags: list[str] = []
        for r in raw:
            tags.extend(t.strip() for t in r.split(",") if t.strip())
        return tags

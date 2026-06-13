"""Scrapy Item definitions for crawled documents."""

import scrapy


class DocumentItem(scrapy.Item):
    """A crawled document with metadata for downstream processing."""

    title = scrapy.Field()
    content = scrapy.Field()
    url = scrapy.Field()
    last_modified = scrapy.Field()

    # Optional metadata fields
    source = scrapy.Field()         # e.g. "internal-wiki", "manual"
    content_type = scrapy.Field()   # e.g. "text/html", "text/plain"
    crawl_timestamp = scrapy.Field()
    depth = scrapy.Field()
    links_out = scrapy.Field()      # count of outgoing links


class WikiPageItem(DocumentItem):
    """Item specifically for wiki pages, with wiki-specific metadata."""

    page_id = scrapy.Field()
    revision = scrapy.Field()
    author = scrapy.Field()
    category = scrapy.Field()
    tags = scrapy.Field()           # list of tags

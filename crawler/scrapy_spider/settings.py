"""Scrapy settings for hermatic_data_pipeline project."""

import os

# ---------------------------------------------------------------------------
# Spider modules
# ---------------------------------------------------------------------------
SPIDER_MODULES = ["crawler.scrapy_spider.spiders"]
NEWSPIDER_MODULE = "crawler.scrapy_spider.spiders"

# ---------------------------------------------------------------------------
# Crawl responsibly
# ---------------------------------------------------------------------------
ROBOTSTXT_OBEY = False
USER_AGENT = "HermeticDataPipeline/1.0 (+https://github.com/bella/hermetic-data-pipeline)"

# ---------------------------------------------------------------------------
# Concurrency & delay
# ---------------------------------------------------------------------------
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 0.5
RANDOMIZE_DOWNLOAD_DELAY = True

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
DOWNLOADER_MIDDLEWARES = {
    "crawler.scrapy_spider.middlewares.UserAgentRotationMiddleware": 350,
    "crawler.scrapy_spider.middlewares.CustomRetryMiddleware": 500,
    "crawler.scrapy_spider.middlewares.ErrorLoggingMiddleware": 550,
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
}

# ---------------------------------------------------------------------------
# Item pipelines
# ---------------------------------------------------------------------------
ITEM_PIPELINES = {
    "crawler.scrapy_spider.pipelines.CleanAndSavePipeline": 300,
}

# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [429, 500, 502, 503, 504]
RETRY_MAX_DELAY = 60

# ---------------------------------------------------------------------------
# Depth & limits
# ---------------------------------------------------------------------------
DEPTH_LIMIT = 3
DEPTH_PRIORITY = 1

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
OUTPUT_DIR = os.environ.get("CRAWLER_OUTPUT_DIR", "data/crawled")
SAVE_PARQUET = os.environ.get("CRAWLER_SAVE_PARQUET", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------
HTTPCACHE_ENABLED = False

# ---------------------------------------------------------------------------
# Extensions
# ---------------------------------------------------------------------------
EXTENSIONS = {
    "scrapy.extensions.telnet.TelnetConsole": None,
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.environ.get("CRAWLER_LOG_LEVEL", "INFO")

"""Pipeline that cleans HTML, extracts text, and saves to JSON / Parquet."""

import json
import logging
import os
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Optional

from itemadapter import ItemAdapter


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTML stripping helper
# ---------------------------------------------------------------------------

class _HTMLStripper(HTMLParser):
    """Minimal HTML parser that collects visible text."""

    def __init__(self):
        super().__init__()
        self._text_parts: list[str] = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            stripped = data.strip()
            if stripped:
                self._text_parts.append(stripped)

    def get_text(self) -> str:
        return " ".join(self._text_parts)


def strip_html(html: str) -> str:
    """Return plain text from HTML, removing script/style content."""
    stripper = _HTMLStripper()
    try:
        stripper.feed(html)
    except Exception:
        return html  # fallback
    return stripper.get_text()


# ---------------------------------------------------------------------------
# Text cleaning
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    """Normalise whitespace and strip control characters."""
    import re
    # Replace any whitespace runs (including newlines, tabs) with a single space
    text = re.sub(r"\s+", " ", text)
    # Remove non-printable characters except common whitespace
    text = "".join(ch for ch in text if ch.isprintable() or ch in ("\n", "\t", "\r"))
    return text.strip()


# ---------------------------------------------------------------------------
# Output directory helpers
# ---------------------------------------------------------------------------

def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class CleanAndSavePipeline:
    """Process items: strip HTML, clean text, and persist to JSON (and optionally Parquet)."""

    def __init__(
        self,
        output_dir: str = "data/crawled",
        save_parquet: bool = False,
    ):
        self.output_dir = _ensure_dir(output_dir)
        self.save_parquet = save_parquet
        self.items: list[dict] = []

    @classmethod
    def from_crawler(cls, crawler):
        output_dir = crawler.settings.get("OUTPUT_DIR", "data/crawled")
        save_parquet = crawler.settings.getbool("SAVE_PARQUET", False)
        return cls(output_dir=output_dir, save_parquet=save_parquet)

    def open_spider(self, spider):
        logger.info(
            "CleanAndSavePipeline initialised – output: %s (parquet=%s)",
            self.output_dir,
            self.save_parquet,
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # --- Clean HTML content ---
        content = adapter.get("content")
        if content and isinstance(content, str):
            # Detect HTML by looking for typical tags
            is_html = any(tag in content[:500].lower() for tag in ("<html", "<!doctype", "<div", "<p>", "<span"))
            if is_html:
                content = strip_html(content)
            content = clean_text(content)
            adapter["content"] = content

        # --- Normalise title ---
        title = adapter.get("title")
        if title and isinstance(title, str):
            title = clean_text(strip_html(title))
            adapter["title"] = title

        # --- Add crawl timestamp if missing ---
        if not adapter.get("crawl_timestamp"):
            adapter["crawl_timestamp"] = datetime.now(timezone.utc).isoformat()

        # -- Keep a copy for batch persistence --
        self.items.append(dict(adapter))

        return item

    def close_spider(self, spider):
        """Write all collected items to a JSON lines file (and optionally Parquet)."""

        if not self.items:
            logger.warning("No items collected – nothing to save.")
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        json_path = os.path.join(self.output_dir, f"crawl_{timestamp}.jsonl")

        with open(json_path, "w", encoding="utf-8") as fh:
            for item in self.items:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")

        logger.info("Saved %d items to %s", len(self.items), json_path)

        if self.save_parquet:
            try:
                self._save_parquet(timestamp)
            except ImportError:
                logger.warning(
                    "Parquet export requires pyarrow – install with: pip install pyarrow"
                )
            except Exception as exc:
                logger.error("Parquet export failed: %s", exc)

    def _save_parquet(self, timestamp: str):
        """Convert items to Parquet via pyarrow."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pylist(self.items)
        pq_path = os.path.join(
            self.output_dir, f"crawl_{timestamp}.parquet"
        )
        pq.write_table(table, pq_path)
        logger.info("Saved %d items to %s", len(self.items), pq_path)

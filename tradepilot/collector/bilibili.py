"""Bilibili video content collector skeleton.

Phase-one stub: defines the collector interface and data flow.
Actual subtitle download logic will be integrated later.
"""

from loguru import logger

from tradepilot.db import get_conn
from tradepilot.ingestion.models import VideoContentRecord


class BilibiliCollector:
    """Collect Bilibili video metadata and subtitles, persist to DuckDB."""

    def collect(self, video_urls: list[str] | None = None) -> list[VideoContentRecord]:
        """Fetch and store video metadata.

        Args:
            video_urls: Bilibili video URLs to process.

        Returns:
            List of persisted video records.
        """
        logger.info("BilibiliCollector.collect called (stub), urls={}", video_urls)
        # TODO: download subtitles, extract metadata, persist
        return []

    def _persist(self, items: list[VideoContentRecord]) -> int:
        """Write video records to DuckDB, skipping duplicates.

        Args:
            items: Video content records to persist.

        Returns:
            Number of newly inserted rows.
        """
        if not items:
            return 0
        conn = get_conn()
        inserted = 0
        for item in items:
            try:
                conn.execute(
                    """
                    INSERT INTO video_content (source, source_item_id, title, video_url, file_path, published_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [
                        item.source,
                        item.source_item_id,
                        item.title,
                        item.video_url,
                        item.file_path,
                        item.published_at,
                    ],
                )
                inserted += 1
            except Exception:
                logger.exception(
                    "Failed to persist video content {}", item.source_item_id
                )
        return inserted

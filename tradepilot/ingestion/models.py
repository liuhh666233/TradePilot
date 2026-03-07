"""Pydantic models for phase-one ingestion workflows."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class SourceType(StrEnum):
    """Supported ingestion source types."""

    MARKET = "market"
    NEWS = "news"
    BILIBILI = "bilibili"


class RunStatus(StrEnum):
    """Execution status for an ingestion run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class TriggerMode(StrEnum):
    """How an ingestion run was triggered."""

    MANUAL = "manual"
    SCHEDULED = "scheduled"


class IngestionRun(BaseModel):
    """Execution summary for one ingestion job."""

    id: int
    job_name: str
    source_type: SourceType
    trigger_mode: TriggerMode = TriggerMode.MANUAL
    status: RunStatus = RunStatus.PENDING
    started_at: datetime
    finished_at: datetime | None = None
    records_discovered: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_failed: int = 0
    error_message: str | None = None


class SyncRequest(BaseModel):
    """Request payload for manual ingestion sync endpoints."""

    start_date: str = "2024-01-01"
    end_date: str = "2025-12-31"
    stock_codes: list[str] = Field(default_factory=list)
    index_codes: list[str] = Field(default_factory=list)
    full_refresh: bool = False


class SyncResult(BaseModel):
    """Response payload for ingestion sync endpoints."""

    run: IngestionRun
    message: str


class NewsSyncRequest(BaseModel):
    """Request payload for a news sync."""

    stock_codes: list[str] = Field(default_factory=list)


class BilibiliSyncRequest(BaseModel):
    """Request payload for a Bilibili video sync."""

    video_urls: list[str] = Field(default_factory=list)


class NewsItemRecord(BaseModel):
    """Raw news item persisted for later processing."""

    source: str
    source_item_id: str
    title: str
    content: str
    category: str | None = None
    published_at: datetime | None = None


class VideoContentRecord(BaseModel):
    """Video metadata persisted for later processing."""

    source: str
    source_item_id: str
    title: str
    video_url: str
    file_path: str | None = None
    published_at: datetime | None = None

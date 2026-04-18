"""Pydantic models for ETF all-weather stage-one operations."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class EtfAllWeatherRunStatus(StrEnum):
    """Execution status for ETF all-weather stage-one jobs."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class EtfAllWeatherJobRun(BaseModel):
    """Execution summary for one ETF all-weather stage-one job."""

    run_id: int
    job_name: str
    dataset_name: str
    source_name: str
    status: EtfAllWeatherRunStatus
    started_at: datetime
    finished_at: datetime | None = None
    records_discovered: int = 0
    records_inserted: int = 0
    records_failed: int = 0
    error_message: str | None = None


class EtfAllWeatherJobResult(BaseModel):
    """Response payload for ETF all-weather stage-one jobs."""

    run: EtfAllWeatherJobRun
    message: str
    details: dict[str, str | int | None] | None = None


class EtfAllWeatherCalendarSyncRequest(BaseModel):
    """Request payload for canonical trading calendar sync."""

    start_date: str
    end_date: str
    exchange: str = "SSE"


class EtfAllWeatherInitResult(BaseModel):
    """Result payload for schema and storage bootstrap."""

    schema_applied: bool
    storage_root: str
    raw_root: str
    normalized_root: str
    derived_root: str


class EtfAllWeatherMarketSyncRequest(BaseModel):
    """Request payload for canonical sleeve daily market sync."""

    start_date: str
    end_date: str


class EtfAllWeatherSlowMacroSyncRequest(BaseModel):
    """Request payload for canonical slow macro sync."""

    start_month: str
    end_month: str


class EtfAllWeatherCurveSyncRequest(BaseModel):
    """Request payload for curve extraction sync."""

    start_date: str
    end_date: str
    window_days: int = 7


class EtfAllWeatherFeatureSnapshotRequest(BaseModel):
    """Request payload for derived monthly feature snapshot generation."""

    start_date: str | None = None
    end_date: str | None = None


class EtfAllWeatherRegimeSnapshotRequest(BaseModel):
    """Request payload for rule-based regime and confidence generation."""

    start_date: str | None = None
    end_date: str | None = None

"""Core models for the generic ETL foundation."""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class DatasetCategory(StrEnum):
    """Supported dataset families."""

    REFERENCE = "reference"
    MARKET = "market"
    RATES = "rates"
    MACRO = "macro"
    ALT = "alt"
    DERIVED = "derived"


class StorageZone(StrEnum):
    """Top-level lakehouse storage zones."""

    RAW = "raw"
    NORMALIZED = "normalized"
    DERIVED = "derived"


class TriggerMode(StrEnum):
    """How an ETL run is triggered."""

    MANUAL = "manual"
    SCHEDULED = "scheduled"
    BACKFILL = "backfill"
    RETRY_FAILED = "retry_failed"
    VALIDATION_ONLY = "validation_only"


class RunStatus(StrEnum):
    """Framework-level status for one ETL run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"


class ValidationStatus(StrEnum):
    """Structured validation result status."""

    PASS = "pass"
    PASS_WITH_CAVEAT = "pass_with_caveat"
    WARNING = "warning"
    VALIDATION_ONLY = "validation_only"
    DEFER = "defer"
    FAIL = "fail"


class IngestionRequest(BaseModel):
    """Common request envelope for future dataset sync entrypoints."""

    request_start: date | None = None
    request_end: date | None = None
    full_refresh: bool = False
    trigger_mode: TriggerMode = TriggerMode.MANUAL
    context: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class IngestionRunRecord(BaseModel):
    """Execution record for one ETL run."""

    run_id: int
    job_name: str
    dataset_name: str
    source_name: str
    trigger_mode: TriggerMode
    status: RunStatus = RunStatus.PENDING
    started_at: datetime
    finished_at: datetime | None = None
    request_start: date | None = None
    request_end: date | None = None
    records_discovered: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_failed: int = 0
    partitions_written: int = 0
    error_message: str | None = None
    code_version: str | None = None


class RawBatchRecord(BaseModel):
    """Manifest row for one immutable raw batch."""

    raw_batch_id: int
    run_id: int
    dataset_name: str
    source_name: str
    source_endpoint: str | None = None
    storage_path: str
    file_format: str
    compression: str | None = None
    partition_year: int | None = None
    partition_month: int | None = None
    window_start: date | None = None
    window_end: date | None = None
    row_count: int = 0
    content_hash: str | None = None
    fetched_at: datetime
    schema_version: str | None = None
    is_fallback_source: bool = False


class ValidationResultRecord(BaseModel):
    """Structured output for one validation check."""

    validation_id: int
    run_id: int
    raw_batch_id: int | None = None
    dataset_name: str
    check_name: str
    check_level: str
    status: ValidationStatus
    subject_key: str | None = None
    metric_value: float | None = None
    threshold_value: float | None = None
    details_json: str | None = None
    created_at: datetime


class SourceWatermarkRecord(BaseModel):
    """Latest successful incremental boundary for one dataset-source pair."""

    dataset_name: str
    source_name: str
    latest_available_date: date | None = None
    latest_fetched_date: date | None = None
    latest_successful_run_id: int | None = None
    updated_at: datetime

"""Pydantic models for daily workflow snapshots."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class WorkflowPhase(StrEnum):
    """Supported workflow phases for the daily operating loop."""

    PRE_MARKET = "pre_market"
    POST_MARKET = "post_market"


class WorkflowStatus(StrEnum):
    """Execution status for one workflow run."""

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"


class WorkflowTrigger(StrEnum):
    """How a workflow run was triggered."""

    MANUAL = "manual"
    SCHEDULER = "scheduler"


class WorkflowStepResult(BaseModel):
    """Status summary for one workflow step."""

    name: str
    status: str
    records_affected: int = 0
    error_message: str | None = None
    details: dict = Field(default_factory=dict)


class WorkflowSummary(BaseModel):
    """Top-level payload rendered by the workflow UI."""

    title: str
    overview: str
    requested_date: str | None = None
    resolved_date: str | None = None
    date_resolution: str = "exact"
    market_overview: dict = Field(default_factory=dict)
    sector_positioning: dict = Field(default_factory=dict)
    position_health: dict = Field(default_factory=dict)
    next_day_prep: dict = Field(default_factory=dict)
    yesterday_recap: dict = Field(default_factory=dict)
    overnight_news: dict = Field(default_factory=dict)
    today_watchlist: dict = Field(default_factory=dict)
    action_frame: dict = Field(default_factory=dict)
    watch_context: dict = Field(default_factory=dict)
    alerts: list[dict] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)
    steps: list[WorkflowStepResult] = Field(default_factory=list)
    watchlist: dict = Field(default_factory=dict)
    scan: dict = Field(default_factory=dict)
    news: dict = Field(default_factory=dict)
    scheduler: dict = Field(default_factory=dict)
    carry_over: dict = Field(default_factory=dict)


class WorkflowRunRecord(BaseModel):
    """Persisted workflow run record."""

    id: int
    workflow_date: str
    phase: WorkflowPhase
    triggered_by: WorkflowTrigger
    status: WorkflowStatus
    started_at: datetime
    finished_at: datetime | None = None
    summary: WorkflowSummary
    error_message: str | None = None


class WorkflowRunResponse(BaseModel):
    """API payload for one workflow run."""

    run: WorkflowRunRecord


class WorkflowHistoryItem(BaseModel):
    """Compact workflow history row for tables."""

    id: int
    workflow_date: str
    phase: str
    triggered_by: str
    status: str
    started_at: str
    finished_at: str | None = None
    error_message: str | None = None

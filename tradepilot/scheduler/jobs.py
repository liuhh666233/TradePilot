from __future__ import annotations

from datetime import datetime
from importlib import import_module
import time

from loguru import logger

from tradepilot.db import get_conn
from tradepilot.scanner.daily import DailyScanner
from tradepilot.workflow.models import WorkflowTrigger
from tradepilot.workflow.service import DailyWorkflowService


def _tushare_client():
    return import_module("tradepilot.data.tushare_client").TushareClient()


def _job_id() -> int:
    return time.time_ns()


def _record_history(
    job_name: str,
    started_at: datetime,
    status: str,
    records_affected: int = 0,
    error_message: str | None = None,
) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO scheduler_history (id, job_name, started_at, finished_at, status, records_affected, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            _job_id(),
            job_name,
            started_at,
            datetime.now(),
            status,
            records_affected,
            error_message,
        ],
    )


def _create_failure_alert(job_name: str, error_message: str) -> None:
    DailyScanner().create_system_alert(
        title=f"调度任务失败：{job_name}",
        message=error_message,
        urgency="high",
    )


def _should_run_for_trading_day(target_date: str) -> bool:
    return _tushare_client().is_trading_day(target_date)


def _run_workflow_job(job_name: str, runner) -> dict:
    started_at = datetime.now()
    try:
        run = runner()
        affected = sum(step.records_affected for step in run.summary.steps)
        _record_history(
            job_name, started_at, run.status.value, affected, run.error_message
        )
        if run.status.value in {"failed", "partial"} and run.error_message:
            _create_failure_alert(job_name, run.error_message)
        return {
            "status": run.status.value,
            "records_affected": affected,
            "workflow_date": run.workflow_date,
        }
    except Exception as exc:
        logger.exception("scheduler {} failed", job_name)
        _record_history(job_name, started_at, "failed", 0, str(exc))
        _create_failure_alert(job_name, str(exc))
        return {"status": "failed", "error": str(exc)}


def pre_market_workflow_job() -> dict:
    service = DailyWorkflowService()
    return _run_workflow_job(
        "pre_market_workflow",
        lambda: service.run_pre_market_workflow(triggered_by=WorkflowTrigger.SCHEDULER),
    )


def post_market_workflow_job() -> dict:
    service = DailyWorkflowService()
    return _run_workflow_job(
        "post_market_workflow",
        lambda: service.run_post_market_workflow(
            triggered_by=WorkflowTrigger.SCHEDULER
        ),
    )


def get_scheduler_history(limit: int = 20) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM scheduler_history ORDER BY started_at DESC, id DESC LIMIT ?",
        [limit],
    ).fetchdf()
    return rows.to_dict(orient="records")

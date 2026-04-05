from __future__ import annotations

from datetime import datetime
from importlib import import_module
import time

from loguru import logger

from tradepilot.db import get_conn
from tradepilot.ingestion.models import NewsSyncRequest, SyncRequest
from tradepilot.ingestion.service import IngestionService
from tradepilot.scanner.daily import DailyScanner


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


def market_sync_job() -> dict:
    started_at = datetime.now()
    today = started_at.strftime("%Y-%m-%d")
    if not _should_run_for_trading_day(today):
        _record_history("market_sync", started_at, "skipped", 0, "non-trading day")
        return {"status": "skipped", "reason": "non-trading day"}
    try:
        result = IngestionService().sync_market(SyncRequest(start_date=today, end_date=today, full_refresh=False))
        affected = result.run.records_inserted + result.run.records_updated
        _record_history("market_sync", started_at, "success", affected, None)
        return {"status": "success", "records_affected": affected}
    except Exception as exc:
        logger.exception("scheduler market_sync failed")
        _record_history("market_sync", started_at, "failed", 0, str(exc))
        _create_failure_alert("market_sync", str(exc))
        return {"status": "failed", "error": str(exc)}


def news_sync_job() -> dict:
    started_at = datetime.now()
    try:
        result = IngestionService().sync_news(NewsSyncRequest())
        affected = result.run.records_inserted + result.run.records_updated
        _record_history("news_sync", started_at, "success", affected, None)
        return {"status": "success", "records_affected": affected}
    except Exception as exc:
        logger.exception("scheduler news_sync failed")
        _record_history("news_sync", started_at, "failed", 0, str(exc))
        _create_failure_alert("news_sync", str(exc))
        return {"status": "failed", "error": str(exc)}


def daily_scan_job() -> dict:
    started_at = datetime.now()
    today = started_at.strftime("%Y-%m-%d")
    if not _should_run_for_trading_day(today):
        _record_history("daily_scan", started_at, "skipped", 0, "non-trading day")
        return {"status": "skipped", "reason": "non-trading day"}
    try:
        result = DailyScanner().run(scan_date=today)
        affected = len(result.watchlist_advice) + len(result.position_advice) + len(result.core_instrument_advice)
        _record_history("daily_scan", started_at, "success", affected, None)
        return {"status": "success", "records_affected": affected}
    except Exception as exc:
        logger.exception("scheduler daily_scan failed")
        _record_history("daily_scan", started_at, "failed", 0, str(exc))
        _create_failure_alert("daily_scan", str(exc))
        return {"status": "failed", "error": str(exc)}


def get_scheduler_history(limit: int = 20) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM scheduler_history ORDER BY started_at DESC, id DESC LIMIT ?",
        [limit],
    ).fetchdf()
    return rows.to_dict(orient="records")

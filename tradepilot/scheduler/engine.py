from __future__ import annotations

import os
from datetime import datetime
from importlib import import_module
from typing import Any

from loguru import logger

from tradepilot.scheduler.jobs import post_market_workflow_job, pre_market_workflow_job

_scheduler: Any | None = None


def _scheduler_enabled() -> bool:
    return (
        os.environ.get("RUN_MAIN") == "true"
        or os.environ.get("WERKZEUG_RUN_MAIN") == "true"
        or os.environ.get("UVICORN_RELOAD") != "true"
    )


def get_scheduler() -> Any:
    global _scheduler
    if _scheduler is None:
        background = import_module("apscheduler.schedulers.background")
        _scheduler = background.BackgroundScheduler(timezone="Asia/Shanghai")
    return _scheduler


def start_scheduler() -> None:
    scheduler = get_scheduler()
    cron = import_module("apscheduler.triggers.cron")
    if scheduler.running:
        return
    if not _scheduler_enabled():
        logger.info("scheduler startup skipped in reload parent process")
        return
    scheduler.add_job(
        pre_market_workflow_job,
        cron.CronTrigger(day_of_week="mon-fri", hour=8, minute=30),
        id="pre_market_workflow",
        replace_existing=True,
    )
    scheduler.add_job(
        post_market_workflow_job,
        cron.CronTrigger(day_of_week="mon-fri", hour=16, minute=30),
        id="post_market_workflow",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("scheduler started with {} jobs", len(scheduler.get_jobs()))


def stop_scheduler() -> None:
    scheduler = get_scheduler()
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("scheduler stopped")


def scheduler_status() -> dict:
    scheduler = get_scheduler()
    jobs = []
    for job in scheduler.get_jobs():
        next_run_time = (
            job.next_run_time.astimezone().isoformat() if job.next_run_time else None
        )
        jobs.append({"id": job.id, "name": job.name, "next_run_time": next_run_time})
    return {
        "running": scheduler.running,
        "generated_at": datetime.now().isoformat(),
        "jobs": jobs,
    }

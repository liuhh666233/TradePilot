"""Workflow service for pre-market and post-market daily operations."""

from __future__ import annotations

import json
import time
from datetime import date, datetime
from importlib import import_module

from loguru import logger

from tradepilot.data import get_provider
from tradepilot.db import get_conn
from tradepilot.ingestion.models import NewsSyncRequest, SyncRequest
from tradepilot.ingestion.service import IngestionService
from tradepilot.scanner.daily import DailyScanner, normalize_scan_date
from tradepilot.summary.models import WatchlistConfig
from tradepilot.workflow.models import (
    WorkflowHistoryItem,
    WorkflowPhase,
    WorkflowRunRecord,
    WorkflowStatus,
    WorkflowStepResult,
    WorkflowSummary,
    WorkflowTrigger,
)

_DEFAULT_INDEX_CODES = ["000001", "399001", "399006", "000688"]


class DailyWorkflowService:
    """Coordinate simplified pre-market and post-market workflows."""

    def __init__(self) -> None:
        self._scanner = DailyScanner()
        self._ingestion = IngestionService()
        self._tushare = import_module("tradepilot.data.tushare_client").TushareClient()
        self._summary_api = import_module("tradepilot.api.summary")

    def run_pre_market_workflow(
        self,
        workflow_date: str | None = None,
        triggered_by: WorkflowTrigger = WorkflowTrigger.MANUAL,
    ) -> WorkflowRunRecord:
        """Run the pre-market workflow and persist the resulting snapshot."""
        started_at = datetime.now()
        requested_date, resolved_date, date_resolution = self._resolve_pre_market_date(workflow_date)
        steps: list[WorkflowStepResult] = []
        error_messages: list[str] = []
        previous_post_market = self.get_latest_run(WorkflowPhase.POST_MARKET)
        alerts = self._scanner.list_alerts(unread_only=False)[:10]
        watchlist = self._load_watchlist().model_dump()
        news_items: list[dict] = []

        watch_context = self._build_watch_context(watchlist)

        if not self._should_run_for_trading_day(resolved_date):
            carry_over = self._build_carry_over(previous_post_market)
            summary = WorkflowSummary(
                title="盘前准备已跳过",
                overview="非交易日，未执行盘前工作流。",
                requested_date=requested_date,
                resolved_date=resolved_date,
                date_resolution=date_resolution,
                yesterday_recap=self._build_yesterday_recap(carry_over),
                overnight_news={"summary": "非交易日未执行夜间信息同步", "highlights": []},
                today_watchlist=self._build_today_watchlist(watch_context, alerts),
                action_frame=self._build_action_frame(carry_over, alerts),
                watch_context=watch_context,
                alerts=alerts[:5],
                metadata={"data_sources": ["workflow_runs", "alerts", "watchlist"]},
                watchlist=watchlist,
                carry_over=carry_over,
                steps=[
                    WorkflowStepResult(
                        name="trading_day_check",
                        status=WorkflowStatus.SKIPPED.value,
                        details={"reason": "non-trading day"},
                    )
                ],
            )
            return self._persist_run(
                workflow_date=resolved_date,
                phase=WorkflowPhase.PRE_MARKET,
                triggered_by=triggered_by,
                status=WorkflowStatus.SKIPPED,
                started_at=started_at,
                finished_at=datetime.now(),
                summary=summary,
                error_message="non-trading day",
            )

        news_result = self._ingestion.sync_news(NewsSyncRequest())
        news_status = news_result.run.status.value
        news_step_status = WorkflowStatus.SUCCESS.value if news_status == "success" else WorkflowStatus.FAILED.value
        if news_step_status == WorkflowStatus.FAILED.value:
            error_messages.append(news_result.run.error_message or "news sync failed")
        steps.append(
            WorkflowStepResult(
                name="news_sync",
                status=news_step_status,
                records_affected=news_result.run.records_inserted + news_result.run.records_updated,
                error_message=news_result.run.error_message,
            )
        )
        news_items = self._get_latest_news(limit=8)

        status = self._resolve_status(steps)
        carry_over = self._build_carry_over(previous_post_market)
        overview = self._build_pre_market_overview(
            previous_post_market,
            news_items,
            watchlist,
            alerts,
            requested_date,
            resolved_date,
            date_resolution,
        )
        summary = WorkflowSummary(
            title="盘前准备",
            overview=overview,
            requested_date=requested_date,
            resolved_date=resolved_date,
            date_resolution=date_resolution,
            yesterday_recap=self._build_yesterday_recap(carry_over),
            overnight_news=self._build_overnight_news(news_step_status, news_items),
            today_watchlist=self._build_today_watchlist(watch_context, alerts),
            action_frame=self._build_action_frame(carry_over, alerts),
            watch_context=watch_context,
            alerts=alerts[:8],
            metadata={
                "data_sources": ["workflow_runs", "news_items", "alerts", "watchlist"],
                "has_previous_post_market": previous_post_market is not None,
            },
            news={
                "status": news_step_status,
                "items": news_items,
            },
            watchlist=watchlist,
            carry_over=carry_over,
            scheduler={
                "has_previous_post_market": previous_post_market is not None,
            },
            steps=steps,
        )
        return self._persist_run(
            workflow_date=resolved_date,
            phase=WorkflowPhase.PRE_MARKET,
            triggered_by=triggered_by,
            status=status,
            started_at=started_at,
            finished_at=datetime.now(),
            summary=summary,
            error_message="；".join(error_messages) or None,
        )

    def run_post_market_workflow(
        self,
        workflow_date: str | None = None,
        triggered_by: WorkflowTrigger = WorkflowTrigger.MANUAL,
    ) -> WorkflowRunRecord:
        """Run the post-market workflow and persist the resulting snapshot."""
        started_at = datetime.now()
        requested_date, resolved_date, date_resolution = self._resolve_post_market_date(workflow_date)
        steps: list[WorkflowStepResult] = []
        error_messages: list[str] = []
        watchlist = self._load_watchlist().model_dump()

        watch_context = self._build_watch_context(watchlist)

        if not self._should_run_for_trading_day(resolved_date):
            summary = WorkflowSummary(
                title="盘后复盘已跳过",
                overview="非交易日，未执行盘后工作流。",
                requested_date=requested_date,
                resolved_date=resolved_date,
                date_resolution=date_resolution,
                market_overview={"summary": "非交易日，未生成市场大势。", "indices": []},
                sector_positioning={"market_leaders": [], "market_laggards": [], "watch_sectors": []},
                position_health={"portfolio_health_summary": "非交易日，未生成持仓健康度。", "tracked_items": []},
                next_day_prep={"market_bias": "observe", "focus_sectors": [], "focus_items": [], "risk_notes": []},
                watch_context=watch_context,
                metadata={"data_sources": ["watchlist"]},
                watchlist=watchlist,
                steps=[
                    WorkflowStepResult(
                        name="trading_day_check",
                        status=WorkflowStatus.SKIPPED.value,
                        details={"reason": "non-trading day"},
                    )
                ],
            )
            return self._persist_run(
                workflow_date=resolved_date,
                phase=WorkflowPhase.POST_MARKET,
                triggered_by=triggered_by,
                status=WorkflowStatus.SKIPPED,
                started_at=started_at,
                finished_at=datetime.now(),
                summary=summary,
                error_message="non-trading day",
            )

        stock_codes, index_codes = self._build_post_market_targets(watchlist)
        try:
            market_request = SyncRequest(
                start_date=resolved_date,
                end_date=resolved_date,
                stock_codes=stock_codes,
                index_codes=index_codes,
                full_refresh=False,
            )
            market_result = self._ingestion.sync_market(market_request)
            market_status = market_result.run.status.value
            market_step_status = WorkflowStatus.SUCCESS.value if market_status == "success" else WorkflowStatus.FAILED.value
            market_records = market_result.run.records_inserted + market_result.run.records_updated
            market_error = market_result.run.error_message
        except Exception as exc:
            logger.exception("post-market workflow market sync failed before run persisted")
            market_step_status = WorkflowStatus.FAILED.value
            market_records = 0
            market_error = str(exc)
        if market_step_status == WorkflowStatus.FAILED.value:
            error_messages.append(market_error or "market sync failed")
        steps.append(
            WorkflowStepResult(
                name="market_sync",
                status=market_step_status,
                records_affected=market_records,
                error_message=market_error,
                details={
                    "stock_codes": stock_codes,
                    "index_codes": index_codes,
                },
            )
        )

        scan_payload: dict = {}
        latest_scan: dict = {"scan_date": None, "advice": []}
        alerts = self._scanner.list_alerts(unread_only=False)[:10]
        try:
            scan_result = self._scanner.run(scan_date=resolved_date)
            latest_scan = self._scanner.get_latest_scan() or {"scan_date": None, "advice": []}
            scan_payload = scan_result.model_dump()
            scan_step_status = WorkflowStatus.SUCCESS.value
            scan_records = len(scan_result.watchlist_advice) + len(scan_result.position_advice) + len(scan_result.core_instrument_advice)
            scan_error = None
        except Exception as exc:
            logger.exception("post-market workflow scan failed")
            scan_step_status = WorkflowStatus.FAILED.value
            scan_records = 0
            scan_error = str(exc)
            error_messages.append(scan_error)
        steps.append(
            WorkflowStepResult(
                name="daily_scan",
                status=scan_step_status,
                records_affected=scan_records,
                error_message=scan_error,
            )
        )

        status = self._resolve_status(steps)
        overview = self._build_post_market_overview(
            latest_scan,
            steps,
            requested_date,
            resolved_date,
            date_resolution,
        )
        market_overview = self._build_market_overview(resolved_date, latest_scan, steps)
        sector_positioning = self._build_sector_positioning(watch_context, latest_scan)
        position_health = self._build_position_health(watch_context, latest_scan)
        next_day_prep = self._build_next_day_prep(sector_positioning, position_health)
        summary = WorkflowSummary(
            title="盘后复盘",
            overview=overview,
            requested_date=requested_date,
            resolved_date=resolved_date,
            date_resolution=date_resolution,
            market_overview=market_overview,
            sector_positioning=sector_positioning,
            position_health=position_health,
            next_day_prep=next_day_prep,
            watch_context=watch_context,
            alerts=alerts[:8],
            metadata={
                "data_sources": ["market_sync", "daily_scan", "alerts", "watchlist"],
                "steps_completed": len([step for step in steps if step.status == WorkflowStatus.SUCCESS.value]),
                "steps_total": len(steps),
            },
            watchlist=watchlist,
            scan={
                "latest": latest_scan,
                "result": scan_payload,
            },
            scheduler={
                "steps_completed": len([step for step in steps if step.status == WorkflowStatus.SUCCESS.value]),
                "steps_total": len(steps),
            },
            steps=steps,
        )
        return self._persist_run(
            workflow_date=resolved_date,
            phase=WorkflowPhase.POST_MARKET,
            triggered_by=triggered_by,
            status=status,
            started_at=started_at,
            finished_at=datetime.now(),
            summary=summary,
            error_message="；".join(error_messages) or None,
        )

    def get_latest_run(self, phase: WorkflowPhase) -> WorkflowRunRecord | None:
        """Return the latest workflow snapshot for one phase."""
        conn = get_conn()
        row = conn.execute(
            """
            SELECT id, workflow_date, phase, triggered_by, status, started_at, finished_at, summary_json, error_message
            FROM workflow_runs
            WHERE phase = ?
            ORDER BY workflow_date DESC, started_at DESC, id DESC
            LIMIT 1
            """,
            [phase.value],
        ).fetchone()
        if row is None:
            return None
        return self._row_to_run(row)

    def list_history(self, limit: int = 20) -> list[WorkflowHistoryItem]:
        """Return recent workflow history rows."""
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT id, workflow_date, phase, triggered_by, status, started_at, finished_at, error_message
            FROM workflow_runs
            ORDER BY started_at DESC, id DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            WorkflowHistoryItem(
                id=row[0],
                workflow_date=str(row[1]),
                phase=str(row[2]),
                triggered_by=str(row[3]),
                status=str(row[4]),
                started_at=row[5].isoformat() if row[5] else "",
                finished_at=row[6].isoformat() if row[6] else None,
                error_message=row[7],
            )
            for row in rows
        ]

    def get_workflow_status(self) -> dict:
        """Return the latest status for both workflow phases."""
        return {
            "pre_market": self._status_summary(self.get_latest_run(WorkflowPhase.PRE_MARKET)),
            "post_market": self._status_summary(self.get_latest_run(WorkflowPhase.POST_MARKET)),
        }

    def _resolve_requested_date(self, workflow_date: str | None) -> str:
        normalized = normalize_scan_date(workflow_date)
        return normalized or date.today().isoformat()

    def _resolve_pre_market_date(self, workflow_date: str | None) -> tuple[str, str, str]:
        requested_date = self._resolve_requested_date(workflow_date)
        if self._should_run_for_trading_day(requested_date):
            return requested_date, requested_date, "exact"
        next_trading_date = self._next_trading_day(requested_date)
        if next_trading_date is not None:
            return requested_date, next_trading_date, "fallback_next_trading_day"
        return requested_date, requested_date, "exact"

    def _resolve_post_market_date(self, workflow_date: str | None) -> tuple[str, str, str]:
        requested_date = self._resolve_requested_date(workflow_date)
        if self._should_run_for_trading_day(requested_date):
            return requested_date, requested_date, "exact"
        previous_trading_date = self._tushare.previous_trading_day(requested_date)
        if previous_trading_date is None:
            previous_trading_date = self._previous_trading_day(requested_date)
        if previous_trading_date is not None:
            return requested_date, previous_trading_date, "fallback_previous_trading_day"
        return requested_date, requested_date, "exact"

    def _next_trading_day(self, target_date: str) -> str | None:
        cursor = date.fromisoformat(target_date)
        for _ in range(14):
            cursor = cursor.fromordinal(cursor.toordinal() + 1)
            cursor_str = cursor.isoformat()
            if self._should_run_for_trading_day(cursor_str):
                return cursor_str
        return None

    def _previous_trading_day(self, target_date: str) -> str | None:
        cursor = date.fromisoformat(target_date)
        for _ in range(14):
            cursor = cursor.fromordinal(cursor.toordinal() - 1)
            cursor_str = cursor.isoformat()
            if self._should_run_for_trading_day(cursor_str):
                return cursor_str
        return None

    def _should_run_for_trading_day(self, target_date: str) -> bool:
        return self._tushare.is_trading_day(target_date)

    def _build_post_market_targets(self, watchlist: dict) -> tuple[list[str], list[str]]:
        stock_codes: list[str] = []
        for item in watchlist.get("watch_stocks", []):
            code = str(item.get("code", "")).strip()
            if code and code not in stock_codes:
                stock_codes.append(code)
        for position in self._load_positions():
            code = str(position.get("stock_code", "")).strip()
            if code and code not in stock_codes:
                stock_codes.append(code)
        if not stock_codes:
            stock_codes = [item.get("code", "") for item in watchlist.get("watch_stocks", []) if item.get("code")]
        return stock_codes, _DEFAULT_INDEX_CODES.copy()

    def _load_watchlist(self) -> WatchlistConfig:
        return self._summary_api.get_watchlist()

    def _build_watch_context(self, watchlist: dict) -> dict:
        positions = self._load_positions()
        return {
            "watch_sectors": watchlist.get("watch_sectors", []),
            "watch_stocks": watchlist.get("watch_stocks", []),
            "open_positions": [
                {
                    "code": position.get("stock_code"),
                    "name": position.get("stock_name"),
                }
                for position in positions
            ],
        }

    def _load_positions(self) -> list[dict]:
        conn = get_conn()
        rows = conn.execute(
            "SELECT stock_code, stock_name FROM portfolio WHERE status = 'open' ORDER BY buy_date DESC"
        ).fetchdf()
        return rows.to_dict(orient="records")

    def _get_latest_news(self, limit: int = 8) -> list[dict]:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT source, source_item_id, title, content, category, published_at, collected_at
            FROM news_items
            ORDER BY COALESCE(published_at, collected_at) DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()
        return rows.to_dict(orient="records")

    def _resolve_status(self, steps: list[WorkflowStepResult]) -> WorkflowStatus:
        statuses = {step.status for step in steps}
        if statuses == {WorkflowStatus.SUCCESS.value}:
            return WorkflowStatus.SUCCESS
        if WorkflowStatus.FAILED.value in statuses and WorkflowStatus.SUCCESS.value in statuses:
            return WorkflowStatus.PARTIAL
        if WorkflowStatus.FAILED.value in statuses:
            return WorkflowStatus.FAILED
        if WorkflowStatus.SKIPPED.value in statuses and len(statuses) == 1:
            return WorkflowStatus.SKIPPED
        return WorkflowStatus.PARTIAL

    def _build_carry_over(self, previous_post_market: WorkflowRunRecord | None) -> dict:
        if previous_post_market is None:
            return {"available": False}
        return {
            "available": True,
            "workflow_date": previous_post_market.workflow_date,
            "overview": previous_post_market.summary.overview,
            "scan": previous_post_market.summary.scan,
            "market_overview": previous_post_market.summary.market_overview,
            "sector_positioning": previous_post_market.summary.sector_positioning,
            "position_health": previous_post_market.summary.position_health,
            "next_day_prep": previous_post_market.summary.next_day_prep,
        }

    def _build_yesterday_recap(self, carry_over: dict) -> dict:
        if not carry_over.get("available"):
            return {
                "summary": "暂无上一交易日盘后结论。",
                "regime": "unknown",
                "key_metrics": {},
                "carry_over_points": [],
            }
        market_overview = carry_over.get("market_overview", {})
        return {
            "summary": carry_over.get("overview") or market_overview.get("summary") or "已有上一交易日盘后结论。",
            "regime": market_overview.get("regime", "unknown"),
            "key_metrics": {
                "up_down_ratio": market_overview.get("breadth", {}).get("up_down_ratio"),
                "ratio_5d_avg": market_overview.get("breadth", {}).get("ratio_5d_avg"),
                "limit_up_count": market_overview.get("limit_stats", {}).get("limit_up_count"),
                "limit_down_count": market_overview.get("limit_stats", {}).get("limit_down_count"),
                "broken_board_count": market_overview.get("limit_stats", {}).get("broken_board_count"),
                "max_consecutive_board": market_overview.get("limit_stats", {}).get("max_consecutive_board"),
                "style_label": market_overview.get("style", {}).get("style_label"),
            },
            "carry_over_points": carry_over.get("next_day_prep", {}).get("tomorrow_checkpoints", []),
        }

    def _build_overnight_news(self, news_status: str, news_items: list[dict]) -> dict:
        highlights = news_items[:5]
        categorized = {
            "macro": [item for item in news_items if item.get("category") == "macro"],
            "industry": [item for item in news_items if item.get("category") == "industry"],
            "company": [item for item in news_items if item.get("category") == "company"],
            "geopolitics": [item for item in news_items if item.get("category") == "geopolitics"],
            "overseas": [item for item in news_items if item.get("category") == "overseas"],
        }
        summary = f"夜间信息同步状态：{news_status}；共整理 {len(news_items)} 条信息。"
        return {
            "summary": summary,
            "status": news_status,
            "highlights": highlights,
            "categorized": categorized,
            "sector_mappings": [],
        }

    def _build_today_watchlist(self, watch_context: dict, alerts: list[dict]) -> dict:
        watch_sectors = watch_context.get("watch_sectors", [])
        open_positions = watch_context.get("open_positions", [])
        return {
            "market_checkpoints": [
                "开盘前30分钟涨跌比是否明显强于昨日",
                "成交额节奏是否延续上一交易日风险偏好",
                "市场对隔夜信息反应是强化还是钝化",
            ],
            "focus_sectors": [
                {
                    "sector_name": sector,
                    "role": "watch_sector",
                    "trend_5d": "unknown",
                    "consistency": None,
                    "leader_stock": None,
                    "today_observation": f"观察 {sector} 是否进入今日主线。",
                }
                for sector in watch_sectors[:8]
            ],
            "position_watch": [
                {
                    "code": item.get("code"),
                    "name": item.get("name") or item.get("code"),
                    "role": "position",
                    "cost_price": None,
                    "today_observation": f"跟踪 {item.get('name') or item.get('code')} 的盘中强弱与量价配合。",
                    "risk_note": None,
                }
                for item in open_positions[:8]
            ],
            "latest_alerts": alerts[:5],
        }

    def _build_action_frame(self, carry_over: dict, alerts: list[dict]) -> dict:
        next_day_prep = carry_over.get("next_day_prep", {}) if carry_over else {}
        risk_notes = next_day_prep.get("risk_notes") or [item.get("title") for item in alerts[:3] if item.get("title")]
        return {
            "posture": next_day_prep.get("market_bias", "observe"),
            "focus_directions": next_day_prep.get("focus_sectors", []),
            "risk_warnings": risk_notes,
            "notes": next_day_prep.get("tomorrow_checkpoints", []),
        }

    def _build_market_overview(self, workflow_date: str, latest_scan: dict, steps: list[WorkflowStepResult]) -> dict:
        advice_count = len(latest_scan.get("advice", []))
        return {
            "summary": f"{workflow_date} 盘后已完成 {len([step for step in steps if step.status == WorkflowStatus.SUCCESS.value])}/{len(steps)} 个步骤，生成 {advice_count} 条观察结果。",
            "regime": "neutral",
            "confidence": "low",
            "indices": [],
            "breadth": {
                "up_count": None,
                "down_count": None,
                "up_down_ratio": None,
                "ratio_5d_avg": None,
            },
            "limit_stats": {
                "limit_up_count": None,
                "limit_down_count": None,
                "broken_board_count": None,
                "broken_board_rate": None,
                "max_consecutive_board": None,
            },
            "style": {
                "style_label": "unknown",
                "hs300_5d_pct": None,
                "zz1000_5d_pct": None,
                "relative_strength_5d": None,
            },
            "risk_proxies": [],
            "key_takeaways": [
                f"workflow 日期：{workflow_date}",
                f"观察结果数量：{advice_count}",
            ],
        }

    def _build_sector_positioning(self, watch_context: dict, latest_scan: dict) -> dict:
        leaders = []
        for advice in latest_scan.get("advice", [])[:5]:
            leaders.append(
                {
                    "sector_name": advice.get("stock_name") or advice.get("stock_code"),
                    "pct_change": None,
                    "net_flow": None,
                    "leader_stock": advice.get("stock_name") or advice.get("stock_code"),
                }
            )
        watch_sectors = [
            {
                "sector_name": sector,
                "role": "watch_sector",
                "trend_5d": "unknown",
                "consistency": None,
                "leader_stock": None,
                "observation_note": f"观察 {sector} 是否强化为当日主线。",
                "status": "unknown",
            }
            for sector in watch_context.get("watch_sectors", [])[:8]
        ]
        return {
            "market_leaders": leaders,
            "market_laggards": [],
            "watch_sectors": watch_sectors,
            "observation_focus": [
                "优先确认当日主线是否与固定观察池重合",
                "若主线不在观察池内，仅记录不立即扩展系统范围",
            ],
        }

    def _build_position_health(self, watch_context: dict, latest_scan: dict) -> dict:
        advice_map = {item.get("stock_code"): item for item in latest_scan.get("advice", [])}
        tracked_items = []
        for item in watch_context.get("open_positions", []):
            advice = advice_map.get(item.get("code"), {})
            tracked_items.append(
                {
                    "subject_type": "position",
                    "code": item.get("code"),
                    "name": item.get("name") or item.get("code"),
                    "role": "position",
                    "pct_change": None,
                    "turnover_rate": None,
                    "volume_ratio": None,
                    "state": self._map_advice_to_state(advice),
                    "observation_note": self._build_observation_note(item, advice),
                    "risk_flags": advice.get("risk_alerts", []),
                }
            )
        for item in watch_context.get("watch_stocks", [])[:5]:
            advice = advice_map.get(item.get("code"), {})
            tracked_items.append(
                {
                    "subject_type": "watch_stock",
                    "code": item.get("code"),
                    "name": item.get("name") or item.get("code"),
                    "role": "watch_stock",
                    "pct_change": None,
                    "turnover_rate": None,
                    "volume_ratio": None,
                    "state": self._map_advice_to_state(advice),
                    "observation_note": self._build_observation_note(item, advice),
                    "risk_flags": advice.get("risk_alerts", []),
                }
            )
        return {
            "portfolio_health_summary": f"当前纳入 {len(tracked_items)} 个持仓/观察对象的健康度跟踪。",
            "sector_health": [],
            "tracked_items": tracked_items,
        }

    def _build_next_day_prep(self, sector_positioning: dict, position_health: dict) -> dict:
        watch_sector_names = [item.get("sector_name") for item in sector_positioning.get("watch_sectors", []) if item.get("sector_name")]
        risky_items = [
            item.get("name")
            for item in position_health.get("tracked_items", [])
            if item.get("state") == "breakdown"
        ]
        return {
            "market_bias": "observe",
            "focus_sectors": watch_sector_names[:5],
            "focus_items": [item.get("name") for item in position_health.get("tracked_items", [])[:5] if item.get("name")],
            "risk_notes": [f"重点跟踪 {name} 的风险演化。" for name in risky_items[:3]],
            "tomorrow_checkpoints": [
                "确认盘前关注方向是否获得开盘资金响应",
                "确认持仓对象是否延续强弱分化",
            ],
        }

    def _map_advice_to_state(self, advice: dict) -> str:
        action = advice.get("action")
        if action in {"建仓", "关注"}:
            return "breakout"
        if action in {"减仓", "清仓"}:
            return "breakdown"
        if action in {"持有", "观望"}:
            return "watch"
        return "neutral"

    def _build_observation_note(self, item: dict, advice: dict) -> str:
        reasons = advice.get("reasons") or []
        if isinstance(reasons, list) and reasons:
            return "；".join(reasons[:2])
        return f"继续观察 {item.get('name') or item.get('code')} 的强弱变化。"

    def _build_pre_market_overview(
        self,
        previous_post_market: WorkflowRunRecord | None,
        news_items: list[dict],
        watchlist: dict,
        alerts: list[dict],
        requested_date: str,
        resolved_date: str,
        date_resolution: str,
    ) -> str:
        post_market_text = "已有上一交易日盘后结果" if previous_post_market else "缺少上一交易日盘后结果"
        news_text = f"夜间新闻 {len(news_items)} 条"
        watch_text = f"关注板块 {len(watchlist.get('watch_sectors', []))} 个，关注股票 {len(watchlist.get('watch_stocks', []))} 个"
        alert_text = f"未读/最新预警 {min(len(alerts), 8)} 条"
        date_text = f"日期：{resolved_date}"
        if date_resolution == "fallback_next_trading_day":
            date_text = f"请求日期 {requested_date} 为非交易日，已切换到下一个交易日 {resolved_date}"
        return "；".join([date_text, post_market_text, news_text, watch_text, alert_text])

    def _build_post_market_overview(
        self,
        latest_scan: dict,
        steps: list[WorkflowStepResult],
        requested_date: str,
        resolved_date: str,
        date_resolution: str,
    ) -> str:
        latest_date = latest_scan.get("scan_date") or "未生成扫描日期"
        completed_steps = len([step for step in steps if step.status == WorkflowStatus.SUCCESS.value])
        date_text = f"日期：{resolved_date}"
        if date_resolution == "fallback_previous_trading_day":
            date_text = f"请求日期 {requested_date} 为非交易日，已切换到上一个交易日 {resolved_date}"
        return f"{date_text}；已完成 {completed_steps}/{len(steps)} 个步骤；最新扫描日期：{latest_date}"

    def _persist_run(
        self,
        workflow_date: str,
        phase: WorkflowPhase,
        triggered_by: WorkflowTrigger,
        status: WorkflowStatus,
        started_at: datetime,
        finished_at: datetime,
        summary: WorkflowSummary,
        error_message: str | None,
    ) -> WorkflowRunRecord:
        run = WorkflowRunRecord(
            id=time.time_ns(),
            workflow_date=workflow_date,
            phase=phase,
            triggered_by=triggered_by,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            summary=summary,
            error_message=error_message,
        )
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO workflow_runs (
                id, workflow_date, phase, triggered_by, status,
                started_at, finished_at, summary_json, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.id,
                workflow_date,
                phase.value,
                triggered_by.value,
                status.value,
                started_at,
                finished_at,
                json.dumps(summary.model_dump(), ensure_ascii=False),
                error_message,
            ],
        )
        return run

    def _row_to_run(self, row: tuple) -> WorkflowRunRecord:
        return WorkflowRunRecord(
            id=row[0],
            workflow_date=str(row[1]),
            phase=WorkflowPhase(str(row[2])),
            triggered_by=WorkflowTrigger(str(row[3])),
            status=WorkflowStatus(str(row[4])),
            started_at=row[5],
            finished_at=row[6],
            summary=WorkflowSummary(**json.loads(row[7])),
            error_message=row[8],
        )

    def _status_summary(self, run: WorkflowRunRecord | None) -> dict:
        if run is None:
            return {"available": False}
        return {
            "available": True,
            "workflow_date": run.workflow_date,
            "phase": run.phase.value,
            "status": run.status.value,
            "started_at": run.started_at.isoformat(),
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        }

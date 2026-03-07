"""Phase-one ingestion orchestration service."""

from datetime import datetime

from loguru import logger

from tradepilot.data import MOCK_INDICES, MOCK_STOCKS, get_provider
from tradepilot.db import get_conn
from tradepilot.ingestion.models import (
    BilibiliSyncRequest,
    IngestionRun,
    NewsSyncRequest,
    RunStatus,
    SourceType,
    SyncRequest,
    SyncResult,
    TriggerMode,
)


_STOCK_DAILY_COLS = "stock_code, date, open, high, low, close, volume, amount, turnover"
_STOCK_WEEKLY_COLS = _STOCK_DAILY_COLS
_STOCK_MONTHLY_COLS = _STOCK_DAILY_COLS
_INDEX_DAILY_COLS = "index_code, date, open, high, low, close, volume, amount"


def _insert_df(conn, table: str, columns: str, df_name: str, df) -> None:
    """Register a DataFrame and INSERT OR REPLACE into a table with explicit column mapping."""
    col_list = [c.strip() for c in columns.split(",")]
    # Reorder the DataFrame to match table column order
    reordered = df[col_list]
    conn.register(df_name, reordered)
    conn.execute(f"INSERT OR REPLACE INTO {table} SELECT {columns} FROM {df_name}")
    conn.unregister(df_name)


class IngestionService:
    """Coordinate manual sync flows and persist run history."""

    def sync_market(self, request: SyncRequest) -> SyncResult:
        """Run a market sync and persist its execution history."""
        run = IngestionRun(
            id=int(datetime.now().timestamp() * 1000),
            job_name="market_sync",
            source_type=SourceType.MARKET,
            trigger_mode=TriggerMode.MANUAL,
            status=RunStatus.RUNNING,
            started_at=datetime.now(),
        )
        try:
            inserted = self._do_market_sync(request)
            run.status = RunStatus.SUCCESS
            run.records_discovered = inserted
            run.records_inserted = inserted
        except Exception as exc:
            logger.exception("market sync failed")
            run.status = RunStatus.FAILED
            run.error_message = str(exc)

        run.finished_at = datetime.now()
        self._save_run(run)
        return SyncResult(run=run, message=f"market sync {run.status.value}")

    def sync_news(self, request: NewsSyncRequest) -> SyncResult:
        """Run a news sync and persist its execution history."""
        run = IngestionRun(
            id=int(datetime.now().timestamp() * 1000),
            job_name="news_sync",
            source_type=SourceType.NEWS,
            trigger_mode=TriggerMode.MANUAL,
            status=RunStatus.RUNNING,
            started_at=datetime.now(),
        )
        try:
            from tradepilot.collector.news import NewsCollector

            collector = NewsCollector()
            items = collector.collect(stock_codes=request.stock_codes or None)
            run.status = RunStatus.SUCCESS
            run.records_discovered = len(items)
            run.records_inserted = len(items)
        except Exception as exc:
            logger.exception("news sync failed")
            run.status = RunStatus.FAILED
            run.error_message = str(exc)

        run.finished_at = datetime.now()
        self._save_run(run)
        return SyncResult(run=run, message=f"news sync {run.status.value}")

    def sync_bilibili(self, request: BilibiliSyncRequest) -> SyncResult:
        """Run a Bilibili video sync and persist its execution history."""
        run = IngestionRun(
            id=int(datetime.now().timestamp() * 1000),
            job_name="bilibili_sync",
            source_type=SourceType.BILIBILI,
            trigger_mode=TriggerMode.MANUAL,
            status=RunStatus.RUNNING,
            started_at=datetime.now(),
        )
        try:
            from tradepilot.collector.bilibili import BilibiliCollector

            collector = BilibiliCollector()
            items = collector.collect(video_urls=request.video_urls or None)
            run.status = RunStatus.SUCCESS
            run.records_discovered = len(items)
            run.records_inserted = len(items)
        except Exception as exc:
            logger.exception("bilibili sync failed")
            run.status = RunStatus.FAILED
            run.error_message = str(exc)

        run.finished_at = datetime.now()
        self._save_run(run)
        return SyncResult(run=run, message=f"bilibili sync {run.status.value}")

    def get_runs(self) -> list[dict]:
        """Return ingestion run history."""
        conn = get_conn()
        rows = conn.execute("SELECT * FROM ingestion_runs ORDER BY started_at DESC").fetchdf()
        return rows.to_dict(orient="records")

    def get_status(self) -> dict:
        """Return high-level ingestion status."""
        return {
            "provider": type(get_provider()).__name__,
            "sources": [source.value for source in SourceType],
        }

    def _do_market_sync(self, request: SyncRequest) -> int:
        """Fetch market data from provider and write to DuckDB."""
        provider = get_provider()
        conn = get_conn()

        stock_codes = request.stock_codes or list(MOCK_STOCKS.keys())[:3]
        index_codes = request.index_codes or list(MOCK_INDICES.keys())[:2]

        inserted = 0
        for stock_code in stock_codes:
            daily_df = provider.get_stock_daily(stock_code, request.start_date, request.end_date)
            weekly_df = provider.get_stock_weekly(stock_code, request.start_date, request.end_date)
            monthly_df = provider.get_stock_monthly(stock_code, request.start_date, request.end_date)

            _insert_df(conn, "stock_daily", _STOCK_DAILY_COLS, "tmp_daily", daily_df)
            _insert_df(conn, "stock_weekly", _STOCK_WEEKLY_COLS, "tmp_weekly", weekly_df)
            _insert_df(conn, "stock_monthly", _STOCK_MONTHLY_COLS, "tmp_monthly", monthly_df)
            inserted += len(daily_df) + len(weekly_df) + len(monthly_df)

        for index_code in index_codes:
            index_df = provider.get_index_daily(index_code, request.start_date, request.end_date)
            _insert_df(conn, "index_daily", _INDEX_DAILY_COLS, "tmp_index", index_df)
            inserted += len(index_df)

        return inserted

    def _save_run(self, run: IngestionRun) -> None:
        """Persist one ingestion run into DuckDB."""
        conn = get_conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO ingestion_runs (
                id, job_name, source_type, trigger_mode, status, started_at, finished_at,
                records_discovered, records_inserted, records_updated, records_failed, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.id,
                run.job_name,
                run.source_type.value,
                run.trigger_mode.value,
                run.status.value,
                run.started_at,
                run.finished_at,
                run.records_discovered,
                run.records_inserted,
                run.records_updated,
                run.records_failed,
                run.error_message,
            ],
        )

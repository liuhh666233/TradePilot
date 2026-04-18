"""Stage-one orchestration service for ETF all-weather data foundations."""

from __future__ import annotations

from datetime import date, datetime, timedelta
import json
from pathlib import Path
import time
from typing import Callable

import duckdb
import pandas as pd

from tradepilot.config import ETF_AW_DATA_ROOT
from tradepilot.data.tushare_client import TushareClient
from tradepilot.db import get_conn
from tradepilot.etf_all_weather.calendar import build_rebalance_calendar
from tradepilot.etf_all_weather.models import (
    EtfAllWeatherCalendarSyncRequest,
    EtfAllWeatherCurveSyncRequest,
    EtfAllWeatherFeatureSnapshotRequest,
    EtfAllWeatherInitResult,
    EtfAllWeatherJobResult,
    EtfAllWeatherJobRun,
    EtfAllWeatherMarketSyncRequest,
    EtfAllWeatherRegimeSnapshotRequest,
    EtfAllWeatherSlowMacroSyncRequest,
    EtfAllWeatherRunStatus,
)
from tradepilot.etf_all_weather.storage import (
    build_benchmark_index_raw_path,
    build_curve_partition_dir,
    build_curve_raw_path,
    build_daily_market_partition_dir,
    build_monthly_feature_snapshot_dir,
    build_monthly_regime_snapshot_dir,
    build_slow_field_partition_dir,
    build_slow_macro_raw_path,
    build_sleeve_market_raw_path,
    build_trade_calendar_raw_path,
    ensure_storage_layout,
    write_frame_json_records,
    write_json_payload,
)


_CANONICAL_SLEEVES: tuple[str, ...] = (
    "510300.SH",
    "159845.SZ",
    "511010.SH",
    "518850.SH",
    "159001.SZ",
)

_BENCHMARK_INDEXES: tuple[str, ...] = (
    "000300.SH",
    "000852.SH",
)

_M1_DEFINITION_BOUNDARY = "2025-01"
_CURVE_CODE = "1001.CB"
_NEUTRAL_RISK_BUDGETS = {
    "equity_large": 0.25,
    "equity_small": 0.15,
    "bond": 0.30,
    "gold": 0.20,
    "cash": 0.10,
}
_SLEEVE_ROLE_BY_CODE = {
    "510300.SH": "equity_large",
    "159845.SZ": "equity_small",
    "511010.SH": "bond",
    "518850.SH": "gold",
    "159001.SZ": "cash",
}


class EtfAllWeatherStageOneService:
    """Own schema bootstrap and stage-one ETF all-weather jobs."""

    def __init__(
        self,
        *,
        client: TushareClient | None = None,
        conn_factory: Callable[[], duckdb.DuckDBPyConnection] = get_conn,
        data_root: Path = ETF_AW_DATA_ROOT,
    ) -> None:
        self._client = client or TushareClient()
        self._conn_factory = conn_factory
        self._data_root = data_root

    def initialize_schema(self) -> EtfAllWeatherInitResult:
        """Apply the stage-one DDL and create the storage layout."""

        paths = ensure_storage_layout(self._data_root)
        ddl = self._ddl_path().read_text(encoding="utf-8")
        self._conn_factory().execute(ddl)
        return EtfAllWeatherInitResult(
            schema_applied=True,
            storage_root=str(paths["root"]),
            raw_root=str(paths["raw"]),
            normalized_root=str(paths["normalized"]),
            derived_root=str(paths["derived"]),
        )

    def get_status(self) -> dict:
        """Return high-level stage-one readiness and table counts."""

        conn = self._conn_factory()
        table_names = [
            "canonical_sleeves",
            "canonical_trading_calendar",
            "canonical_rebalance_calendar",
            "etf_aw_ingestion_runs",
            "etf_aw_raw_batches",
        ]
        tables: dict[str, dict[str, int | bool]] = {}
        for table_name in table_names:
            exists = self._table_exists(conn, table_name)
            row_count = 0
            if exists:
                row_count = int(conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
            tables[table_name] = {"exists": exists, "row_count": row_count}
        paths = ensure_storage_layout(self._data_root)
        return {
            "provider_enabled": self._client.enabled,
            "storage_root": str(paths["root"]),
            "tables": tables,
        }

    def sync_trading_calendar(self, request: EtfAllWeatherCalendarSyncRequest) -> EtfAllWeatherJobResult:
        """Sync the canonical trading and rebalance calendars."""

        self.initialize_schema()
        run = EtfAllWeatherJobRun(
            run_id=self._next_id(),
            job_name="trading_calendar_sync",
            dataset_name="trade_calendar",
            source_name="tushare",
            status=EtfAllWeatherRunStatus.RUNNING,
            started_at=datetime.now(),
        )
        raw_batch_id = self._next_id()
        try:
            calendar_df = self._client.get_trade_calendar(
                request.start_date,
                request.end_date,
                exchange=request.exchange,
            )
            if calendar_df.empty:
                raise ValueError("trade calendar fetch returned no rows")
            rebalance_df = build_rebalance_calendar(
                calendar_df,
                calendar_source="trade_calendar_sync",
            )
            raw_path = build_trade_calendar_raw_path(
                exchange=request.exchange,
                start_date=request.start_date,
                end_date=request.end_date,
                raw_batch_id=raw_batch_id,
                root=self._data_root,
            )
            row_count, content_hash = write_frame_json_records(calendar_df, raw_path)
            conn = self._conn_factory()
            self._insert_run(conn, run)
            self._insert_raw_batch(
                conn,
                raw_batch_id=raw_batch_id,
                run_id=run.run_id,
                dataset_name="trade_calendar",
                source_endpoint="trade_cal",
                storage_path=raw_path,
                file_format="json",
                partition_year=int(request.start_date[:4]),
                partition_month=int(request.start_date[5:7]),
                window_start=request.start_date,
                window_end=request.end_date,
                row_count=row_count,
                content_hash=content_hash,
            )
            self._upsert_trading_calendar(conn, calendar_df)
            self._upsert_rebalance_calendar(conn, rebalance_df)
            self._record_calendar_validations(conn, run.run_id, raw_batch_id, calendar_df, rebalance_df)
            self._update_watermark(conn, dataset_name="trade_calendar", source_name="tushare", latest_fetched_date=request.end_date, run_id=run.run_id)
            run.status = EtfAllWeatherRunStatus.SUCCESS
            run.records_discovered = len(calendar_df)
            run.records_inserted = len(calendar_df) + len(rebalance_df)
        except Exception as exc:
            run.status = EtfAllWeatherRunStatus.FAILED
            run.records_failed = 1
            run.error_message = str(exc)
        run.finished_at = datetime.now()
        self._upsert_run(self._conn_factory(), run)
        return EtfAllWeatherJobResult(
            run=run,
            message=f"trading calendar sync {run.status.value}",
            details={
                "exchange": request.exchange,
                "start_date": request.start_date,
                "end_date": request.end_date,
            },
        )

    def sync_sleeve_daily_market(self, request: EtfAllWeatherMarketSyncRequest) -> EtfAllWeatherJobResult:
        """Sync the canonical five-sleeve daily ETF market history."""

        self.initialize_schema()
        conn = self._conn_factory()
        run = EtfAllWeatherJobRun(
            run_id=self._next_id(),
            job_name="sleeve_daily_market_sync",
            dataset_name="sleeve_daily_market",
            source_name="tushare",
            status=EtfAllWeatherRunStatus.RUNNING,
            started_at=datetime.now(),
        )
        self._insert_run(conn, run)
        details: dict[str, str | int | None] | None = None
        try:
            frames: list[pd.DataFrame] = []
            raw_rows = 0
            for sleeve_code in _CANONICAL_SLEEVES:
                daily_df = self._client.get_fund_daily(sleeve_code, request.start_date, request.end_date)
                adj_df = self._client.get_fund_adj(sleeve_code, request.start_date, request.end_date)
                if daily_df.empty:
                    raise ValueError(f"fund_daily returned no rows for {sleeve_code}")
                if adj_df.empty:
                    raise ValueError(f"fund_adj returned no rows for {sleeve_code}")
                raw_batch_id = self._next_id()
                raw_path = build_sleeve_market_raw_path(
                    sleeve_code=sleeve_code,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    raw_batch_id=raw_batch_id,
                    root=self._data_root,
                )
                row_count, content_hash = write_json_payload(
                    {
                        "sleeve_code": sleeve_code,
                        "daily": daily_df.to_dict(orient="records"),
                        "adj": adj_df.to_dict(orient="records"),
                    },
                    raw_path,
                )
                self._insert_raw_batch(
                    conn,
                    raw_batch_id=raw_batch_id,
                    run_id=run.run_id,
                    dataset_name="sleeve_daily_market",
                    source_endpoint="fund_daily+fund_adj",
                    storage_path=raw_path,
                    file_format="json",
                    partition_year=int(request.start_date[:4]),
                    partition_month=int(request.start_date[5:7]),
                    window_start=request.start_date,
                    window_end=request.end_date,
                    row_count=row_count,
                    content_hash=content_hash,
                )
                frames.append(self._normalize_sleeve_market_frame(sleeve_code, daily_df, adj_df, raw_batch_id))
                raw_rows += row_count
            market_frame = pd.concat(frames, ignore_index=True).sort_values(["instrument_code", "trade_date"])
            parquet_files = self._write_daily_market_partitions(conn, market_frame, run.run_id)
            self._record_daily_market_validations(conn, run.run_id, "sleeve_daily_market", market_frame)
            self._update_watermark(
                conn,
                dataset_name="sleeve_daily_market",
                source_name="tushare",
                latest_fetched_date=request.end_date,
                run_id=run.run_id,
            )
            run.status = EtfAllWeatherRunStatus.SUCCESS
            run.records_discovered = raw_rows
            run.records_inserted = len(market_frame)
            details = {
                "sleeves_synced": len(_CANONICAL_SLEEVES),
                "parquet_files": parquet_files,
                "start_date": request.start_date,
                "end_date": request.end_date,
            }
        except Exception as exc:
            run.status = EtfAllWeatherRunStatus.FAILED
            run.records_failed = 1
            run.error_message = str(exc)
        run.finished_at = datetime.now()
        self._upsert_run(conn, run)
        return EtfAllWeatherJobResult(
            run=run,
            message=f"sleeve daily market sync {run.status.value}",
            details=details,
        )

    def sync_benchmark_index_daily_market(self, request: EtfAllWeatherMarketSyncRequest) -> EtfAllWeatherJobResult:
        """Sync the benchmark index history for market-confirmation inputs."""

        self.initialize_schema()
        conn = self._conn_factory()
        run = EtfAllWeatherJobRun(
            run_id=self._next_id(),
            job_name="benchmark_index_daily_sync",
            dataset_name="benchmark_index_daily",
            source_name="tushare",
            status=EtfAllWeatherRunStatus.RUNNING,
            started_at=datetime.now(),
        )
        self._insert_run(conn, run)
        details: dict[str, str | int | None] | None = None
        try:
            frames: list[pd.DataFrame] = []
            raw_rows = 0
            for index_code in _BENCHMARK_INDEXES:
                index_df = self._client.get_index_daily(index_code, request.start_date, request.end_date)
                if index_df.empty:
                    raise ValueError(f"index_daily returned no rows for {index_code}")
                raw_batch_id = self._next_id()
                raw_path = build_benchmark_index_raw_path(
                    index_code=index_code,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    raw_batch_id=raw_batch_id,
                    root=self._data_root,
                )
                row_count, content_hash = write_frame_json_records(index_df, raw_path)
                self._insert_raw_batch(
                    conn,
                    raw_batch_id=raw_batch_id,
                    run_id=run.run_id,
                    dataset_name="benchmark_index_daily",
                    source_endpoint="index_daily",
                    storage_path=raw_path,
                    file_format="json",
                    partition_year=int(request.start_date[:4]),
                    partition_month=int(request.start_date[5:7]),
                    window_start=request.start_date,
                    window_end=request.end_date,
                    row_count=row_count,
                    content_hash=content_hash,
                )
                frames.append(self._normalize_index_market_frame(index_code, index_df, raw_batch_id))
                raw_rows += row_count
            market_frame = pd.concat(frames, ignore_index=True).sort_values(["instrument_code", "trade_date"])
            parquet_files = self._write_daily_market_partitions(conn, market_frame, run.run_id)
            self._record_daily_market_validations(conn, run.run_id, "benchmark_index_daily", market_frame)
            self._update_watermark(
                conn,
                dataset_name="benchmark_index_daily",
                source_name="tushare",
                latest_fetched_date=request.end_date,
                run_id=run.run_id,
            )
            run.status = EtfAllWeatherRunStatus.SUCCESS
            run.records_discovered = raw_rows
            run.records_inserted = len(market_frame)
            details = {
                "indexes_synced": len(_BENCHMARK_INDEXES),
                "parquet_files": parquet_files,
                "start_date": request.start_date,
                "end_date": request.end_date,
            }
        except Exception as exc:
            run.status = EtfAllWeatherRunStatus.FAILED
            run.records_failed = 1
            run.error_message = str(exc)
        run.finished_at = datetime.now()
        self._upsert_run(conn, run)
        return EtfAllWeatherJobResult(
            run=run,
            message=f"benchmark index daily sync {run.status.value}",
            details=details,
        )

    def sync_slow_macro(self, request: EtfAllWeatherSlowMacroSyncRequest) -> EtfAllWeatherJobResult:
        """Sync the v1 primary slow macro field set with timing metadata."""

        self.initialize_schema()
        conn = self._conn_factory()
        run = EtfAllWeatherJobRun(
            run_id=self._next_id(),
            job_name="slow_macro_sync",
            dataset_name="slow_macro",
            source_name="tushare",
            status=EtfAllWeatherRunStatus.RUNNING,
            started_at=datetime.now(),
        )
        self._insert_run(conn, run)
        details: dict[str, str | int | None] | None = None
        try:
            datasets = {
                "cn_pmi": self._client.get_cn_pmi(request.start_month, request.end_month),
                "cn_ppi": self._client.get_cn_ppi(request.start_month, request.end_month),
                "cn_m": self._client.get_cn_money_supply(request.start_month, request.end_month),
                "sf_month": self._client.get_sf_month(request.start_month, request.end_month),
            }
            raw_rows = 0
            for dataset_code, frame in datasets.items():
                if frame.empty:
                    raise ValueError(f"{dataset_code} returned no rows")
                raw_batch_id = self._next_id()
                raw_path = build_slow_macro_raw_path(
                    dataset_code=dataset_code,
                    start_month=request.start_month,
                    end_month=request.end_month,
                    raw_batch_id=raw_batch_id,
                    root=self._data_root,
                )
                row_count, content_hash = write_frame_json_records(frame, raw_path)
                self._insert_raw_batch(
                    conn,
                    raw_batch_id=raw_batch_id,
                    run_id=run.run_id,
                    dataset_name="slow_macro",
                    source_endpoint=dataset_code,
                    storage_path=raw_path,
                    file_format="json",
                    partition_year=int(request.start_month[:4]),
                    partition_month=int(request.start_month[5:7]),
                    window_start=f"{request.start_month}-01",
                    window_end=f"{request.end_month}-01",
                    row_count=row_count,
                    content_hash=content_hash,
                )
                raw_rows += row_count
            slow_frame = self._build_slow_macro_frame(conn, request, datasets)
            parquet_files = self._write_slow_field_partitions(conn, slow_frame, run.run_id)
            self._record_slow_macro_validations(conn, run.run_id, slow_frame)
            self._update_watermark(
                conn,
                dataset_name="slow_macro",
                source_name="tushare",
                latest_fetched_date=f"{request.end_month}-01",
                run_id=run.run_id,
            )
            run.status = EtfAllWeatherRunStatus.SUCCESS
            run.records_discovered = raw_rows
            run.records_inserted = len(slow_frame)
            details = {
                "fields_synced": slow_frame["field_name"].nunique(),
                "rows_written": len(slow_frame),
                "parquet_files": parquet_files,
            }
        except Exception as exc:
            run.status = EtfAllWeatherRunStatus.FAILED
            run.records_failed = 1
            run.error_message = str(exc)
        run.finished_at = datetime.now()
        self._upsert_run(conn, run)
        return EtfAllWeatherJobResult(
            run=run,
            message=f"slow macro sync {run.status.value}",
            details=details,
        )

    def sync_curve(self, request: EtfAllWeatherCurveSyncRequest) -> EtfAllWeatherJobResult:
        """Sync China government curve points with windowed extraction."""

        self.initialize_schema()
        conn = self._conn_factory()
        run = EtfAllWeatherJobRun(
            run_id=self._next_id(),
            job_name="curve_sync",
            dataset_name="curve",
            source_name="tushare",
            status=EtfAllWeatherRunStatus.RUNNING,
            started_at=datetime.now(),
        )
        self._insert_run(conn, run)
        details: dict[str, str | int | None] | None = None
        try:
            curve_frames: list[pd.DataFrame] = []
            raw_rows = 0
            for window_start, window_end in self._iter_date_windows(request.start_date, request.end_date, request.window_days):
                frame = self._client.get_yc_cb(_CURVE_CODE, window_start, window_end, curve_type="0")
                if frame.empty:
                    continue
                raw_batch_id = self._next_id()
                raw_path = build_curve_raw_path(
                    curve_code=_CURVE_CODE,
                    start_date=window_start,
                    end_date=window_end,
                    raw_batch_id=raw_batch_id,
                    root=self._data_root,
                )
                row_count, content_hash = write_frame_json_records(frame, raw_path)
                self._insert_raw_batch(
                    conn,
                    raw_batch_id=raw_batch_id,
                    run_id=run.run_id,
                    dataset_name="curve",
                    source_endpoint="yc_cb",
                    storage_path=raw_path,
                    file_format="json",
                    partition_year=int(window_start[:4]),
                    partition_month=int(window_start[5:7]),
                    window_start=window_start,
                    window_end=window_end,
                    row_count=row_count,
                    content_hash=content_hash,
                )
                curve_frames.append(self._normalize_curve_frame(frame, raw_batch_id))
                raw_rows += row_count
            if not curve_frames:
                raise ValueError("yc_cb returned no rows across all extraction windows")
            curve_frame = pd.concat(curve_frames, ignore_index=True)
            curve_frame = curve_frame.drop_duplicates(subset=["curve_code", "curve_date", "tenor_years", "source_name"])
            parquet_files = self._write_curve_partitions(conn, curve_frame, run.run_id)
            self._record_curve_validations(conn, run.run_id, curve_frame)
            self._update_watermark(
                conn,
                dataset_name="curve",
                source_name="tushare",
                latest_fetched_date=request.end_date,
                run_id=run.run_id,
            )
            run.status = EtfAllWeatherRunStatus.SUCCESS
            run.records_discovered = raw_rows
            run.records_inserted = len(curve_frame)
            details = {
                "windows_processed": len(list(self._iter_date_windows(request.start_date, request.end_date, request.window_days))),
                "rows_written": len(curve_frame),
                "parquet_files": parquet_files,
            }
        except Exception as exc:
            run.status = EtfAllWeatherRunStatus.FAILED
            run.records_failed = 1
            run.error_message = str(exc)
        run.finished_at = datetime.now()
        self._upsert_run(conn, run)
        return EtfAllWeatherJobResult(
            run=run,
            message=f"curve sync {run.status.value}",
            details=details,
        )

    def build_monthly_feature_snapshot(self, request: EtfAllWeatherFeatureSnapshotRequest) -> EtfAllWeatherJobResult:
        """Build the explainability-ready monthly as-of feature snapshot dataset."""

        self.initialize_schema()
        conn = self._conn_factory()
        run = EtfAllWeatherJobRun(
            run_id=self._next_id(),
            job_name="monthly_feature_snapshot_build",
            dataset_name="monthly_feature_snapshot",
            source_name="derived",
            status=EtfAllWeatherRunStatus.RUNNING,
            started_at=datetime.now(),
        )
        self._insert_run(conn, run)
        details: dict[str, str | int | None] | None = None
        try:
            rebalance_dates = self._load_rebalance_dates(conn, request)
            snapshots = [self._build_snapshot_row(conn, rebalance_date, run.run_id) for rebalance_date in rebalance_dates]
            snapshot_frame = pd.DataFrame(snapshots)
            parquet_files = self._write_monthly_feature_snapshot_partitions(conn, snapshot_frame, run.run_id)
            self._record_monthly_feature_snapshot_validations(conn, run.run_id, snapshot_frame)
            self._update_watermark(
                conn,
                dataset_name="monthly_feature_snapshot",
                source_name="derived",
                latest_fetched_date=str(snapshot_frame["rebalance_date"].max().date()),
                run_id=run.run_id,
            )
            run.status = EtfAllWeatherRunStatus.SUCCESS
            run.records_discovered = len(snapshot_frame)
            run.records_inserted = len(snapshot_frame)
            details = {
                "rebalance_dates": len(snapshot_frame),
                "parquet_files": parquet_files,
            }
        except Exception as exc:
            run.status = EtfAllWeatherRunStatus.FAILED
            run.records_failed = 1
            run.error_message = str(exc)
        run.finished_at = datetime.now()
        self._upsert_run(conn, run)
        return EtfAllWeatherJobResult(
            run=run,
            message=f"monthly feature snapshot build {run.status.value}",
            details=details,
        )

    def build_monthly_regime_snapshot(self, request: EtfAllWeatherRegimeSnapshotRequest) -> EtfAllWeatherJobResult:
        """Build rule-based regime, confidence, and target budgets from feature snapshots."""

        self.initialize_schema()
        conn = self._conn_factory()
        run = EtfAllWeatherJobRun(
            run_id=self._next_id(),
            job_name="monthly_regime_snapshot_build",
            dataset_name="monthly_regime_snapshot",
            source_name="derived",
            status=EtfAllWeatherRunStatus.RUNNING,
            started_at=datetime.now(),
        )
        self._insert_run(conn, run)
        details: dict[str, str | int | None] | None = None
        try:
            feature_frame = self._load_feature_snapshot_rows(conn, request)
            regime_rows = [self._score_regime_row(record, run.run_id) for record in feature_frame.to_dict(orient="records")]
            regime_frame = pd.DataFrame(regime_rows)
            parquet_files = self._write_monthly_regime_snapshot_partitions(conn, regime_frame, run.run_id)
            self._record_monthly_regime_snapshot_validations(conn, run.run_id, regime_frame)
            self._update_watermark(
                conn,
                dataset_name="monthly_regime_snapshot",
                source_name="derived",
                latest_fetched_date=str(pd.Timestamp(regime_frame["rebalance_date"].max()).date()),
                run_id=run.run_id,
            )
            run.status = EtfAllWeatherRunStatus.SUCCESS
            run.records_discovered = len(regime_frame)
            run.records_inserted = len(regime_frame)
            details = {
                "rebalance_dates": len(regime_frame),
                "parquet_files": parquet_files,
            }
        except Exception as exc:
            run.status = EtfAllWeatherRunStatus.FAILED
            run.records_failed = 1
            run.error_message = str(exc)
        run.finished_at = datetime.now()
        self._upsert_run(conn, run)
        return EtfAllWeatherJobResult(
            run=run,
            message=f"monthly regime snapshot build {run.status.value}",
            details=details,
        )

    def _ddl_path(self) -> Path:
        return Path(__file__).resolve().parents[2] / "docs" / "etf-all-weather-implementation" / "ddl-v1-schema.sql"

    def _table_exists(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        query = """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
        """
        return bool(conn.execute(query, [table_name]).fetchone()[0])

    def _insert_run(self, conn: duckdb.DuckDBPyConnection, run: EtfAllWeatherJobRun) -> None:
        conn.execute(
            """
            INSERT INTO etf_aw_ingestion_runs (
                run_id, job_name, dataset_name, source_name, trigger_mode, status,
                started_at, finished_at, records_discovered, records_inserted,
                records_failed, error_message, code_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.run_id,
                run.job_name,
                run.dataset_name,
                run.source_name,
                "manual",
                run.status.value,
                run.started_at,
                run.finished_at,
                run.records_discovered,
                run.records_inserted,
                run.records_failed,
                run.error_message,
                "stage1",
            ],
        )

    def _upsert_run(self, conn: duckdb.DuckDBPyConnection, run: EtfAllWeatherJobRun) -> None:
        conn.execute(
            """
            INSERT OR REPLACE INTO etf_aw_ingestion_runs (
                run_id, job_name, dataset_name, source_name, trigger_mode, status,
                started_at, finished_at, records_discovered, records_inserted,
                records_failed, error_message, code_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run.run_id,
                run.job_name,
                run.dataset_name,
                run.source_name,
                "manual",
                run.status.value,
                run.started_at,
                run.finished_at,
                run.records_discovered,
                run.records_inserted,
                run.records_failed,
                run.error_message,
                "stage1",
            ],
        )

    def _insert_raw_batch(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        raw_batch_id: int,
        run_id: int,
        dataset_name: str,
        source_endpoint: str,
        storage_path: Path,
        file_format: str,
        partition_year: int,
        partition_month: int,
        window_start: str,
        window_end: str,
        row_count: int,
        content_hash: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO etf_aw_raw_batches (
                raw_batch_id, run_id, dataset_name, source_name, source_endpoint,
                storage_path, file_format, partition_year, partition_month,
                window_start, window_end, row_count, content_hash, fetched_at,
                schema_version, is_fallback_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                raw_batch_id,
                run_id,
                dataset_name,
                "tushare",
                source_endpoint,
                str(storage_path),
                file_format,
                partition_year,
                partition_month,
                window_start,
                window_end,
                row_count,
                content_hash,
                datetime.now(),
                "v1",
                False,
            ],
        )

    def _upsert_trading_calendar(self, conn: duckdb.DuckDBPyConnection, calendar_df: pd.DataFrame) -> None:
        frame = calendar_df.copy()
        frame["exchange"] = frame["exchange"].astype(str)
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame["pretrade_date"] = pd.to_datetime(frame["pretrade_date"], errors="coerce")
        frame["calendar_source"] = "tushare_trade_cal"
        frame = frame.loc[:, ["exchange", "trade_date", "is_open", "pretrade_date", "calendar_source"]]
        conn.register("tmp_etf_aw_trading_calendar", frame)
        conn.execute(
            """
            DELETE FROM canonical_trading_calendar AS target
            USING tmp_etf_aw_trading_calendar AS source
            WHERE target.exchange = source.exchange
              AND target.trade_date = source.trade_date
            """
        )
        conn.execute(
            """
            INSERT INTO canonical_trading_calendar (
                exchange, trade_date, is_open, pretrade_date, calendar_source
            )
            SELECT exchange, trade_date, is_open, pretrade_date, calendar_source
            FROM tmp_etf_aw_trading_calendar
            """
        )
        conn.unregister("tmp_etf_aw_trading_calendar")

    def _upsert_rebalance_calendar(self, conn: duckdb.DuckDBPyConnection, rebalance_df: pd.DataFrame) -> None:
        if rebalance_df.empty:
            return
        frame = rebalance_df.copy()
        frame["rebalance_date"] = pd.to_datetime(frame["rebalance_date"], errors="coerce")
        frame["previous_rebalance_date"] = pd.to_datetime(frame["previous_rebalance_date"], errors="coerce")
        conn.register("tmp_etf_aw_rebalance_calendar", frame)
        conn.execute(
            """
            DELETE FROM canonical_rebalance_calendar AS target
            USING tmp_etf_aw_rebalance_calendar AS source
            WHERE target.rebalance_date = source.rebalance_date
            """
        )
        conn.execute(
            """
            INSERT INTO canonical_rebalance_calendar (
                rebalance_date, calendar_month, rule_name,
                anchor_day, previous_rebalance_date, calendar_source
            )
            SELECT rebalance_date, calendar_month, rule_name, anchor_day, previous_rebalance_date, calendar_source
            FROM tmp_etf_aw_rebalance_calendar
            """
        )
        conn.unregister("tmp_etf_aw_rebalance_calendar")

    def _record_calendar_validations(
        self,
        conn: duckdb.DuckDBPyConnection,
        run_id: int,
        raw_batch_id: int,
        calendar_df: pd.DataFrame,
        rebalance_df: pd.DataFrame,
    ) -> None:
        duplicate_count = int(calendar_df.duplicated(subset=["exchange", "trade_date"]).sum())
        validations = pd.DataFrame(
            [
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": raw_batch_id,
                    "dataset_name": "trade_calendar",
                    "check_name": "duplicate_trade_date_check",
                    "check_level": "error",
                    "status": "success" if duplicate_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(duplicate_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"duplicate_count": {duplicate_count}}}',
                },
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": raw_batch_id,
                    "dataset_name": "trade_calendar",
                    "check_name": "rebalance_rows_generated",
                    "check_level": "warning",
                    "status": "success" if not rebalance_df.empty else "failed",
                    "subject_key": None,
                    "metric_value": float(len(rebalance_df)),
                    "threshold_value": 1.0,
                    "details_json": f'{{"rebalance_rows": {len(rebalance_df)}}}',
                },
            ]
        )
        conn.register("tmp_etf_aw_validations", validations)
        conn.execute(
            """
            INSERT INTO etf_aw_validation_results (
                validation_id, run_id, raw_batch_id, dataset_name, check_name,
                check_level, status, subject_key, metric_value, threshold_value, details_json
            )
            SELECT validation_id, run_id, raw_batch_id, dataset_name, check_name,
                   check_level, status, subject_key, metric_value, threshold_value, details_json
            FROM tmp_etf_aw_validations
            """
        )
        conn.unregister("tmp_etf_aw_validations")

    def _normalize_sleeve_market_frame(
        self,
        sleeve_code: str,
        daily_df: pd.DataFrame,
        adj_df: pd.DataFrame,
        raw_batch_id: int,
    ) -> pd.DataFrame:
        frame = daily_df.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], format="%Y%m%d", errors="coerce")
        frame["pct_chg"] = pd.to_numeric(frame.get("pct_chg"), errors="coerce")
        adj = adj_df.copy()
        adj["trade_date"] = pd.to_datetime(adj["trade_date"], format="%Y%m%d", errors="coerce")
        merged = frame.merge(adj.loc[:, ["trade_date", "adj_factor"]], on="trade_date", how="left")
        if merged["adj_factor"].isna().any():
            raise ValueError(f"missing adj_factor rows for {sleeve_code}")
        merged = merged.sort_values("trade_date").reset_index(drop=True)
        merged["instrument_code"] = sleeve_code
        merged["adj_close"] = merged["close"] * merged["adj_factor"]
        merged["adj_pct_chg"] = merged["adj_close"].pct_change() * 100.0
        if not merged.empty:
            merged.loc[merged.index[0], "adj_pct_chg"] = None
        merged["source_name"] = "tushare"
        merged["source_trade_date"] = merged["trade_date"]
        merged["raw_batch_id"] = raw_batch_id
        merged["quality_status"] = "ok"
        merged["dataset_year"] = merged["trade_date"].dt.year.astype(int)
        merged["dataset_month"] = merged["trade_date"].dt.month.astype(int)
        return merged.loc[
            :,
            [
                "instrument_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "pct_chg",
                "adj_pct_chg",
                "vol",
                "amount",
                "source_name",
                "source_trade_date",
                "raw_batch_id",
                "quality_status",
                "dataset_year",
                "dataset_month",
            ],
        ]

    def _normalize_index_market_frame(
        self,
        index_code: str,
        index_df: pd.DataFrame,
        raw_batch_id: int,
    ) -> pd.DataFrame:
        frame = index_df.copy()
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["instrument_code"] = index_code
        frame["trade_date"] = frame["date"]
        frame["adj_close"] = frame["close"]
        frame["pct_chg"] = frame["close"].pct_change() * 100.0
        frame["adj_pct_chg"] = frame["pct_chg"]
        if not frame.empty:
            frame.loc[frame.index[0], "pct_chg"] = None
            frame.loc[frame.index[0], "adj_pct_chg"] = None
        frame["source_name"] = "tushare"
        frame["source_trade_date"] = frame["trade_date"]
        frame["raw_batch_id"] = raw_batch_id
        frame["quality_status"] = "ok"
        frame["dataset_year"] = frame["trade_date"].dt.year.astype(int)
        frame["dataset_month"] = frame["trade_date"].dt.month.astype(int)
        return frame.loc[
            :,
            [
                "instrument_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "pct_chg",
                "adj_pct_chg",
                "volume",
                "amount",
                "source_name",
                "source_trade_date",
                "raw_batch_id",
                "quality_status",
                "dataset_year",
                "dataset_month",
            ],
        ].rename(columns={"volume": "vol"})

    def _write_daily_market_partitions(
        self,
        conn: duckdb.DuckDBPyConnection,
        market_frame: pd.DataFrame,
        run_id: int,
    ) -> int:
        file_count = 0
        for (dataset_year, dataset_month), partition in market_frame.groupby(["dataset_year", "dataset_month"], sort=True):
            output_dir = build_daily_market_partition_dir(
                dataset_year=int(dataset_year),
                dataset_month=int(dataset_month),
                root=self._data_root,
            )
            output_path = output_dir / f"canonical_daily_market_fact__run_{run_id}.parquet"
            conn.register("tmp_etf_aw_daily_market", partition.reset_index(drop=True))
            conn.execute(f"COPY tmp_etf_aw_daily_market TO '{output_path.as_posix()}' (FORMAT PARQUET)")
            conn.unregister("tmp_etf_aw_daily_market")
            file_count += 1
        return file_count

    def _record_daily_market_validations(
        self,
        conn: duckdb.DuckDBPyConnection,
        run_id: int,
        dataset_name: str,
        market_frame: pd.DataFrame,
    ) -> None:
        duplicate_count = int(market_frame.duplicated(subset=["instrument_code", "trade_date"]).sum())
        null_adj_count = int(market_frame["adj_close"].isna().sum())
        validations = pd.DataFrame(
            [
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": dataset_name,
                    "check_name": "duplicate_instrument_trade_date_check",
                    "check_level": "error",
                    "status": "success" if duplicate_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(duplicate_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"duplicate_count": {duplicate_count}}}',
                },
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": dataset_name,
                    "check_name": "adj_close_presence_check",
                    "check_level": "error",
                    "status": "success" if null_adj_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(null_adj_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"null_adj_close_count": {null_adj_count}}}',
                },
            ]
        )
        conn.register("tmp_etf_aw_market_validations", validations)
        conn.execute(
            """
            INSERT INTO etf_aw_validation_results (
                validation_id, run_id, raw_batch_id, dataset_name, check_name,
                check_level, status, subject_key, metric_value, threshold_value, details_json
            )
            SELECT validation_id, run_id, raw_batch_id, dataset_name, check_name,
                   check_level, status, subject_key, metric_value, threshold_value, details_json
            FROM tmp_etf_aw_market_validations
            """
        )
        conn.unregister("tmp_etf_aw_market_validations")

    def _build_slow_macro_frame(
        self,
        conn: duckdb.DuckDBPyConnection,
        request: EtfAllWeatherSlowMacroSyncRequest,
        datasets: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        calendar_dates = self._load_open_trading_days(
            conn,
            start_date=f"{request.start_month}-01",
            end_date=(self._month_start(request.end_month) + timedelta(days=40)).isoformat(),
        )
        pmi = datasets["cn_pmi"].copy()
        pmi["period_label"] = pmi["month"].astype(str)
        pmi["value"] = pd.to_numeric(self._select_first_existing_column(pmi, ["pmi", "manufacturing", "manufacturing_pmi"]), errors="coerce")
        pmi_rows = self._finalize_slow_field_rows(
            base=pmi,
            field_name="official_pmi",
            release_day=1,
            calendar_dates=calendar_dates,
        )
        pmi_mom = pmi.loc[:, ["period_label", "value"]].copy().sort_values("period_label")
        pmi_mom["value"] = pmi_mom["value"].diff()
        pmi_mom_rows = self._finalize_slow_field_rows(
            base=pmi_mom,
            field_name="official_pmi_mom",
            release_day=1,
            calendar_dates=calendar_dates,
        )
        ppi = datasets["cn_ppi"].copy()
        ppi["period_label"] = ppi["month"].astype(str)
        ppi["value"] = pd.to_numeric(self._select_first_existing_column(ppi, ["ppi_yoy", "ppi", "ppi_mp_rm"]), errors="coerce")
        ppi_rows = self._finalize_slow_field_rows(
            base=ppi,
            field_name="ppi_yoy",
            release_day=12,
            calendar_dates=calendar_dates,
        )
        money = datasets["cn_m"].copy()
        money["period_label"] = money["month"].astype(str)
        money["m1_value"] = pd.to_numeric(self._select_first_existing_column(money, ["m1_yoy", "m1", "m1同比"]), errors="coerce")
        money["m2_value"] = pd.to_numeric(self._select_first_existing_column(money, ["m2_yoy", "m2", "m2同比"]), errors="coerce")
        m1_rows = self._finalize_slow_field_rows(
            base=money.rename(columns={"m1_value": "value"}),
            field_name="m1_yoy",
            release_day=15,
            calendar_dates=calendar_dates,
            definition_regime_series=money["period_label"].map(self._m1_definition_regime),
            regime_note_series=money["period_label"].map(self._m1_regime_note),
        )
        m2_rows = self._finalize_slow_field_rows(
            base=money.rename(columns={"m2_value": "value"}),
            field_name="m2_yoy",
            release_day=15,
            calendar_dates=calendar_dates,
            definition_regime_series=money["period_label"].map(self._m1_definition_regime),
            regime_note_series=money["period_label"].map(self._m1_regime_note),
        )
        spread = money.loc[:, ["period_label", "m1_value", "m2_value"]].copy()
        spread["value"] = spread["m1_value"] - spread["m2_value"]
        spread_rows = self._finalize_slow_field_rows(
            base=spread,
            field_name="m1_m2_spread",
            release_day=15,
            calendar_dates=calendar_dates,
            definition_regime_series=spread["period_label"].map(self._m1_definition_regime),
            regime_note_series=spread["period_label"].map(self._m1_regime_note),
        )
        tsf = datasets["sf_month"].copy()
        tsf["period_label"] = tsf["month"].astype(str)
        tsf["value"] = pd.to_numeric(self._select_first_existing_column(tsf, ["tsf_yoy", "inc_yoy", "afre_yoy"]), errors="coerce")
        tsf_rows = self._finalize_slow_field_rows(
            base=tsf,
            field_name="tsf_yoy",
            release_day=15,
            calendar_dates=calendar_dates,
        )
        return pd.concat([pmi_rows, pmi_mom_rows, ppi_rows, m1_rows, m2_rows, spread_rows, tsf_rows], ignore_index=True)

    def _finalize_slow_field_rows(
        self,
        *,
        base: pd.DataFrame,
        field_name: str,
        release_day: int,
        calendar_dates: list[date],
        definition_regime_series: pd.Series | None = None,
        regime_note_series: pd.Series | None = None,
    ) -> pd.DataFrame:
        frame = base.loc[:, ["period_label", "value"]].copy()
        frame = frame.dropna(subset=["period_label", "value"]).sort_values("period_label").reset_index(drop=True)
        frame["field_name"] = field_name
        frame["period_type"] = "monthly"
        frame["unit"] = "pct"
        frame["release_date"] = frame["period_label"].map(lambda label: self._release_date_for_period(label, release_day))
        frame["effective_date"] = frame["release_date"].map(lambda value: self._next_open_date(value, calendar_dates))
        frame["revision_note"] = "latest_history_only_unless_vintage_captured;revision-risk-present"
        frame["definition_regime"] = None
        frame["regime_note"] = None
        if definition_regime_series is not None:
            frame["definition_regime"] = definition_regime_series.values[: len(frame)]
        if regime_note_series is not None:
            frame["regime_note"] = regime_note_series.values[: len(frame)]
        frame["source_name"] = "tushare"
        frame["raw_batch_id"] = None
        frame["quality_status"] = "ok"
        frame["dataset_year"] = frame["period_label"].str.slice(0, 4).astype(int)
        return frame.loc[
            :,
            [
                "field_name",
                "period_label",
                "period_type",
                "value",
                "unit",
                "release_date",
                "effective_date",
                "revision_note",
                "definition_regime",
                "regime_note",
                "source_name",
                "raw_batch_id",
                "quality_status",
                "dataset_year",
            ],
        ]

    def _write_slow_field_partitions(
        self,
        conn: duckdb.DuckDBPyConnection,
        slow_frame: pd.DataFrame,
        run_id: int,
    ) -> int:
        file_count = 0
        for (field_name, dataset_year), partition in slow_frame.groupby(["field_name", "dataset_year"], sort=True):
            output_dir = build_slow_field_partition_dir(
                field_name=str(field_name),
                dataset_year=int(dataset_year),
                root=self._data_root,
            )
            output_path = output_dir / f"canonical_slow_field_fact__run_{run_id}.parquet"
            conn.register("tmp_etf_aw_slow_fields", partition.reset_index(drop=True))
            conn.execute(f"COPY tmp_etf_aw_slow_fields TO '{output_path.as_posix()}' (FORMAT PARQUET)")
            conn.unregister("tmp_etf_aw_slow_fields")
            file_count += 1
        return file_count

    def _record_slow_macro_validations(
        self,
        conn: duckdb.DuckDBPyConnection,
        run_id: int,
        slow_frame: pd.DataFrame,
    ) -> None:
        null_effective_count = int(slow_frame["effective_date"].isna().sum())
        duplicate_count = int(slow_frame.duplicated(subset=["field_name", "period_label", "source_name"]).sum())
        validations = pd.DataFrame(
            [
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": "slow_macro",
                    "check_name": "effective_date_presence_check",
                    "check_level": "error",
                    "status": "success" if null_effective_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(null_effective_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"null_effective_date_count": {null_effective_count}}}',
                },
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": "slow_macro",
                    "check_name": "duplicate_field_period_check",
                    "check_level": "error",
                    "status": "success" if duplicate_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(duplicate_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"duplicate_count": {duplicate_count}}}',
                },
            ]
        )
        conn.register("tmp_etf_aw_slow_validations", validations)
        conn.execute(
            """
            INSERT INTO etf_aw_validation_results (
                validation_id, run_id, raw_batch_id, dataset_name, check_name,
                check_level, status, subject_key, metric_value, threshold_value, details_json
            )
            SELECT validation_id, run_id, raw_batch_id, dataset_name, check_name,
                   check_level, status, subject_key, metric_value, threshold_value, details_json
            FROM tmp_etf_aw_slow_validations
            """
        )
        conn.unregister("tmp_etf_aw_slow_validations")

    def _normalize_curve_frame(self, frame: pd.DataFrame, raw_batch_id: int) -> pd.DataFrame:
        normalized = frame.copy()
        normalized["curve_date"] = pd.to_datetime(
            self._select_first_existing_column(normalized, ["workTime", "trade_date", "date"]),
            errors="coerce",
        )
        normalized["tenor_years"] = pd.to_numeric(
            self._select_first_existing_column(normalized, ["curve_term", "term", "years"]),
            errors="coerce",
        )
        normalized["yield_value"] = pd.to_numeric(
            self._select_first_existing_column(normalized, ["yield", "yield_value", "rate"]),
            errors="coerce",
        )
        normalized["curve_code"] = _CURVE_CODE
        normalized["curve_type"] = "government_spot"
        normalized["source_name"] = "tushare"
        normalized["raw_batch_id"] = raw_batch_id
        normalized["quality_status"] = "ok"
        normalized["dataset_year"] = normalized["curve_date"].dt.year.astype(int)
        normalized["dataset_month"] = normalized["curve_date"].dt.month.astype(int)
        return normalized.loc[
            :,
            [
                "curve_code",
                "curve_date",
                "curve_type",
                "tenor_years",
                "yield_value",
                "source_name",
                "raw_batch_id",
                "quality_status",
                "dataset_year",
                "dataset_month",
            ],
        ].dropna(subset=["curve_date", "tenor_years", "yield_value"])

    def _write_curve_partitions(
        self,
        conn: duckdb.DuckDBPyConnection,
        curve_frame: pd.DataFrame,
        run_id: int,
    ) -> int:
        file_count = 0
        for (dataset_year, dataset_month), partition in curve_frame.groupby(["dataset_year", "dataset_month"], sort=True):
            output_dir = build_curve_partition_dir(
                dataset_year=int(dataset_year),
                dataset_month=int(dataset_month),
                root=self._data_root,
            )
            output_path = output_dir / f"canonical_curve_fact__run_{run_id}.parquet"
            conn.register("tmp_etf_aw_curve", partition.reset_index(drop=True))
            conn.execute(f"COPY tmp_etf_aw_curve TO '{output_path.as_posix()}' (FORMAT PARQUET)")
            conn.unregister("tmp_etf_aw_curve")
            file_count += 1
        return file_count

    def _record_curve_validations(
        self,
        conn: duckdb.DuckDBPyConnection,
        run_id: int,
        curve_frame: pd.DataFrame,
    ) -> None:
        duplicate_count = int(curve_frame.duplicated(subset=["curve_code", "curve_date", "tenor_years", "source_name"]).sum())
        required_tenor_dates = int(
            curve_frame.loc[curve_frame["tenor_years"].isin([1.0, 10.0]), "curve_date"].nunique()
        )
        validations = pd.DataFrame(
            [
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": "curve",
                    "check_name": "duplicate_curve_point_check",
                    "check_level": "error",
                    "status": "success" if duplicate_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(duplicate_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"duplicate_count": {duplicate_count}}}',
                },
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": "curve",
                    "check_name": "required_tenor_presence_check",
                    "check_level": "warning",
                    "status": "success" if required_tenor_dates > 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(required_tenor_dates),
                    "threshold_value": 1.0,
                    "details_json": f'{{"dates_with_required_tenors": {required_tenor_dates}}}',
                },
            ]
        )
        conn.register("tmp_etf_aw_curve_validations", validations)
        conn.execute(
            """
            INSERT INTO etf_aw_validation_results (
                validation_id, run_id, raw_batch_id, dataset_name, check_name,
                check_level, status, subject_key, metric_value, threshold_value, details_json
            )
            SELECT validation_id, run_id, raw_batch_id, dataset_name, check_name,
                   check_level, status, subject_key, metric_value, threshold_value, details_json
            FROM tmp_etf_aw_curve_validations
            """
        )
        conn.unregister("tmp_etf_aw_curve_validations")

    def _load_rebalance_dates(
        self,
        conn: duckdb.DuckDBPyConnection,
        request: EtfAllWeatherFeatureSnapshotRequest,
    ) -> list[pd.Timestamp]:
        query = "SELECT rebalance_date FROM canonical_rebalance_calendar"
        params: list[str] = []
        clauses: list[str] = []
        if request.start_date:
            clauses.append("rebalance_date >= ?")
            params.append(request.start_date)
        if request.end_date:
            clauses.append("rebalance_date <= ?")
            params.append(request.end_date)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY rebalance_date"
        frame = conn.execute(query, params).fetchdf()
        if frame.empty:
            raise ValueError("no rebalance dates available for monthly feature snapshot")
        return [pd.Timestamp(value) for value in pd.to_datetime(frame["rebalance_date"]).tolist()]

    def _build_snapshot_row(
        self,
        conn: duckdb.DuckDBPyConnection,
        rebalance_date: pd.Timestamp,
        run_id: int,
    ) -> dict:
        market = self._load_daily_market_as_of(conn, rebalance_date)
        slow = self._load_slow_fields_as_of(conn, rebalance_date)
        curve = self._load_curve_as_of(conn, rebalance_date)
        feature_payload = {
            "rebalance_date": rebalance_date.strftime("%Y-%m-%d"),
            "sleeves": self._build_sleeve_feature_block(market),
            "market_confirmation": self._build_market_confirmation_block(market),
            "execution": self._build_execution_block(market),
            "slow_macro": slow,
            "rates_curve": curve,
        }
        source_run_set = {
            "market_raw_batch_ids": sorted({int(value) for value in market["raw_batch_id"].dropna().tolist()}),
            "slow_raw_batch_ids": [],
            "curve_raw_batch_ids": sorted({int(value) for value in curve.get("raw_batch_ids", [])}),
            "builder_run_id": run_id,
        }
        return {
            "rebalance_date": rebalance_date,
            "schema_version": "v1",
            "feature_payload_json": json.dumps(feature_payload, ensure_ascii=False, sort_keys=True),
            "source_run_set_json": json.dumps(source_run_set, ensure_ascii=False, sort_keys=True),
            "created_at": datetime.now(),
            "rebalance_year": rebalance_date.year,
        }

    def _load_daily_market_as_of(self, conn: duckdb.DuckDBPyConnection, rebalance_date: pd.Timestamp) -> pd.DataFrame:
        pattern = (self._data_root / "normalized" / "daily_market" / "**" / "*.parquet").as_posix()
        return conn.execute(
            f"""
            WITH market AS (
                SELECT *
                FROM read_parquet('{pattern}', hive_partitioning = true)
                WHERE trade_date <= ?
            ), ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY instrument_code ORDER BY trade_date DESC) AS rn
                FROM market
            )
            SELECT * FROM ranked WHERE rn = 1
            """,
            [rebalance_date],
        ).fetchdf()

    def _load_slow_fields_as_of(self, conn: duckdb.DuckDBPyConnection, rebalance_date: pd.Timestamp) -> dict:
        pattern = (self._data_root / "normalized" / "slow_fields" / "**" / "*.parquet").as_posix()
        frame = conn.execute(
            f"""
            WITH slow AS (
                SELECT *
                FROM read_parquet('{pattern}', hive_partitioning = true)
                WHERE effective_date <= ?
            ), ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY field_name ORDER BY effective_date DESC, period_label DESC) AS rn
                FROM slow
            )
            SELECT * FROM ranked WHERE rn = 1
            """,
            [rebalance_date],
        ).fetchdf()
        payload: dict[str, dict] = {}
        for record in frame.to_dict(orient="records"):
            payload[str(record["field_name"])] = {
                "value": record["value"],
                "period_label": record["period_label"],
                "release_date": str(pd.Timestamp(record["release_date"]).date()),
                "effective_date": str(pd.Timestamp(record["effective_date"]).date()),
                "revision_note": record["revision_note"],
                "definition_regime": record.get("definition_regime"),
                "regime_note": record.get("regime_note"),
            }
        return payload

    def _load_curve_as_of(self, conn: duckdb.DuckDBPyConnection, rebalance_date: pd.Timestamp) -> dict:
        pattern = (self._data_root / "normalized" / "curve" / "**" / "*.parquet").as_posix()
        frame = conn.execute(
            f"""
            WITH curve AS (
                SELECT *
                FROM read_parquet('{pattern}', hive_partitioning = true)
                WHERE curve_date <= ?
            ), latest_date AS (
                SELECT MAX(curve_date) AS curve_date FROM curve
            )
            SELECT *
            FROM curve
            WHERE curve_date = (SELECT curve_date FROM latest_date)
              AND tenor_years IN (1.0, 10.0)
            ORDER BY tenor_years
            """,
            [rebalance_date],
        ).fetchdf()
        if frame.empty:
            return {}
        values = {float(row["tenor_years"]): row["yield_value"] for row in frame.to_dict(orient="records")}
        payload = {
            "curve_date": str(pd.Timestamp(frame.iloc[0]["curve_date"]).date()),
            "cn_gov_1y_yield": values.get(1.0),
            "cn_gov_10y_yield": values.get(10.0),
            "cn_yield_curve_slope_10y_1y": None,
            "raw_batch_ids": sorted({int(value) for value in frame["raw_batch_id"].dropna().tolist()}),
        }
        if values.get(1.0) is not None and values.get(10.0) is not None:
            payload["cn_yield_curve_slope_10y_1y"] = values[10.0] - values[1.0]
        return payload

    def _build_sleeve_feature_block(self, market: pd.DataFrame) -> dict:
        payload: dict[str, dict] = {}
        for record in market.to_dict(orient="records"):
            code = str(record["instrument_code"])
            if code not in _SLEEVE_ROLE_BY_CODE:
                continue
            payload[_SLEEVE_ROLE_BY_CODE[code]] = {
                "instrument_code": code,
                "trade_date": str(pd.Timestamp(record["trade_date"]).date()),
                "close": record["close"],
                "adj_close": record["adj_close"],
                "pct_chg": record["pct_chg"],
                "adj_pct_chg": record["adj_pct_chg"],
                "vol": record["vol"],
                "amount": record["amount"],
            }
        return payload

    def _build_market_confirmation_block(self, market: pd.DataFrame) -> dict:
        market_index = market.set_index("instrument_code")
        payload = {
            "hs300_close": self._safe_market_value(market_index, "000300.SH", "close"),
            "zz1000_close": self._safe_market_value(market_index, "000852.SH", "close"),
            "hs300_vs_zz1000_20d": self._relative_ratio(
                self._safe_market_value(market_index, "000300.SH", "adj_close"),
                self._safe_market_value(market_index, "000852.SH", "adj_close"),
            ),
            "bond_trend_20d": self._safe_market_value(market_index, "511010.SH", "adj_pct_chg"),
            "gold_trend_20d": self._safe_market_value(market_index, "518850.SH", "adj_pct_chg"),
        }
        return payload

    def _build_execution_block(self, market: pd.DataFrame) -> dict:
        market_index = market.set_index("instrument_code")
        return {
            "realized_vol_20d_equity_large": self._safe_market_value(market_index, "510300.SH", "adj_pct_chg"),
            "realized_vol_20d_equity_small": self._safe_market_value(market_index, "159845.SZ", "adj_pct_chg"),
            "realized_vol_20d_bond": self._safe_market_value(market_index, "511010.SH", "adj_pct_chg"),
            "realized_vol_20d_gold": self._safe_market_value(market_index, "518850.SH", "adj_pct_chg"),
            "realized_vol_20d_cash": self._safe_market_value(market_index, "159001.SZ", "adj_pct_chg"),
        }

    def _write_monthly_feature_snapshot_partitions(
        self,
        conn: duckdb.DuckDBPyConnection,
        snapshot_frame: pd.DataFrame,
        run_id: int,
    ) -> int:
        file_count = 0
        for rebalance_year, partition in snapshot_frame.groupby("rebalance_year", sort=True):
            output_dir = build_monthly_feature_snapshot_dir(rebalance_year=int(rebalance_year), root=self._data_root)
            output_path = output_dir / f"monthly_feature_snapshot__run_{run_id}.parquet"
            conn.register("tmp_etf_aw_feature_snapshot", partition.reset_index(drop=True))
            conn.execute(f"COPY tmp_etf_aw_feature_snapshot TO '{output_path.as_posix()}' (FORMAT PARQUET)")
            conn.unregister("tmp_etf_aw_feature_snapshot")
            file_count += 1
        return file_count

    def _record_monthly_feature_snapshot_validations(
        self,
        conn: duckdb.DuckDBPyConnection,
        run_id: int,
        snapshot_frame: pd.DataFrame,
    ) -> None:
        duplicate_count = int(snapshot_frame.duplicated(subset=["rebalance_date"]).sum())
        null_payload_count = int(snapshot_frame["feature_payload_json"].isna().sum())
        validations = pd.DataFrame(
            [
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": "monthly_feature_snapshot",
                    "check_name": "duplicate_rebalance_date_check",
                    "check_level": "error",
                    "status": "success" if duplicate_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(duplicate_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"duplicate_count": {duplicate_count}}}',
                },
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": "monthly_feature_snapshot",
                    "check_name": "feature_payload_presence_check",
                    "check_level": "error",
                    "status": "success" if null_payload_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(null_payload_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"null_payload_count": {null_payload_count}}}',
                },
            ]
        )
        conn.register("tmp_etf_aw_snapshot_validations", validations)
        conn.execute(
            """
            INSERT INTO etf_aw_validation_results (
                validation_id, run_id, raw_batch_id, dataset_name, check_name,
                check_level, status, subject_key, metric_value, threshold_value, details_json
            )
            SELECT validation_id, run_id, raw_batch_id, dataset_name, check_name,
                   check_level, status, subject_key, metric_value, threshold_value, details_json
            FROM tmp_etf_aw_snapshot_validations
            """
        )
        conn.unregister("tmp_etf_aw_snapshot_validations")

    def _load_feature_snapshot_rows(
        self,
        conn: duckdb.DuckDBPyConnection,
        request: EtfAllWeatherRegimeSnapshotRequest,
    ) -> pd.DataFrame:
        pattern = (self._data_root / "derived" / "monthly_feature_snapshot" / "**" / "*.parquet").as_posix()
        query = f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = true)"
        clauses: list[str] = []
        params: list[str] = []
        if request.start_date:
            clauses.append("rebalance_date >= ?")
            params.append(request.start_date)
        if request.end_date:
            clauses.append("rebalance_date <= ?")
            params.append(request.end_date)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY rebalance_date"
        frame = conn.execute(query, params).fetchdf()
        if frame.empty:
            raise ValueError("no monthly feature snapshots available for regime build")
        return frame

    def _score_regime_row(self, record: dict, run_id: int) -> dict:
        feature_payload = json.loads(record["feature_payload_json"])
        slow = feature_payload.get("slow_macro", {})
        market = feature_payload.get("market_confirmation", {})
        rates = feature_payload.get("rates_curve", {})
        growth_components = self._build_growth_components(slow, market)
        credit_components = self._build_credit_components(slow, rates)
        confirmation_components = self._build_confirmation_components(market)
        growth_score = sum(component["score"] for component in growth_components)
        credit_score = sum(component["score"] for component in credit_components)
        confirmation_score = sum(component["score"] for component in confirmation_components)
        macro_balance = 0.6 * growth_score + 0.4 * credit_score
        total_score = macro_balance + 0.3 * confirmation_score
        market_alignment = self._market_alignment_ratio(confirmation_components)
        confidence = self._compute_confidence(growth_score, credit_score, market_alignment)
        regime_label = self._regime_label(total_score, growth_score, credit_score)
        target_budgets = self._target_risk_budgets(total_score, confidence)
        regime_payload = {
            "rebalance_date": feature_payload.get("rebalance_date"),
            "regime_label": regime_label,
            "growth_score": growth_score,
            "credit_score": credit_score,
            "confirmation_score": confirmation_score,
            "total_score": total_score,
            "confidence": confidence,
            "components": {
                "growth": growth_components,
                "credit": credit_components,
                "confirmation": confirmation_components,
            },
            "target_risk_budgets": target_budgets,
        }
        feature_source = json.loads(record["source_run_set_json"])
        source_run_set = {
            "feature_source": feature_source,
            "builder_run_id": run_id,
        }
        rebalance_date = pd.Timestamp(record["rebalance_date"])
        return {
            "rebalance_date": rebalance_date,
            "schema_version": "v1",
            "regime_payload_json": json.dumps(regime_payload, ensure_ascii=False, sort_keys=True),
            "source_run_set_json": json.dumps(source_run_set, ensure_ascii=False, sort_keys=True),
            "created_at": datetime.now(),
            "rebalance_year": rebalance_date.year,
        }

    def _build_growth_components(self, slow: dict, market: dict) -> list[dict]:
        components = [
            self._threshold_component("official_pmi", self._slow_value(slow, "official_pmi"), positive_threshold=50.0),
            self._threshold_component("official_pmi_mom", self._slow_value(slow, "official_pmi_mom"), positive_threshold=0.0),
            self._threshold_component("ppi_yoy", self._slow_value(slow, "ppi_yoy"), positive_threshold=-1.0),
            self._threshold_component("hs300_vs_zz1000_20d", market.get("hs300_vs_zz1000_20d"), positive_threshold=1.0),
        ]
        return components

    def _build_credit_components(self, slow: dict, rates: dict) -> list[dict]:
        components = [
            self._threshold_component("m1_m2_spread", self._slow_value(slow, "m1_m2_spread"), positive_threshold=0.0),
            self._threshold_component("tsf_yoy", self._slow_value(slow, "tsf_yoy"), positive_threshold=8.0),
            self._threshold_component("m1_yoy", self._slow_value(slow, "m1_yoy"), positive_threshold=0.0),
            self._threshold_component(
                "cn_yield_curve_slope_10y_1y",
                rates.get("cn_yield_curve_slope_10y_1y"),
                positive_threshold=0.0,
            ),
        ]
        return components

    def _build_confirmation_components(self, market: dict) -> list[dict]:
        return [
            self._threshold_component("bond_trend_20d", market.get("bond_trend_20d"), positive_threshold=0.0),
            self._threshold_component("gold_trend_20d", market.get("gold_trend_20d"), positive_threshold=0.0),
            self._threshold_component("hs300_vs_zz1000_20d", market.get("hs300_vs_zz1000_20d"), positive_threshold=1.0),
        ]

    def _threshold_component(self, name: str, value, *, positive_threshold: float) -> dict:
        if value is None or pd.isna(value):
            return {"name": name, "value": value, "score": 0.0, "state": "missing"}
        score = 1.0 if float(value) > positive_threshold else -1.0
        return {"name": name, "value": value, "score": score, "state": "positive" if score > 0 else "negative"}

    def _slow_value(self, slow: dict, field_name: str):
        payload = slow.get(field_name)
        if not payload:
            return None
        return payload.get("value")

    def _market_alignment_ratio(self, components: list[dict]) -> float:
        observed = [component for component in components if component["state"] != "missing"]
        if not observed:
            return 0.0
        positive = sum(1 for component in observed if component["score"] > 0)
        negative = len(observed) - positive
        return abs(positive - negative) / len(observed)

    def _compute_confidence(self, growth_score: float, credit_score: float, market_alignment: float) -> float:
        macro_strength = min(1.0, (abs(growth_score) + abs(credit_score)) / 8.0)
        confidence = 0.7 * macro_strength + 0.3 * market_alignment
        return round(float(confidence), 4)

    def _regime_label(self, total_score: float, growth_score: float, credit_score: float) -> str:
        if total_score >= 1.5:
            return "pro_growth"
        if total_score <= -1.5:
            return "defensive"
        if growth_score >= 0 and credit_score < 0:
            return "mixed_growth_without_credit"
        if growth_score < 0 and credit_score >= 0:
            return "mixed_credit_without_growth"
        return "neutral_mixed"

    def _target_risk_budgets(self, total_score: float, confidence: float) -> dict[str, float]:
        budgets = dict(_NEUTRAL_RISK_BUDGETS)
        tilt = max(-1.0, min(1.0, total_score / 4.0)) * confidence * 0.12
        budgets["equity_large"] += tilt * 0.55
        budgets["equity_small"] += tilt * 0.45
        budgets["bond"] -= tilt * 0.5
        budgets["gold"] -= tilt * 0.3
        budgets["cash"] -= tilt * 0.2
        total = sum(budgets.values())
        return {name: round(value / total, 4) for name, value in budgets.items()}

    def _write_monthly_regime_snapshot_partitions(
        self,
        conn: duckdb.DuckDBPyConnection,
        regime_frame: pd.DataFrame,
        run_id: int,
    ) -> int:
        file_count = 0
        for rebalance_year, partition in regime_frame.groupby("rebalance_year", sort=True):
            output_dir = build_monthly_regime_snapshot_dir(rebalance_year=int(rebalance_year), root=self._data_root)
            output_path = output_dir / f"monthly_regime_snapshot__run_{run_id}.parquet"
            conn.register("tmp_etf_aw_regime_snapshot", partition.reset_index(drop=True))
            conn.execute(f"COPY tmp_etf_aw_regime_snapshot TO '{output_path.as_posix()}' (FORMAT PARQUET)")
            conn.unregister("tmp_etf_aw_regime_snapshot")
            file_count += 1
        return file_count

    def _record_monthly_regime_snapshot_validations(
        self,
        conn: duckdb.DuckDBPyConnection,
        run_id: int,
        regime_frame: pd.DataFrame,
    ) -> None:
        duplicate_count = int(regime_frame.duplicated(subset=["rebalance_date"]).sum())
        null_payload_count = int(regime_frame["regime_payload_json"].isna().sum())
        validations = pd.DataFrame(
            [
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": "monthly_regime_snapshot",
                    "check_name": "duplicate_rebalance_date_check",
                    "check_level": "error",
                    "status": "success" if duplicate_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(duplicate_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"duplicate_count": {duplicate_count}}}',
                },
                {
                    "validation_id": self._next_id(),
                    "run_id": run_id,
                    "raw_batch_id": None,
                    "dataset_name": "monthly_regime_snapshot",
                    "check_name": "regime_payload_presence_check",
                    "check_level": "error",
                    "status": "success" if null_payload_count == 0 else "failed",
                    "subject_key": None,
                    "metric_value": float(null_payload_count),
                    "threshold_value": 0.0,
                    "details_json": f'{{"null_payload_count": {null_payload_count}}}',
                },
            ]
        )
        conn.register("tmp_etf_aw_regime_validations", validations)
        conn.execute(
            """
            INSERT INTO etf_aw_validation_results (
                validation_id, run_id, raw_batch_id, dataset_name, check_name,
                check_level, status, subject_key, metric_value, threshold_value, details_json
            )
            SELECT validation_id, run_id, raw_batch_id, dataset_name, check_name,
                   check_level, status, subject_key, metric_value, threshold_value, details_json
            FROM tmp_etf_aw_regime_validations
            """
        )
        conn.unregister("tmp_etf_aw_regime_validations")

    def _safe_market_value(self, market_index: pd.DataFrame, instrument_code: str, field: str):
        if instrument_code not in market_index.index:
            return None
        value = market_index.loc[instrument_code, field]
        if isinstance(value, pd.Series):
            value = value.iloc[0]
        return value

    def _relative_ratio(self, numerator, denominator):
        if numerator in (None, 0) or denominator in (None, 0):
            return None
        return numerator / denominator

    def _load_open_trading_days(self, conn: duckdb.DuckDBPyConnection, *, start_date: str, end_date: str) -> list[date]:
        if self._table_exists(conn, "canonical_trading_calendar"):
            frame = conn.execute(
                """
                SELECT trade_date
                FROM canonical_trading_calendar
                WHERE exchange = 'SSE' AND is_open = TRUE AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date
                """,
                [start_date, end_date],
            ).fetchdf()
            if not frame.empty:
                return [value.date() for value in pd.to_datetime(frame["trade_date"]).tolist()]
        fetched = self._client.get_trade_calendar(start_date, end_date, exchange="SSE")
        if fetched.empty:
            return []
        open_days = fetched.loc[fetched["is_open"]].copy()
        return [value.date() for value in pd.to_datetime(open_days["trade_date"]).tolist()]

    def _iter_date_windows(self, start_date: str, end_date: str, window_days: int) -> list[tuple[str, str]]:
        current = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        windows: list[tuple[str, str]] = []
        while current <= end:
            window_end = min(current + timedelta(days=window_days - 1), end)
            windows.append((current.isoformat(), window_end.isoformat()))
            current = window_end + timedelta(days=1)
        return windows

    def _release_date_for_period(self, period_label: str, release_day: int) -> date:
        month_start = self._month_start(period_label)
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        return next_month.replace(day=release_day)

    def _next_open_date(self, target_date: date, calendar_dates: list[date]) -> date:
        for candidate in calendar_dates:
            if candidate >= target_date:
                return candidate
        fallback = target_date
        while fallback.weekday() >= 5:
            fallback += timedelta(days=1)
        return fallback

    def _month_start(self, period_label: str) -> date:
        return date.fromisoformat(f"{period_label}-01")

    def _select_first_existing_column(self, frame: pd.DataFrame, candidates: list[str]) -> pd.Series:
        for column in candidates:
            if column in frame.columns:
                return frame[column]
        raise ValueError(f"missing expected columns: {', '.join(candidates)}")

    def _m1_definition_regime(self, period_label: str) -> str:
        return "post_2025_m1_definition" if period_label >= _M1_DEFINITION_BOUNDARY else "pre_2025_m1_definition"

    def _m1_regime_note(self, period_label: str) -> str:
        if period_label >= _M1_DEFINITION_BOUNDARY:
            return "post_2025_definition_regime"
        return "pre_2025_definition_regime"

    def _update_watermark(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        dataset_name: str,
        source_name: str,
        latest_fetched_date: str,
        run_id: int,
    ) -> None:
        conn.execute(
            """
            INSERT OR REPLACE INTO etf_aw_source_watermarks (
                dataset_name, source_name, latest_available_date, latest_fetched_date,
                latest_successful_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [dataset_name, source_name, latest_fetched_date, latest_fetched_date, run_id, datetime.now()],
        )

    def _next_id(self) -> int:
        return time.time_ns()

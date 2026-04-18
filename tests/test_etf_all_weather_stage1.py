"""Tests for ETF all-weather stage-one foundations."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from datetime import date, timedelta
import unittest

import duckdb
import pandas as pd

from tradepilot.etf_all_weather.calendar import build_rebalance_calendar
from tradepilot.etf_all_weather.models import (
    EtfAllWeatherCalendarSyncRequest,
    EtfAllWeatherCurveSyncRequest,
    EtfAllWeatherFeatureSnapshotRequest,
    EtfAllWeatherMarketSyncRequest,
    EtfAllWeatherRegimeSnapshotRequest,
    EtfAllWeatherSlowMacroSyncRequest,
)
from tradepilot.etf_all_weather.service import EtfAllWeatherStageOneService


class _FakeTushareClient:
    """Minimal fake client for stage-one calendar tests."""

    enabled = True

    def get_trade_calendar(self, start_date: str, end_date: str, exchange: str = "SSE") -> pd.DataFrame:
        current = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        rows: list[dict] = []
        previous_open: date | None = None
        forced_closed = {
            date(2026, 1, 20),
            date(2026, 2, 20),
        }
        while current <= end:
            is_open = current.weekday() < 5 and current not in forced_closed
            rows.append(
                {
                    "exchange": exchange,
                    "trade_date": pd.Timestamp(current),
                    "is_open": is_open,
                    "pretrade_date": pd.Timestamp(previous_open) if previous_open else pd.NaT,
                }
            )
            if is_open:
                previous_open = current
            current += timedelta(days=1)
        return pd.DataFrame(rows)

    def get_fund_daily(self, fund_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        closes = {
            "510300.SH": [4.0, 4.2, 4.3],
            "159845.SZ": [1.0, 1.1, 1.2],
            "511010.SH": [110.0, 110.2, 110.4],
            "518850.SH": [5.0, 5.1, 5.3],
            "159001.SZ": [100.0, 100.01, 100.02],
        }[fund_code]
        return pd.DataFrame(
            {
                "ts_code": [fund_code] * 3,
                "trade_date": ["20260119", "20260120", "20260121"],
                "open": closes,
                "high": [value + 0.1 for value in closes],
                "low": [value - 0.1 for value in closes],
                "close": closes,
                "vol": [1000.0, 1200.0, 1300.0],
                "amount": [10000.0, 12000.0, 13000.0],
                "pct_chg": [0.0, 1.0, 2.0],
            }
        )

    def get_fund_adj(self, fund_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        factors = {
            "510300.SH": [1.0, 1.0, 1.1],
            "159845.SZ": [1.0, 1.0, 1.0],
            "511010.SH": [1.0, 1.0, 1.02],
            "518850.SH": [1.0, 1.0, 1.0],
            "159001.SZ": [1.0, 1.0, 1.0],
        }[fund_code]
        return pd.DataFrame(
            {
                "ts_code": [fund_code] * 3,
                "trade_date": ["20260119", "20260120", "20260121"],
                "adj_factor": factors,
            }
        )

    def get_index_daily(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        closes = {
            "000300.SH": [3800.0, 3810.0, 3825.0],
            "000852.SH": [6200.0, 6180.0, 6210.0],
        }[index_code]
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-01-19", "2026-01-20", "2026-01-21"]),
                "index_code": [index_code] * 3,
                "open": closes,
                "high": [value + 10 for value in closes],
                "low": [value - 10 for value in closes],
                "close": closes,
                "volume": [1000000.0, 1200000.0, 1300000.0],
                "amount": [1.0e8, 1.1e8, 1.2e8],
            }
        )

    def get_cn_pmi(self, start_month: str, end_month: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "month": ["2024-11", "2024-12", "2025-01"],
                "pmi": [49.5, 50.1, 50.3],
            }
        )

    def get_cn_ppi(self, start_month: str, end_month: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "month": ["2024-11", "2024-12", "2025-01"],
                "ppi_yoy": [-2.5, -2.3, -2.1],
            }
        )

    def get_cn_money_supply(self, start_month: str, end_month: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "month": ["2024-11", "2024-12", "2025-01"],
                "m1_yoy": [-3.5, -2.8, 1.2],
                "m2_yoy": [7.1, 7.3, 7.0],
            }
        )

    def get_sf_month(self, start_month: str, end_month: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "month": ["2024-11", "2024-12", "2025-01"],
                "tsf_yoy": [8.5, 8.8, 9.0],
            }
        )

    def get_yc_cb(self, ts_code: str, start_date: str, end_date: str, curve_type: str = "0") -> pd.DataFrame:
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        if start > pd.Timestamp("2026-01-21") or end < pd.Timestamp("2026-01-19"):
            return pd.DataFrame(columns=["curve_term", "workTime", "yield"])
        rows = []
        for work_day, shift in [("2026-01-19", 0.0), ("2026-01-20", 0.02), ("2026-01-21", 0.05)]:
            for tenor, base in [(1.0, 1.55), (5.0, 1.82), (10.0, 1.95)]:
                rows.append({"curve_term": tenor, "workTime": work_day, "yield": base + shift})
        return pd.DataFrame(rows)


class BuildRebalanceCalendarTests(unittest.TestCase):
    """Verify the frozen monthly decision-clock helper."""

    def test_selects_first_open_day_on_or_after_anchor(self) -> None:
        frame = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(
                    [
                        "2026-01-19",
                        "2026-01-20",
                        "2026-01-21",
                        "2026-02-19",
                        "2026-02-20",
                        "2026-02-23",
                    ]
                ),
                "is_open": [True, False, True, True, False, True],
            }
        )

        result = build_rebalance_calendar(frame)

        self.assertEqual(result["rebalance_date"].dt.strftime("%Y-%m-%d").tolist(), ["2026-01-21", "2026-02-23"])
        previous = result["previous_rebalance_date"].dt.strftime("%Y-%m-%d").tolist()
        self.assertTrue(pd.isna(previous[0]))
        self.assertEqual(previous[1], "2026-01-21")


class EtfAllWeatherStageOneServiceTests(unittest.TestCase):
    """Verify schema bootstrap and the first stage-one job."""

    def test_initialize_schema_creates_seed_tables(self) -> None:
        conn = duckdb.connect(":memory:")
        with TemporaryDirectory() as tmp_dir:
            service = EtfAllWeatherStageOneService(
                client=_FakeTushareClient(),
                conn_factory=lambda: conn,
                data_root=Path(tmp_dir),
            )

            result = service.initialize_schema()

            self.assertTrue(result.schema_applied)
            sleeve_count = conn.execute("SELECT COUNT(*) FROM canonical_sleeves").fetchone()[0]
            self.assertEqual(sleeve_count, 5)

    def test_sync_trading_calendar_populates_calendar_tables(self) -> None:
        conn = duckdb.connect(":memory:")
        with TemporaryDirectory() as tmp_dir:
            service = EtfAllWeatherStageOneService(
                client=_FakeTushareClient(),
                conn_factory=lambda: conn,
                data_root=Path(tmp_dir),
            )

            result = service.sync_trading_calendar(
                EtfAllWeatherCalendarSyncRequest(
                    start_date="2026-01-01",
                    end_date="2026-02-28",
                    exchange="SSE",
                )
            )

            self.assertEqual(result.run.status.value, "success")
            self.assertGreater(conn.execute("SELECT COUNT(*) FROM canonical_trading_calendar").fetchone()[0], 40)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM canonical_rebalance_calendar").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM etf_aw_raw_batches").fetchone()[0], 1)

    def test_sync_sleeve_daily_market_writes_partitioned_parquet(self) -> None:
        conn = duckdb.connect(":memory:")
        with TemporaryDirectory() as tmp_dir:
            service = EtfAllWeatherStageOneService(
                client=_FakeTushareClient(),
                conn_factory=lambda: conn,
                data_root=Path(tmp_dir),
            )

            result = service.sync_sleeve_daily_market(
                EtfAllWeatherMarketSyncRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )

            self.assertEqual(result.run.status.value, "success")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM etf_aw_raw_batches").fetchone()[0], 5)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM etf_aw_validation_results WHERE dataset_name = 'sleeve_daily_market'"
                ).fetchone()[0],
                2,
            )
            parquet_dir = (
                Path(tmp_dir)
                / "normalized"
                / "daily_market"
                / "dataset_year=2026"
                / "dataset_month=01"
            )
            parquet_files = sorted(parquet_dir.glob("*.parquet"))
            self.assertEqual(len(parquet_files), 1)
            normalized_count = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_files[0].as_posix()}')"
            ).fetchone()[0]
            self.assertEqual(normalized_count, 15)

    def test_sync_benchmark_index_daily_market_writes_partitioned_parquet(self) -> None:
        conn = duckdb.connect(":memory:")
        with TemporaryDirectory() as tmp_dir:
            service = EtfAllWeatherStageOneService(
                client=_FakeTushareClient(),
                conn_factory=lambda: conn,
                data_root=Path(tmp_dir),
            )

            result = service.sync_benchmark_index_daily_market(
                EtfAllWeatherMarketSyncRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )

            self.assertEqual(result.run.status.value, "success")
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM etf_aw_raw_batches WHERE dataset_name = 'benchmark_index_daily'").fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM etf_aw_validation_results WHERE dataset_name = 'benchmark_index_daily'"
                ).fetchone()[0],
                2,
            )
            parquet_dir = (
                Path(tmp_dir)
                / "normalized"
                / "daily_market"
                / "dataset_year=2026"
                / "dataset_month=01"
            )
            parquet_files = sorted(parquet_dir.glob("*.parquet"))
            self.assertEqual(len(parquet_files), 1)
            normalized_count = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{parquet_files[0].as_posix()}')"
            ).fetchone()[0]
            self.assertEqual(normalized_count, 6)

    def test_sync_slow_macro_writes_timing_aware_parquet(self) -> None:
        conn = duckdb.connect(":memory:")
        with TemporaryDirectory() as tmp_dir:
            service = EtfAllWeatherStageOneService(
                client=_FakeTushareClient(),
                conn_factory=lambda: conn,
                data_root=Path(tmp_dir),
            )

            service.sync_trading_calendar(
                EtfAllWeatherCalendarSyncRequest(
                    start_date="2024-12-01",
                    end_date="2025-03-31",
                    exchange="SSE",
                )
            )
            result = service.sync_slow_macro(
                EtfAllWeatherSlowMacroSyncRequest(
                    start_month="2024-11",
                    end_month="2025-01",
                )
            )

            self.assertEqual(result.run.status.value, "success")
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM etf_aw_raw_batches WHERE dataset_name = 'slow_macro'").fetchone()[0],
                4,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM etf_aw_validation_results WHERE dataset_name = 'slow_macro'").fetchone()[0],
                2,
            )
            pmi_dir = (
                Path(tmp_dir)
                / "normalized"
                / "slow_fields"
                / "field_name=official_pmi"
                / "dataset_year=2024"
            )
            pmi_files = sorted(pmi_dir.glob("*.parquet"))
            self.assertEqual(len(pmi_files), 1)
            rows = conn.execute(
                f"SELECT field_name, period_label, release_date, effective_date FROM read_parquet('{pmi_files[0].as_posix()}') ORDER BY period_label"
            ).fetchall()
            self.assertEqual(rows[0][0], "official_pmi")
            self.assertEqual(str(rows[0][1]), "2024-11")
            self.assertEqual(str(rows[0][2]), "2024-12-01")
            self.assertIsNotNone(rows[0][3])

    def test_sync_curve_writes_windowed_curve_partitions(self) -> None:
        conn = duckdb.connect(":memory:")
        with TemporaryDirectory() as tmp_dir:
            service = EtfAllWeatherStageOneService(
                client=_FakeTushareClient(),
                conn_factory=lambda: conn,
                data_root=Path(tmp_dir),
            )

            result = service.sync_curve(
                EtfAllWeatherCurveSyncRequest(
                    start_date="2026-01-19",
                    end_date="2026-01-21",
                    window_days=2,
                )
            )

            self.assertEqual(result.run.status.value, "success")
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM etf_aw_raw_batches WHERE dataset_name = 'curve'").fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM etf_aw_validation_results WHERE dataset_name = 'curve'").fetchone()[0],
                2,
            )
            curve_dir = (
                Path(tmp_dir)
                / "normalized"
                / "curve"
                / "dataset_year=2026"
                / "dataset_month=01"
            )
            curve_files = sorted(curve_dir.glob("*.parquet"))
            self.assertEqual(len(curve_files), 1)
            rows = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{curve_files[0].as_posix()}')"
            ).fetchone()[0]
            self.assertEqual(rows, 9)

    def test_build_monthly_feature_snapshot_uses_as_of_features(self) -> None:
        conn = duckdb.connect(":memory:")
        with TemporaryDirectory() as tmp_dir:
            service = EtfAllWeatherStageOneService(
                client=_FakeTushareClient(),
                conn_factory=lambda: conn,
                data_root=Path(tmp_dir),
            )

            service.sync_trading_calendar(
                EtfAllWeatherCalendarSyncRequest(
                    start_date="2024-12-01",
                    end_date="2026-02-28",
                    exchange="SSE",
                )
            )
            service.sync_sleeve_daily_market(
                EtfAllWeatherMarketSyncRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )
            service.sync_benchmark_index_daily_market(
                EtfAllWeatherMarketSyncRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )
            service.sync_slow_macro(
                EtfAllWeatherSlowMacroSyncRequest(
                    start_month="2024-11",
                    end_month="2025-01",
                )
            )
            service.sync_curve(
                EtfAllWeatherCurveSyncRequest(
                    start_date="2026-01-19",
                    end_date="2026-01-21",
                    window_days=2,
                )
            )

            result = service.build_monthly_feature_snapshot(
                EtfAllWeatherFeatureSnapshotRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )

            self.assertEqual(result.run.status.value, "success")
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM etf_aw_validation_results WHERE dataset_name = 'monthly_feature_snapshot'"
                ).fetchone()[0],
                2,
            )
            snapshot_dir = Path(tmp_dir) / "derived" / "monthly_feature_snapshot" / "rebalance_year=2026"
            snapshot_files = sorted(snapshot_dir.glob("*.parquet"))
            self.assertEqual(len(snapshot_files), 1)
            row = conn.execute(
                f"SELECT feature_payload_json FROM read_parquet('{snapshot_files[0].as_posix()}') LIMIT 1"
            ).fetchone()[0]
            self.assertIn('"official_pmi"', row)
            self.assertIn('"hs300_vs_zz1000_20d"', row)
            self.assertIn('"cn_gov_10y_yield"', row)

    def test_build_monthly_regime_snapshot_outputs_confidence_and_budgets(self) -> None:
        conn = duckdb.connect(":memory:")
        with TemporaryDirectory() as tmp_dir:
            service = EtfAllWeatherStageOneService(
                client=_FakeTushareClient(),
                conn_factory=lambda: conn,
                data_root=Path(tmp_dir),
            )

            service.sync_trading_calendar(
                EtfAllWeatherCalendarSyncRequest(
                    start_date="2024-12-01",
                    end_date="2026-02-28",
                    exchange="SSE",
                )
            )
            service.sync_sleeve_daily_market(
                EtfAllWeatherMarketSyncRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )
            service.sync_benchmark_index_daily_market(
                EtfAllWeatherMarketSyncRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )
            service.sync_slow_macro(
                EtfAllWeatherSlowMacroSyncRequest(
                    start_month="2024-11",
                    end_month="2025-01",
                )
            )
            service.sync_curve(
                EtfAllWeatherCurveSyncRequest(
                    start_date="2026-01-19",
                    end_date="2026-01-21",
                    window_days=2,
                )
            )
            service.build_monthly_feature_snapshot(
                EtfAllWeatherFeatureSnapshotRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )

            result = service.build_monthly_regime_snapshot(
                EtfAllWeatherRegimeSnapshotRequest(
                    start_date="2026-01-01",
                    end_date="2026-01-31",
                )
            )

            self.assertEqual(result.run.status.value, "success")
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM etf_aw_validation_results WHERE dataset_name = 'monthly_regime_snapshot'"
                ).fetchone()[0],
                2,
            )
            regime_dir = Path(tmp_dir) / "derived" / "monthly_regime_snapshot" / "rebalance_year=2026"
            regime_files = sorted(regime_dir.glob("*.parquet"))
            self.assertEqual(len(regime_files), 1)
            row = conn.execute(
                f"SELECT regime_payload_json FROM read_parquet('{regime_files[0].as_posix()}') LIMIT 1"
            ).fetchone()[0]
            self.assertIn('"confidence"', row)
            self.assertIn('"regime_label"', row)
            self.assertIn('"target_risk_budgets"', row)


if __name__ == "__main__":
    unittest.main()

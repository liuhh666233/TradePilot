"""Stage E ETF all-weather market-only regime score tests."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import unittest

import pandas as pd

from tradepilot import db
from tradepilot.etl.models import RunStatus
from tradepilot.etl.read_models import get_latest_etf_aw_regime_context
from tradepilot.etl.service import ETLService


class StageERegimeScoreTests(unittest.TestCase):
    """Verify ETF all-weather market-only regime scoring behavior."""

    def setUp(self) -> None:
        self._original_db_path = db.DB_PATH
        self._original_thread_local = db._thread_local
        self._original_initialized = db._initialized
        self._temp_dir = TemporaryDirectory()
        db.DB_PATH = Path(self._temp_dir.name) / "test.duckdb"
        db._thread_local = threading.local()
        db._initialized = False
        self.conn = db.get_conn()
        self.lakehouse_root = Path(self._temp_dir.name) / "lakehouse"
        self.service = ETLService(
            conn=self.conn,
            source_adapters=[],
            lakehouse_root=self.lakehouse_root,
        )

    def tearDown(self) -> None:
        conn = getattr(db._thread_local, "conn", None)
        if conn is not None:
            conn.close()
        db._thread_local = self._original_thread_local
        db.DB_PATH = self._original_db_path
        db._initialized = self._original_initialized
        self._temp_dir.cleanup()

    def test_complete_risk_on_score_is_capped_at_market_only_confidence(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
                self._row("511010.SH", "bond", -0.02, -0.04, -0.06),
                self._row("518850.SH", "gold", -0.02, -0.04, -0.06),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )

        result = self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        self.assertEqual(result["status"], RunStatus.SUCCESS.value)
        frame = self._read_score_file(2024, 7)
        row = frame.iloc[0]
        self.assertEqual(row["market_regime_label"], "risk_on")
        self.assertEqual(row["scoring_status"], "complete")
        self.assertLessEqual(row["confidence_score"], 0.70)
        self.assertEqual(row["confidence_cap"], 0.70)

    def test_strong_gold_fixture_scores_hedge_bid(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", -0.02, -0.04, -0.06),
                self._row("159845.SZ", "equity_small", -0.02, -0.04, -0.06),
                self._row("511010.SH", "bond", 0.0, 0.0, 0.0),
                self._row("518850.SH", "gold", 0.02, 0.04, 0.06),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )

        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_score_file(2024, 7)
        self.assertEqual(frame.iloc[0]["market_regime_label"], "hedge_bid")

    def test_negative_equity_and_stable_bond_scores_defensive(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", -0.02, -0.04, -0.06),
                self._row("159845.SZ", "equity_small", -0.02, -0.04, -0.06),
                self._row("511010.SH", "bond", 0.0, 0.0, 0.0),
                self._row("518850.SH", "gold", 0.0, 0.0, 0.0),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )

        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_score_file(2024, 7)
        self.assertEqual(frame.iloc[0]["market_regime_label"], "defensive")

    def test_mixed_signals_score_mixed(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.0, 0.0, 0.0),
                self._row("159845.SZ", "equity_small", 0.0, 0.0, 0.0),
                self._row("511010.SH", "bond", 0.0, 0.0, 0.0),
                self._row("518850.SH", "gold", 0.0, 0.0, 0.0),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )

        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_score_file(2024, 7)
        self.assertEqual(frame.iloc[0]["market_regime_label"], "mixed")

    def test_missing_sleeve_data_scores_unavailable(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
                self._row("511010.SH", "bond", 0.0, 0.0, 0.0),
                self._row("518850.SH", "gold", None, None, None, "missing"),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )

        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_score_file(2024, 7)
        row = frame.iloc[0]
        self.assertEqual(row["scoring_status"], "unavailable")
        self.assertEqual(row["market_regime_label"], "insufficient_data")
        self.assertLessEqual(row["confidence_score"], 0.20)

    def test_stale_and_partial_inputs_cap_confidence(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06, "stale"),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
                self._row("511010.SH", "bond", 0.0, 0.0, 0.0),
                self._row("518850.SH", "gold", 0.0, 0.0, 0.0),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )

        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        stale = self._read_score_file(2024, 7).iloc[0]
        self.assertEqual(stale["scoring_status"], "degraded")
        self.assertLessEqual(stale["confidence_score"], 0.35)

        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06, "partial"),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
                self._row("511010.SH", "bond", 0.0, 0.0, 0.0),
                self._row("518850.SH", "gold", 0.0, 0.0, 0.0),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )
        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        partial = self._read_score_file(2024, 7).iloc[0]
        self.assertEqual(partial["scoring_status"], "degraded")
        self.assertLessEqual(partial["confidence_score"], 0.55)

    def test_repeat_rebuild_upserts_and_read_service_returns_contract(self) -> None:
        self._write_snapshot(
            [
                self._row("510300.SH", "equity_large", 0.02, 0.04, 0.06),
                self._row("159845.SZ", "equity_small", 0.02, 0.04, 0.06),
                self._row("511010.SH", "bond", -0.02, -0.04, -0.06),
                self._row("518850.SH", "gold", -0.02, -0.04, -0.06),
                self._row("159001.SZ", "cash", 0.0, 0.0, 0.0),
            ]
        )
        self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )
        result = self.service.run_bootstrap(
            "derived.etf_aw_regime_score.build",
            start=date(2024, 7, 1),
            end=date(2024, 7, 31),
        )

        frame = self._read_score_file(2024, 7)
        self.assertEqual(result["records_updated"], 1)
        self.assertEqual(len(frame), 1)
        self.assertFalse(
            frame.duplicated(
                ["calendar_name", "rebalance_date", "scorer_name", "scorer_version"]
            ).any()
        )
        context = get_latest_etf_aw_regime_context(
            as_of_date=date(2024, 7, 31),
            lakehouse_root=self.lakehouse_root,
        )
        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(context["schema_version"], "etf_aw_regime_score_v1")
        self.assertEqual(context["market_regime_label"], "risk_on")
        self.assertEqual(len(context["signals"]), 5)
        self.assertNotIn("target_weight", context)
        self.assertNotIn("trade_action", context)

    def _row(
        self,
        sleeve_code: str,
        sleeve_role: str,
        return_1m: float | None,
        return_3m: float | None,
        return_6m: float | None,
        data_status: str = "complete",
    ) -> dict:
        return {
            "calendar_name": "etf_aw_v1_monthly_post_20",
            "calendar_month": "2024-07",
            "rebalance_date": date(2024, 7, 22),
            "effective_date": date(2024, 7, 22),
            "sleeve_code": sleeve_code,
            "sleeve_role": sleeve_role,
            "close": 10.0 if data_status != "missing" else None,
            "adj_factor": 1.0 if data_status != "missing" else None,
            "adj_close": 10.0 if data_status != "missing" else None,
            "return_1m": return_1m,
            "return_3m": return_3m,
            "return_6m": return_6m,
            "volatility_3m": 0.10 if data_status != "missing" else None,
            "max_drawdown_6m": 0.0 if data_status != "missing" else None,
            "data_status": data_status,
            "quality_notes": json.dumps({}),
            "source_max_trade_date": date(2024, 7, 22),
            "ingested_at": pd.Timestamp("2024-07-22"),
        }

    def _write_snapshot(self, rows: list[dict]) -> None:
        self.service._write_etf_aw_rebalance_snapshot(pd.DataFrame(rows))

    def _read_score_file(self, year: int, month: int) -> pd.DataFrame:
        path = (
            self.lakehouse_root
            / "derived"
            / "derived.etf_aw_regime_score"
            / str(year)
            / f"{month:02d}"
            / "part-00000.parquet"
        )
        return pd.read_parquet(path)


if __name__ == "__main__":
    unittest.main()

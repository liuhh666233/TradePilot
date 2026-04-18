"""Filesystem helpers for ETF all-weather stage one."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from tradepilot.config import ETF_AW_DATA_ROOT


def ensure_storage_layout(root: Path = ETF_AW_DATA_ROOT) -> dict[str, Path]:
    """Create and return the ETF all-weather data zone layout."""

    paths = {
        "root": root,
        "raw": root / "raw",
        "normalized": root / "normalized",
        "derived": root / "derived",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def build_trade_calendar_raw_path(
    *,
    exchange: str,
    start_date: str,
    end_date: str,
    raw_batch_id: int,
    root: Path = ETF_AW_DATA_ROOT,
) -> Path:
    """Return the canonical raw-path for one trade-calendar batch."""

    year = start_date[:4]
    directory = root / "raw" / "trade_calendar" / f"exchange={exchange}" / f"year={year}"
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"trade_calendar__tushare__{start_date}__{end_date}__{raw_batch_id}.json"
    return directory / filename


def write_frame_json_records(df: pd.DataFrame, path: Path) -> tuple[int, str]:
    """Persist a dataframe as JSON records and return row count and content hash."""

    serializable = df.copy()
    for column in serializable.columns:
        if pd.api.types.is_datetime64_any_dtype(serializable[column]):
            serializable[column] = serializable[column].dt.strftime("%Y-%m-%d")
    records = serializable.to_dict(orient="records")
    payload = json.dumps(records, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    path.write_text(payload, encoding="utf-8")
    content_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return len(records), content_hash


def write_json_payload(payload: dict, path: Path) -> tuple[int, str]:
    """Persist a JSON payload and return synthetic row count and content hash."""

    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    path.write_text(text, encoding="utf-8")
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    row_count = 0
    if isinstance(payload.get("daily"), list):
        row_count += len(payload["daily"])
    if isinstance(payload.get("adj"), list):
        row_count += len(payload["adj"])
    return row_count, content_hash


def build_sleeve_market_raw_path(
    *,
    sleeve_code: str,
    start_date: str,
    end_date: str,
    raw_batch_id: int,
    root: Path = ETF_AW_DATA_ROOT,
) -> Path:
    """Return the canonical raw-path for one sleeve market batch."""

    year = start_date[:4]
    directory = root / "raw" / "sleeve_daily_market" / f"sleeve_code={sleeve_code}" / f"year={year}"
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"sleeve_daily_market__tushare__{start_date}__{end_date}__{raw_batch_id}.json"
    return directory / filename


def build_benchmark_index_raw_path(
    *,
    index_code: str,
    start_date: str,
    end_date: str,
    raw_batch_id: int,
    root: Path = ETF_AW_DATA_ROOT,
) -> Path:
    """Return the canonical raw-path for one benchmark index batch."""

    year = start_date[:4]
    directory = root / "raw" / "benchmark_index_daily" / f"index_code={index_code}" / f"year={year}"
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"benchmark_index_daily__tushare__{start_date}__{end_date}__{raw_batch_id}.json"
    return directory / filename


def build_slow_macro_raw_path(
    *,
    dataset_code: str,
    start_month: str,
    end_month: str,
    raw_batch_id: int,
    root: Path = ETF_AW_DATA_ROOT,
) -> Path:
    """Return the canonical raw-path for one slow-macro batch."""

    year = start_month[:4]
    directory = root / "raw" / "slow_macro" / f"dataset_code={dataset_code}" / f"year={year}"
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"slow_macro__{dataset_code}__{start_month}__{end_month}__{raw_batch_id}.json"
    return directory / filename


def build_curve_raw_path(
    *,
    curve_code: str,
    start_date: str,
    end_date: str,
    raw_batch_id: int,
    root: Path = ETF_AW_DATA_ROOT,
) -> Path:
    """Return the canonical raw-path for one curve extraction batch."""

    year = start_date[:4]
    directory = root / "raw" / "curve" / f"curve_code={curve_code}" / f"year={year}"
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"curve__{curve_code}__{start_date}__{end_date}__{raw_batch_id}.json"
    return directory / filename


def build_daily_market_partition_dir(
    *,
    dataset_year: int,
    dataset_month: int,
    root: Path = ETF_AW_DATA_ROOT,
) -> Path:
    """Return the normalized partition directory for daily market facts."""

    directory = (
        root
        / "normalized"
        / "daily_market"
        / f"dataset_year={dataset_year:04d}"
        / f"dataset_month={dataset_month:02d}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def build_slow_field_partition_dir(
    *,
    field_name: str,
    dataset_year: int,
    root: Path = ETF_AW_DATA_ROOT,
) -> Path:
    """Return the normalized partition directory for slow fields."""

    directory = root / "normalized" / "slow_fields" / f"field_name={field_name}" / f"dataset_year={dataset_year:04d}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def build_curve_partition_dir(
    *,
    dataset_year: int,
    dataset_month: int,
    root: Path = ETF_AW_DATA_ROOT,
) -> Path:
    """Return the normalized partition directory for curve facts."""

    directory = root / "normalized" / "curve" / f"dataset_year={dataset_year:04d}" / f"dataset_month={dataset_month:02d}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def build_monthly_feature_snapshot_dir(*, rebalance_year: int, root: Path = ETF_AW_DATA_ROOT) -> Path:
    """Return the derived partition directory for monthly feature snapshots."""

    directory = root / "derived" / "monthly_feature_snapshot" / f"rebalance_year={rebalance_year:04d}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def build_monthly_regime_snapshot_dir(*, rebalance_year: int, root: Path = ETF_AW_DATA_ROOT) -> Path:
    """Return the derived partition directory for monthly regime snapshots."""

    directory = root / "derived" / "monthly_regime_snapshot" / f"rebalance_year={rebalance_year:04d}"
    directory.mkdir(parents=True, exist_ok=True)
    return directory

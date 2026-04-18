"""A-share market summary fetcher for The One's a-share-summary skill.

Fetches from akshare market APIs:
- Major indices (上证/深证/创业板/科创50/上证50/沪深300)
- Market breadth (up/down/flat/limit-up/limit-down counts)
- Industry sectors (top gainers/losers)
- Concept sectors (top gainers/losers)

Outputs a JSON file + formatted markdown summary to stdout.
"""

import json
import math
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import akshare as ak
import click
import pandas as pd

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"

TARGET_INDICES: list[tuple[str, str]] = [
    ("000001", "上证指数"),
    ("399001", "深证成指"),
    ("399006", "创业板指"),
    ("000688", "科创50"),
    ("000016", "上证50"),
    ("000300", "沪深300"),
]

DEFAULT_BREADTH = {
    "total": 0,
    "up": 0,
    "down": 0,
    "flat": 0,
    "limit_up": 0,
    "limit_up_20": 0,
    "limit_down": 0,
    "limit_down_20": 0,
    "skipped": 0,
}


# ---------------------------------------------------------------------------
# Parsing / normalization helpers
# ---------------------------------------------------------------------------


def _safe_float(value: object, default: float = 0.0) -> float:
    """Convert scalar value to float safely."""
    if value is None:
        return default
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "").replace("%", "")
        if cleaned in {"", "-", "--", "nan", "NaN", "None"}:
            return default
        value = cleaned
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def _safe_int(value: object, default: int = 0) -> int:
    """Convert scalar value to int safely."""
    return int(_safe_float(value, float(default)))


def _safe_str(value: object, default: str = "") -> str:
    """Convert scalar value to string safely."""
    if value is None:
        return default
    text = str(value).strip()
    if text in {"", "-", "--", "nan", "NaN", "None"}:
        return default
    return text


def _normalize_code(code: object) -> str:
    """Normalize security code into plain numeric string when possible."""
    value = _safe_str(code)
    if not value:
        return ""
    if "." in value:
        value = value.split(".")[-1]
    return value[-6:] if value[-6:].isdigit() else value


def _sort_records(data: list[dict], n: int, ascending: bool) -> list[dict]:
    """Sort by change_pct and truncate top n."""
    sorted_data = sorted(data, key=lambda x: x["change_pct"], reverse=not ascending)
    return sorted_data[:n]


def _clip(value: float, lower: float, upper: float) -> float:
    """Clip numeric value into a bounded range."""
    return max(lower, min(upper, value))


def _parse_watch_sector_names(raw: str) -> list[str]:
    """Parse watch sector option into normalized list."""
    names = [_safe_str(part) for part in raw.split(",")]
    return [name for name in names if name]


def _parse_watch_stocks(raw: str) -> list[dict]:
    """Parse watch stock option into list of code/name dicts."""
    records: list[dict] = []
    for part in raw.split(","):
        token = _safe_str(part)
        if not token:
            continue

        code_part = token
        name_part = ""
        if ":" in token:
            code_part, name_part = token.split(":", 1)

        code = _normalize_code(code_part)
        if not code:
            continue

        records.append(
            {
                "code": code,
                "name": _safe_str(name_part),
            }
        )
    return records


def _parse_watch_sectors_from_json(value: object) -> list[str]:
    """Parse watch sector list from JSON value."""
    if not isinstance(value, list):
        return []
    return [name for name in (_safe_str(item) for item in value) if name]


def _parse_watch_stocks_from_json(value: object) -> list[dict]:
    """Parse watch stock list from JSON value."""
    if not isinstance(value, list):
        return []

    records: list[dict] = []
    for item in value:
        if isinstance(item, dict):
            code = _normalize_code(item.get("code"))
            name = _safe_str(item.get("name"))
            if code:
                records.append({"code": code, "name": name})
            continue

        token = _safe_str(item)
        if not token:
            continue

        code = _normalize_code(token)
        if code:
            records.append({"code": code, "name": ""})

    return records


def load_watch_config(config_path: str) -> tuple[list[str], list[dict]]:
    """Load watch sectors and stocks from JSON config file."""
    if not config_path:
        return [], []

    path = Path(config_path).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()

    try:
        config = json.loads(path.read_text())
    except Exception as exc:
        print(f"[a-share] watch config load failed: {exc}", file=sys.stderr)
        return [], []

    if not isinstance(config, dict):
        print("[a-share] watch config should be a JSON object", file=sys.stderr)
        return [], []

    sectors = _parse_watch_sectors_from_json(config.get("watch_sectors"))
    stocks = _parse_watch_stocks_from_json(config.get("watch_stocks"))
    return sectors, stocks


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------


def fetch_indices() -> list[dict]:
    """Fetch major index data.

    Returns:
        List of dicts with keys: code, name, close, change_pct, change_val, volume, turnover.
    """

    def _default_item(code: str, name: str) -> dict:
        return {
            "code": code,
            "name": name,
            "close": 0.0,
            "change_pct": 0.0,
            "change_val": 0.0,
            "volume": 0.0,
            "turnover": 0.0,
        }

    em_df: pd.DataFrame | None = None
    sina_df: pd.DataFrame | None = None

    try:
        em_df = ak.stock_zh_index_spot_em()
    except Exception as exc:
        print(f"[a-share] indices em fetch failed: {exc}", file=sys.stderr)

    try:
        sina_df = ak.stock_zh_index_spot_sina()
    except Exception as exc:
        print(f"[a-share] indices sina fetch failed: {exc}", file=sys.stderr)

    if em_df is None and sina_df is None:
        return [_default_item(code, name) for code, name in TARGET_INDICES]

    em_code_map: dict[str, pd.Series] = {}
    em_name_map: dict[str, pd.Series] = {}
    if em_df is not None:
        em_code_map = {
            _normalize_code(row.get("代码")): row for _, row in em_df.iterrows()
        }
        em_name_map = {_safe_str(row.get("名称")): row for _, row in em_df.iterrows()}

    sina_code_map: dict[str, pd.Series] = {}
    sina_name_map: dict[str, pd.Series] = {}
    if sina_df is not None:
        sina_code_map = {
            _normalize_code(row.get("代码")): row for _, row in sina_df.iterrows()
        }
        sina_name_map = {
            _safe_str(row.get("名称")): row for _, row in sina_df.iterrows()
        }

    results: list[dict] = []
    for code, name in TARGET_INDICES:
        row = em_code_map.get(code)
        if row is None:
            row = em_name_map.get(name)
        if row is not None:
            results.append(
                {
                    "code": _normalize_code(row.get("代码")) or code,
                    "name": _safe_str(row.get("名称")) or name,
                    "close": _safe_float(row.get("最新价")),
                    "change_pct": _safe_float(row.get("涨跌幅")),
                    "change_val": _safe_float(row.get("涨跌额")),
                    "volume": _safe_float(row.get("成交量")),
                    "turnover": _safe_float(row.get("成交额")),
                }
            )
            continue

        row = sina_code_map.get(code)
        if row is None:
            row = sina_name_map.get(name)
        if row is not None:
            results.append(
                {
                    "code": _normalize_code(row.get("代码")) or code,
                    "name": _safe_str(row.get("名称")) or name,
                    "close": _safe_float(row.get("最新价")),
                    "change_pct": _safe_float(row.get("涨跌幅")),
                    "change_val": _safe_float(row.get("涨跌额")),
                    "volume": _safe_float(row.get("成交量")),
                    "turnover": _safe_float(row.get("成交额")),
                }
            )
            continue

        results.append(_default_item(code, name))

    return results


def _fetch_sector_dataframe(is_industry: bool) -> pd.DataFrame:
    """Fetch sector dataframe by category."""
    if is_industry:
        return ak.stock_board_industry_name_em()
    return ak.stock_board_concept_name_em()


def fetch_sectors(
    is_industry: bool,
    top_n: int = 20,
    ascending: bool = False,
) -> list[dict]:
    """Fetch sector performance.

    Args:
        is_industry: True for industry board, False for concept board.
        top_n: Number of sectors to return.
        ascending: If True, sort by worst performers.

    Returns:
        List of dicts with keys: code, name, change_pct, up_count, down_count, leader.
    """
    try:
        df = _fetch_sector_dataframe(is_industry=is_industry)
    except Exception as exc:
        label = "industry" if is_industry else "concept"
        print(f"[a-share] {label} sectors fetch failed: {exc}", file=sys.stderr)
        return []

    records: list[dict] = []
    for _, row in df.iterrows():
        records.append(
            {
                "code": _safe_str(row.get("板块代码")),
                "name": _safe_str(row.get("板块名称")),
                "change_pct": _safe_float(row.get("涨跌幅")),
                "up_count": _safe_int(row.get("上涨家数")),
                "down_count": _safe_int(row.get("下跌家数")),
                "leader": _safe_str(row.get("领涨股票")),
            }
        )

    return _sort_records(records, top_n, ascending)


def _fetch_all_stocks_snapshot() -> pd.DataFrame:
    """Fetch full A-share snapshot once."""
    return ak.stock_zh_a_spot_em()


def _parse_stock_changes(df: pd.DataFrame) -> tuple[list[dict], list[float], int]:
    """Normalize stock snapshot rows into stock list and change series."""
    stocks: list[dict] = []
    changes: list[float] = []
    skipped = 0

    for _, row in df.iterrows():
        change = _safe_float(row.get("涨跌幅"), default=float("nan"))
        if math.isnan(change):
            skipped += 1
            continue

        stock = {
            "code": _normalize_code(row.get("代码")),
            "name": _safe_str(row.get("名称")),
            "change_pct": change,
        }
        stocks.append(stock)
        changes.append(change)

    return stocks, changes, skipped


def _compute_market_breadth_from_changes(changes: list[float], skipped: int) -> dict:
    """Compute breadth statistics from normalized change list."""
    total = len(changes)
    up = sum(1 for c in changes if c > 0)
    down = sum(1 for c in changes if c < 0)
    flat = sum(1 for c in changes if c == 0)
    limit_up = sum(1 for c in changes if c >= 9.9)
    limit_up_20 = sum(1 for c in changes if c >= 19.9)
    limit_down = sum(1 for c in changes if c <= -9.9)
    limit_down_20 = sum(1 for c in changes if c <= -19.9)

    return {
        "total": total,
        "up": up,
        "down": down,
        "flat": flat,
        "limit_up": limit_up,
        "limit_up_20": limit_up_20,
        "limit_down": limit_down,
        "limit_down_20": limit_down_20,
        "skipped": skipped,
    }


def fetch_market_breadth() -> dict:
    """Fetch market-wide up/down/limit statistics from full A-share snapshot.

    Returns:
        Dict with keys: total, up, down, flat, limit_up, limit_up_20,
        limit_down, limit_down_20, skipped.
    """
    try:
        df = _fetch_all_stocks_snapshot()
    except Exception as exc:
        print(f"[a-share] breadth fetch failed: {exc}", file=sys.stderr)
        return DEFAULT_BREADTH.copy()

    _, changes, skipped = _parse_stock_changes(df)
    return _compute_market_breadth_from_changes(changes, skipped)


def fetch_top_stocks(top_n: int = 10, ascending: bool = False) -> list[dict]:
    """Fetch top gaining or losing individual stocks.

    Args:
        top_n: Number of stocks to return.
        ascending: If True, return worst performers.

    Returns:
        List of dicts with keys: code, name, change_pct.
    """
    try:
        df = _fetch_all_stocks_snapshot()
    except Exception as exc:
        side = "bottom" if ascending else "top"
        print(f"[a-share] {side} stocks fetch failed: {exc}", file=sys.stderr)
        return []

    stocks, _, _ = _parse_stock_changes(df)
    return _sort_records(stocks, top_n, ascending)


# ---------------------------------------------------------------------------
# 5-minute brief helpers
# ---------------------------------------------------------------------------


def compute_market_regime(indices: list[dict], breadth: dict) -> dict:
    """Compute market regime score and label for intraday brief."""
    hs300 = next((idx for idx in indices if idx.get("name") == "沪深300"), None)
    cyb = next((idx for idx in indices if idx.get("name") == "创业板指"), None)

    hs300_pct = _safe_float(hs300.get("change_pct") if hs300 else 0.0)
    cyb_pct = _safe_float(cyb.get("change_pct") if cyb else 0.0)
    index_component = ((hs300_pct + cyb_pct) / 2.0) * 20

    total = _safe_int(breadth.get("total"))
    up = _safe_int(breadth.get("up"))
    down = _safe_int(breadth.get("down"))
    limit_up = _safe_int(breadth.get("limit_up"))
    limit_down = _safe_int(breadth.get("limit_down"))

    breadth_component = ((up - down) / total * 100) if total else 0.0
    limit_component = ((limit_up - limit_down) / total * 1000) if total else 0.0

    score = _clip(
        index_component * 0.4 + breadth_component * 0.4 + limit_component * 0.2,
        -100.0,
        100.0,
    )

    if score >= 20:
        label = "risk_on"
    elif score <= -20:
        label = "risk_off"
    else:
        label = "neutral"

    return {
        "label": label,
        "score": round(score, 2),
        "drivers": {
            "hs300_change_pct": round(hs300_pct, 2),
            "cyb_change_pct": round(cyb_pct, 2),
            "up_down_diff": up - down,
            "limit_up_down_diff": limit_up - limit_down,
        },
    }


def _find_sector_match(concepts: list[dict], watch_name: str) -> dict | None:
    """Find watch sector by exact or fuzzy name match."""
    watch_lower = watch_name.lower()

    for item in concepts:
        name = _safe_str(item.get("name"))
        if name == watch_name:
            return item

    for item in concepts:
        name = _safe_str(item.get("name"))
        if watch_lower in name.lower() or name.lower() in watch_lower:
            return item

    return None


def extract_watch_sectors(concepts: list[dict], watch_names: list[str]) -> list[dict]:
    """Extract watchlist sector records and classify signal strength."""
    results: list[dict] = []

    for watch_name in watch_names:
        matched = _find_sector_match(concepts, watch_name)
        if matched is None:
            results.append(
                {
                    "name": watch_name,
                    "matched_name": "",
                    "change_pct": 0.0,
                    "up_count": 0,
                    "down_count": 0,
                    "strength": 0.0,
                    "status": "missing",
                }
            )
            continue

        up_count = _safe_int(matched.get("up_count"))
        down_count = _safe_int(matched.get("down_count"))
        total = up_count + down_count
        strength = up_count / total if total > 0 else 0.0
        change_pct = _safe_float(matched.get("change_pct"))

        status = "neutral"
        if change_pct >= 2.0 and strength >= 0.6:
            status = "strong"
        elif change_pct <= -2.0 and strength <= 0.4:
            status = "weak"

        results.append(
            {
                "name": watch_name,
                "matched_name": _safe_str(matched.get("name")) or watch_name,
                "change_pct": round(change_pct, 2),
                "up_count": up_count,
                "down_count": down_count,
                "strength": round(strength, 2),
                "status": status,
            }
        )

    return results


def extract_watch_stocks(
    snapshot_df: pd.DataFrame, watch_stocks: list[dict]
) -> list[dict]:
    """Extract watchlist stock records and classify signal status."""
    code_map: dict[str, pd.Series] = {
        _normalize_code(row.get("代码")): row for _, row in snapshot_df.iterrows()
    }

    results: list[dict] = []
    for watch in watch_stocks:
        code = _normalize_code(watch.get("code"))
        watch_name = _safe_str(watch.get("name"))
        row = code_map.get(code)

        if row is None:
            results.append(
                {
                    "code": code,
                    "name": watch_name,
                    "price": 0.0,
                    "change_pct": 0.0,
                    "change_val": 0.0,
                    "turnover_rate": 0.0,
                    "volume_ratio": 0.0,
                    "status": "missing",
                }
            )
            continue

        price = _safe_float(row.get("最新价"))
        change_pct = _safe_float(row.get("涨跌幅"))
        change_val = _safe_float(row.get("涨跌额"))
        turnover_rate = _safe_float(row.get("换手率"))
        volume_ratio = _safe_float(row.get("量比"))

        status = "watch"
        if change_pct >= 3.0:
            status = "breakout"
        elif change_pct <= -3.0:
            status = "breakdown"
        elif abs(change_pct) >= 1.5 and (turnover_rate >= 5.0 or volume_ratio >= 1.5):
            status = "active"

        results.append(
            {
                "code": code,
                "name": _safe_str(row.get("名称")) or watch_name,
                "price": round(price, 2),
                "change_pct": round(change_pct, 2),
                "change_val": round(change_val, 2),
                "turnover_rate": round(turnover_rate, 2),
                "volume_ratio": round(volume_ratio, 2),
                "status": status,
            }
        )

    return results


def build_5m_alerts(
    regime: dict, sector_watchlist: list[dict], stock_watchlist: list[dict]
) -> list[str]:
    """Build human-readable alerts from 5m regime and watchlist signals."""
    alerts: list[str] = []

    regime_label = _safe_str(regime.get("label"))
    regime_score = _safe_float(regime.get("score"))
    if regime_label == "risk_on":
        alerts.append(f"市场偏强（score={regime_score:+.1f}）")
    elif regime_label == "risk_off":
        alerts.append(f"市场偏弱（score={regime_score:+.1f}）")

    for sector in sector_watchlist:
        status = _safe_str(sector.get("status"))
        name = _safe_str(sector.get("matched_name")) or _safe_str(sector.get("name"))
        if status == "strong":
            alerts.append(
                f"板块走强：{name} ({_safe_float(sector.get('change_pct')):+.2f}%)"
            )
        elif status == "weak":
            alerts.append(
                f"板块走弱：{name} ({_safe_float(sector.get('change_pct')):+.2f}%)"
            )

    for stock in stock_watchlist:
        status = _safe_str(stock.get("status"))
        name = _safe_str(stock.get("name"))
        change_pct = _safe_float(stock.get("change_pct"))
        if status in {"breakout", "breakdown", "active"}:
            alerts.append(
                f"个股{status}：{name} {_safe_str(stock.get('code'))} ({change_pct:+.2f}%)"
            )

    return alerts


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def format_summary(
    indices: list[dict],
    breadth: dict,
    industry_top: list[dict],
    industry_bottom: list[dict],
    concept_top: list[dict],
    concept_bottom: list[dict],
    stocks_top: list[dict],
    stocks_bottom: list[dict],
    date_str: str,
) -> str:
    """Format all data into a markdown summary."""
    lines: list[str] = []
    a = lines.append

    a(f"# A股市场总结 — {date_str}")
    a("")
    a("## 大盘概况")
    a("")
    a("| 指数 | 收盘 | 涨跌幅 | 成交额 |")
    a("|------|------|--------|--------|")

    total_turnover = 0.0
    sh_sz = {"上证指数", "深证成指"}
    for idx in indices:
        close = f"{idx['close']:.2f}"
        pct = f"{idx['change_pct']:+.2f}%"
        tv = f"{idx['turnover'] / 1e8:,.0f}亿"
        a(f"| {idx['name']} | {close} | {pct} | {tv} |")
        if idx["name"] in sh_sz:
            total_turnover += idx["turnover"]

    a("")
    a(f"**沪深两市合计成交额：{total_turnover / 1e8:,.0f}亿元**")

    # Market breadth
    a("")
    a("## 市场情绪")
    a("")
    b = breadth
    up_pct = b["up"] / b["total"] * 100 if b["total"] else 0
    down_pct = b["down"] / b["total"] * 100 if b["total"] else 0
    ratio = f"1:{b['down'] / b['up']:.1f}" if b["up"] > 0 else "N/A"

    a("| 指标 | 数值 |")
    a("|------|------|")
    a(f"| 上涨 | {b['up']:,}家 ({up_pct:.1f}%) |")
    a(f"| 下跌 | {b['down']:,}家 ({down_pct:.1f}%) |")
    a(f"| 平盘 | {b['flat']}家 |")
    lu_detail = f"（含20cm涨停{b['limit_up_20']}家）" if b["limit_up_20"] else ""
    a(f"| 涨停 | {b['limit_up']}家{lu_detail} |")
    a(f"| 跌停 | {b['limit_down']}家 |")
    a(f"| 涨跌比 | {ratio} |")
    a("")

    # Industry sectors
    a("## 行业板块")
    a("")
    a("### 领涨行业")
    a("")
    a("| # | 行业 | 涨幅 | 涨/跌 | 领涨股 |")
    a("|---|------|------|-------|--------|")
    for i, sector in enumerate(industry_top, 1):
        a(
            f"| {i} | {sector['name']} | {sector['change_pct']:+.2f}% "
            f"| {sector['up_count']}/{sector['down_count']} | {sector['leader']} |"
        )

    a("")
    a("### 领跌行业")
    a("")
    a("| # | 行业 | 跌幅 | 涨/跌 | 领涨股 |")
    a("|---|------|------|-------|--------|")
    for i, sector in enumerate(industry_bottom, 1):
        a(
            f"| {i} | {sector['name']} | {sector['change_pct']:+.2f}% "
            f"| {sector['up_count']}/{sector['down_count']} | {sector['leader']} |"
        )

    # Concept sectors
    a("")
    a("## 概念板块")
    a("")
    a("### 领涨概念")
    a("")
    a("| # | 概念 | 涨幅 | 涨/跌 |")
    a("|---|------|------|-------|")
    for i, sector in enumerate(concept_top, 1):
        a(
            f"| {i} | {sector['name']} | {sector['change_pct']:+.2f}% "
            f"| {sector['up_count']}/{sector['down_count']} |"
        )

    a("")
    a("### 领跌概念")
    a("")
    a("| # | 概念 | 跌幅 | 涨/跌 |")
    a("|---|------|------|-------|")
    for i, sector in enumerate(concept_bottom, 1):
        a(
            f"| {i} | {sector['name']} | {sector['change_pct']:+.2f}% "
            f"| {sector['up_count']}/{sector['down_count']} |"
        )

    # Top/bottom stocks
    a("")
    a("## 个股涨跌幅 TOP10")
    a("")
    a("### 涨幅前10")
    a("")
    a("| # | 股票 | 代码 | 涨幅 |")
    a("|---|------|------|------|")
    for i, stock in enumerate(stocks_top, 1):
        a(f"| {i} | {stock['name']} | {stock['code']} | {stock['change_pct']:+.2f}% |")

    a("")
    a("### 跌幅前10")
    a("")
    a("| # | 股票 | 代码 | 跌幅 |")
    a("|---|------|------|------|")
    for i, stock in enumerate(stocks_bottom, 1):
        a(f"| {i} | {stock['name']} | {stock['code']} | {stock['change_pct']:+.2f}% |")

    a("")
    a("## 数据来源")
    a("- akshare（公开行情源聚合，含指数、行业板块、概念板块、个股涨跌统计）")
    a(f"- 数据时间：{date_str} 收盘")
    a("")

    return "\n".join(lines)


def format_5m_brief(
    timestamp_str: str,
    regime: dict,
    sector_watchlist: list[dict],
    stock_watchlist: list[dict],
    alerts: list[str],
) -> str:
    """Format intraday 5-minute briefing markdown."""
    lines: list[str] = []
    a = lines.append

    a(f"# A股 5分钟简报 — {timestamp_str}")
    a("")
    a("## 市场状态")
    a("")
    a(f"- 状态：**{_safe_str(regime.get('label')).upper()}**")
    a(f"- 分数：**{_safe_float(regime.get('score')):+.2f}**")

    drivers = regime.get("drivers", {})
    a(
        "- 驱动："
        f"沪深300 {_safe_float(drivers.get('hs300_change_pct')):+.2f}% / "
        f"创业板 {_safe_float(drivers.get('cyb_change_pct')):+.2f}% / "
        f"涨跌家差 {_safe_int(drivers.get('up_down_diff')):+d}"
    )

    a("")
    a("## 关注板块")
    a("")
    a("| 板块 | 匹配 | 涨跌幅 | 上涨/下跌 | 强度 | 状态 |")
    a("|------|------|--------|-----------|------|------|")
    for sector in sector_watchlist:
        matched = _safe_str(sector.get("matched_name")) or "-"
        a(
            f"| {_safe_str(sector.get('name'))} | {matched} | "
            f"{_safe_float(sector.get('change_pct')):+.2f}% | "
            f"{_safe_int(sector.get('up_count'))}/{_safe_int(sector.get('down_count'))} | "
            f"{_safe_float(sector.get('strength')):.2f} | {_safe_str(sector.get('status'))} |"
        )

    a("")
    a("## 关注个股")
    a("")
    a("| 股票 | 代码 | 现价 | 涨跌幅 | 涨跌额 | 换手率 | 量比 | 状态 |")
    a("|------|------|------|--------|--------|--------|------|------|")
    for stock in stock_watchlist:
        a(
            f"| {_safe_str(stock.get('name'))} | {_safe_str(stock.get('code'))} | "
            f"{_safe_float(stock.get('price')):.2f} | {_safe_float(stock.get('change_pct')):+.2f}% | "
            f"{_safe_float(stock.get('change_val')):+.2f} | {_safe_float(stock.get('turnover_rate')):.2f}% | "
            f"{_safe_float(stock.get('volume_ratio')):.2f} | {_safe_str(stock.get('status'))} |"
        )

    a("")
    a("## 告警")
    a("")
    if alerts:
        for alert in alerts:
            a(f"- {alert}")
    else:
        a("- 暂无显著告警")

    a("")
    a("## 数据来源")
    a("- akshare（公开行情源聚合）")
    a(f"- 数据时间：{timestamp_str}")
    a("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@click.command()
@click.option(
    "--industry-top", "n_ind_top", default=10, help="Top N gaining industries."
)
@click.option(
    "--industry-bottom", "n_ind_bot", default=10, help="Top N losing industries."
)
@click.option("--concept-top", "n_con_top", default=15, help="Top N gaining concepts.")
@click.option(
    "--concept-bottom", "n_con_bot", default=15, help="Top N losing concepts."
)
@click.option(
    "--json-only", is_flag=True, help="Output raw JSON to stdout, skip markdown."
)
@click.option(
    "--skip-breadth",
    is_flag=True,
    help="Skip market breadth (faster, fewer API calls).",
)
@click.option(
    "--mode",
    type=click.Choice(["daily", "5m"]),
    default="daily",
    show_default=True,
    help="Briefing mode: daily full summary or 5-minute concise brief.",
)
@click.option(
    "--watch-config",
    default="",
    help="JSON file for 5m watchlist; keys: watch_sectors, watch_stocks.",
)
@click.option(
    "--watch-sectors",
    default="",
    help="Comma-separated sector watchlist for 5m mode; overrides config when set.",
)
@click.option(
    "--watch-stocks",
    default="",
    help="Comma-separated stock watchlist for 5m mode; format code:name, overrides config when set.",
)
def main(
    n_ind_top: int,
    n_ind_bot: int,
    n_con_top: int,
    n_con_bot: int,
    json_only: bool,
    skip_breadth: bool,
    mode: str,
    watch_config: str,
    watch_sectors: str,
    watch_stocks: str,
) -> None:
    """Fetch A-share market summary from akshare."""
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    now_str = now.strftime("%Y-%m-%d %H:%M")
    print(
        f"[a-share] fetching market data for {today} (mode={mode})...", file=sys.stderr
    )

    if mode == "5m":
        config_sectors, config_stocks = load_watch_config(watch_config)
        sector_names = (
            _parse_watch_sector_names(watch_sectors)
            if watch_sectors
            else config_sectors
        )
        stock_watch = (
            _parse_watch_stocks(watch_stocks) if watch_stocks else config_stocks
        )

        if not sector_names:
            print(
                "[a-share] 5m mode requires watch sectors (use --watch-config or --watch-sectors)",
                file=sys.stderr,
            )
            sector_names = []

        if not stock_watch:
            print(
                "[a-share] 5m mode requires watch stocks (use --watch-config or --watch-stocks)",
                file=sys.stderr,
            )
            stock_watch = []

        tasks = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            tasks["indices"] = pool.submit(fetch_indices)
            tasks["concept_all"] = pool.submit(fetch_sectors, False, 1000, False)
            tasks["snapshot"] = pool.submit(_fetch_all_stocks_snapshot)

        results: dict[str, object] = {}
        for key, future in tasks.items():
            try:
                results[key] = future.result()
            except Exception as exc:
                print(f"[a-share] task {key} failed: {exc}", file=sys.stderr)
                if key == "snapshot":
                    results[key] = pd.DataFrame()
                else:
                    results[key] = []

        snapshot_df = results.get("snapshot", pd.DataFrame())
        stocks, changes, skipped = _parse_stock_changes(snapshot_df)
        breadth = (
            DEFAULT_BREADTH.copy()
            if skip_breadth
            else _compute_market_breadth_from_changes(changes, skipped)
        )

        regime = compute_market_regime(results.get("indices", []), breadth)
        sector_watchlist = extract_watch_sectors(
            results.get("concept_all", []), sector_names
        )
        stock_watchlist = extract_watch_stocks(snapshot_df, stock_watch)
        alerts = build_5m_alerts(regime, sector_watchlist, stock_watchlist)

        raw = {
            "mode": "5m",
            "date": today,
            "timestamp": now_str,
            "indices": results.get("indices", []),
            "breadth": breadth,
            "industry_top": [],
            "industry_bottom": [],
            "concept_top": [],
            "concept_bottom": [],
            "stocks_top": _sort_records(stocks, 10, False),
            "stocks_bottom": _sort_records(stocks, 10, True),
            "regime": regime,
            "sector_watchlist": sector_watchlist,
            "stock_watchlist": stock_watchlist,
            "alerts": alerts,
        }

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        json_path = OUTPUT_DIR / f"a-share-{today}-5m.json"
        json_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
        print(f"[a-share] saved → {json_path}", file=sys.stderr)

        if json_only:
            print(json.dumps(raw, ensure_ascii=False, indent=2))
            return

        summary = format_5m_brief(
            timestamp_str=now_str,
            regime=regime,
            sector_watchlist=sector_watchlist,
            stock_watchlist=stock_watchlist,
            alerts=alerts,
        )

        print("")
        print(summary)
        return

    tasks = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        tasks["indices"] = pool.submit(fetch_indices)
        tasks["ind_top"] = pool.submit(fetch_sectors, True, n_ind_top, False)
        tasks["ind_bot"] = pool.submit(fetch_sectors, True, n_ind_bot, True)
        tasks["con_top"] = pool.submit(fetch_sectors, False, n_con_top, False)
        tasks["con_bot"] = pool.submit(fetch_sectors, False, n_con_bot, True)
        tasks["stk_top"] = pool.submit(fetch_top_stocks, 10, False)
        tasks["stk_bot"] = pool.submit(fetch_top_stocks, 10, True)
        if not skip_breadth:
            tasks["breadth"] = pool.submit(fetch_market_breadth)

    results = {}
    for key, future in tasks.items():
        try:
            results[key] = future.result()
        except Exception as exc:
            print(f"[a-share] task {key} failed: {exc}", file=sys.stderr)
            if key == "indices":
                results[key] = []
            elif key == "breadth":
                results[key] = DEFAULT_BREADTH.copy()
            else:
                results[key] = []

    breadth = results.get("breadth", DEFAULT_BREADTH.copy())

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    raw = {
        "mode": "daily",
        "date": today,
        "timestamp": now_str,
        "indices": results.get("indices", []),
        "breadth": breadth,
        "industry_top": results.get("ind_top", []),
        "industry_bottom": results.get("ind_bot", []),
        "concept_top": results.get("con_top", []),
        "concept_bottom": results.get("con_bot", []),
        "stocks_top": results.get("stk_top", []),
        "stocks_bottom": results.get("stk_bot", []),
    }
    json_path = OUTPUT_DIR / f"a-share-{today}.json"
    json_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2))
    print(f"[a-share] saved → {json_path}", file=sys.stderr)

    if json_only:
        print(json.dumps(raw, ensure_ascii=False, indent=2))
        return

    summary = format_summary(
        indices=results.get("indices", []),
        breadth=breadth,
        industry_top=results.get("ind_top", []),
        industry_bottom=results.get("ind_bot", []),
        concept_top=results.get("con_top", []),
        concept_bottom=results.get("con_bot", []),
        stocks_top=results.get("stk_top", []),
        stocks_bottom=results.get("stk_bot", []),
        date_str=today,
    )

    print("")
    print(summary)


if __name__ == "__main__":
    main()

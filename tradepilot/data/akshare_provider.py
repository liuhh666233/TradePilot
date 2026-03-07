"""AKShare-backed structured market data provider.

Fetches real A-share market data from East Money via the akshare library.
Each method normalizes Chinese column names from akshare into the English schema
used by our DuckDB tables. When an API call fails (network, rate-limit, etc.),
the provider logs the error and falls back to MockProvider.
"""

from __future__ import annotations

import time

import akshare as ak
import pandas as pd
from loguru import logger

from tradepilot.data.mock_provider import MockProvider
from tradepilot.data.provider import DataProvider

# Column rename maps: akshare Chinese → our English schema
_STOCK_HIST_RENAME = {
    "日期": "date",
    "股票代码": "stock_code",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "换手率": "turnover",
}

_INDEX_HIST_RENAME = {
    "日期": "date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
}

_NORTHBOUND_RENAME = {
    "日期": "date",
    "当日成交净买额": "net_buy",
}

_SECTOR_CONS_RENAME = {
    "代码": "stock_code",
    "名称": "stock_name",
}

# Default inter-request pause to avoid rate limits (seconds).
_REQUEST_PAUSE = 0.3


def _fmt_date(date_str: str) -> str:
    """Normalise date strings like '2025-01-01' → '20250101'."""
    return date_str.replace("-", "")


class AKShareProvider(DataProvider):
    """Structured data provider backed by the akshare library.

    Methods that can be served by akshare call the corresponding API and rename
    columns to match the DuckDB schema.  If a call fails the provider falls
    back to ``MockProvider`` so that the rest of the pipeline keeps working.
    """

    def __init__(self) -> None:
        self._fallback = MockProvider()
        logger.info("AKShareProvider initialised")

    # ------------------------------------------------------------------
    # Stock OHLCV (daily / weekly / monthly)
    # ------------------------------------------------------------------

    def _fetch_stock_hist(
        self, stock_code: str, period: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Shared helper for daily/weekly/monthly stock history."""
        try:
            time.sleep(_REQUEST_PAUSE)
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period=period,
                start_date=_fmt_date(start_date),
                end_date=_fmt_date(end_date),
                adjust="qfq",
            )
            df = df.rename(columns=_STOCK_HIST_RENAME)
            # Keep only columns our schema needs
            keep = ["date", "stock_code", "open", "high", "low", "close", "volume", "amount", "turnover"]
            df = df[[c for c in keep if c in df.columns]]
            if "stock_code" not in df.columns:
                df["stock_code"] = stock_code
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            logger.debug("akshare: fetched {} {} rows for {}", period, len(df), stock_code)
            return df
        except Exception:
            logger.exception("akshare: stock_zh_a_hist({}, {}) failed, using fallback", stock_code, period)
            fallback_fn = {
                "daily": self._fallback.get_stock_daily,
                "weekly": self._fallback.get_stock_weekly,
                "monthly": self._fallback.get_stock_monthly,
            }[period]
            return fallback_fn(stock_code, start_date, end_date)

    def get_stock_daily(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return daily OHLCV data for a stock."""
        return self._fetch_stock_hist(stock_code, "daily", start_date, end_date)

    def get_stock_weekly(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return weekly OHLCV data for a stock."""
        return self._fetch_stock_hist(stock_code, "weekly", start_date, end_date)

    def get_stock_monthly(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return monthly OHLCV data for a stock."""
        return self._fetch_stock_hist(stock_code, "monthly", start_date, end_date)

    # ------------------------------------------------------------------
    # Index daily
    # ------------------------------------------------------------------

    def get_index_daily(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return daily OHLCV data for an index."""
        try:
            time.sleep(_REQUEST_PAUSE)
            df = ak.index_zh_a_hist(
                symbol=index_code,
                period="daily",
                start_date=_fmt_date(start_date),
                end_date=_fmt_date(end_date),
            )
            df = df.rename(columns=_INDEX_HIST_RENAME)
            keep = ["date", "open", "high", "low", "close", "volume", "amount"]
            df = df[[c for c in keep if c in df.columns]]
            df["index_code"] = index_code
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            logger.debug("akshare: fetched index daily {} rows for {}", len(df), index_code)
            return df
        except Exception:
            logger.exception("akshare: index_zh_a_hist({}) failed, using fallback", index_code)
            return self._fallback.get_index_daily(index_code, start_date, end_date)

    # ------------------------------------------------------------------
    # ETF flow
    # ------------------------------------------------------------------

    def get_etf_flow(self, etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return ETF fund flow data.

        Uses individual stock fund flow API treating the ETF as a tradable
        instrument.  Maps ``主力净流入-净额`` to ``net_inflow``.
        """
        try:
            time.sleep(_REQUEST_PAUSE)
            # Determine market prefix for akshare
            market = "sh" if etf_code.startswith(("5", "6")) else "sz"
            df = ak.stock_individual_fund_flow(stock=etf_code, market=market)
            df = df.rename(columns={
                "日期": "date",
                "主力净流入-净额": "net_inflow",
                "收盘价": "close",
            })
            df["etf_code"] = etf_code
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            # Filter to date range
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df["date"] >= start) & (df["date"] <= end)]
            # volume is not directly available; estimate from amount columns or set 0
            if "volume" not in df.columns:
                df["volume"] = 0
            keep = ["date", "etf_code", "net_inflow", "volume"]
            df = df[[c for c in keep if c in df.columns]]
            logger.debug("akshare: fetched etf flow {} rows for {}", len(df), etf_code)
            return df
        except Exception:
            logger.exception("akshare: etf flow({}) failed, using fallback", etf_code)
            return self._fallback.get_etf_flow(etf_code, start_date, end_date)

    # ------------------------------------------------------------------
    # Margin data
    # ------------------------------------------------------------------

    def get_margin_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return margin trading data.

        The SSE margin detail API accepts a single date per call, which is too
        slow for large ranges.  Falls back to MockProvider for now; a future
        iteration can cache daily snapshots incrementally.
        """
        logger.debug("akshare: margin_data uses fallback (single-date API not practical for ranges)")
        return self._fallback.get_margin_data(start_date, end_date)

    # ------------------------------------------------------------------
    # Northbound flow
    # ------------------------------------------------------------------

    def get_northbound_flow(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return northbound capital flow data."""
        try:
            time.sleep(_REQUEST_PAUSE)
            df = ak.stock_hsgt_hist_em(symbol="北向资金")
            df = df.rename(columns=_NORTHBOUND_RENAME)
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            df = df[(df["date"] >= start) & (df["date"] <= end)]
            # API only provides net buy; set buy/sell to 0 as placeholders
            if "buy_amount" not in df.columns:
                df["buy_amount"] = 0.0
            if "sell_amount" not in df.columns:
                df["sell_amount"] = 0.0
            keep = ["date", "net_buy", "buy_amount", "sell_amount"]
            df = df[[c for c in keep if c in df.columns]]
            logger.debug("akshare: fetched northbound flow {} rows", len(df))
            return df
        except Exception:
            logger.exception("akshare: northbound flow failed, using fallback")
            return self._fallback.get_northbound_flow(start_date, end_date)

    # ------------------------------------------------------------------
    # Stock valuation
    # ------------------------------------------------------------------

    def get_stock_valuation(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return valuation data for a stock.

        Fetches PE-TTM, PB, and market cap from Baidu Finance via akshare.
        Each indicator requires a separate API call.
        """
        try:
            indicators = {
                "pe_ttm": "市盈率(TTM)",
                "pb": "市净率",
                "market_cap": "总市值",
            }
            merged: pd.DataFrame | None = None
            for col_name, indicator in indicators.items():
                time.sleep(_REQUEST_PAUSE)
                df = ak.stock_zh_valuation_baidu(
                    symbol=stock_code, indicator=indicator, period="全部"
                )
                df.columns = ["date", col_name]
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                if merged is None:
                    merged = df
                else:
                    merged = merged.merge(df, on="date", how="outer")
            if merged is None or merged.empty:
                raise ValueError("no valuation data returned")
            merged["stock_code"] = stock_code
            # ps not available from Baidu, fill with NaN
            merged["ps"] = float("nan")
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            merged = merged[(merged["date"] >= start) & (merged["date"] <= end)]
            keep = ["stock_code", "date", "pe_ttm", "pb", "ps", "market_cap"]
            merged = merged[[c for c in keep if c in merged.columns]]
            logger.debug("akshare: fetched valuation {} rows for {}", len(merged), stock_code)
            return merged
        except Exception:
            logger.exception("akshare: valuation({}) failed, using fallback", stock_code)
            return self._fallback.get_stock_valuation(stock_code, start_date, end_date)

    # ------------------------------------------------------------------
    # Sector data
    # ------------------------------------------------------------------

    def get_sector_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return aggregated sector metrics.

        The East Money industry board API provides a current-day snapshot with
        change percentages but no historical PE/PB.  Falls back to MockProvider
        for historical multi-day ranges.
        """
        logger.debug("akshare: sector_data uses fallback (no historical avg_pe/avg_pb API)")
        return self._fallback.get_sector_data(start_date, end_date)

    # ------------------------------------------------------------------
    # Sector ↔ stock mapping
    # ------------------------------------------------------------------

    def get_sector_stocks(self, sector: str, as_of_date: str | None = None) -> pd.DataFrame:
        """Return stock members for a sector (industry board)."""
        try:
            time.sleep(_REQUEST_PAUSE)
            df = ak.stock_board_industry_cons_em(symbol=sector)
            df = df.rename(columns=_SECTOR_CONS_RENAME)
            result = pd.DataFrame({
                "sector": sector,
                "stock_code": df["stock_code"],
                "stock_name": df["stock_name"],
                "as_of_date": as_of_date,
            })
            logger.debug("akshare: fetched sector stocks {} rows for {}", len(result), sector)
            return result
        except Exception:
            logger.exception("akshare: sector_stocks({}) failed, using fallback", sector)
            return self._fallback.get_sector_stocks(sector, as_of_date)

    def get_stock_sector(self, stock_code: str, as_of_date: str | None = None) -> pd.DataFrame:
        """Return sector mappings for a stock.

        Iterates the industry board list to find boards containing the stock.
        This is an expensive operation; cache results in DuckDB for repeated use.
        """
        try:
            time.sleep(_REQUEST_PAUSE)
            boards = ak.stock_board_industry_name_em()
            matches: list[str] = []
            for _, row in boards.iterrows():
                sector_name = row.get("板块名称", "")
                if not sector_name:
                    continue
                time.sleep(_REQUEST_PAUSE)
                try:
                    members = ak.stock_board_industry_cons_em(symbol=sector_name)
                    if stock_code in members["代码"].values:
                        matches.append(sector_name)
                except Exception:
                    continue
                # Stop early after finding first match to limit API calls
                if matches:
                    break
            if not matches:
                raise ValueError(f"no sector found for {stock_code}")
            result = pd.DataFrame(
                [{"stock_code": stock_code, "sector": s, "as_of_date": as_of_date} for s in matches]
            )
            logger.debug("akshare: found {} sectors for {}", len(matches), stock_code)
            return result
        except Exception:
            logger.exception("akshare: stock_sector({}) failed, using fallback", stock_code)
            return self._fallback.get_stock_sector(stock_code, as_of_date)

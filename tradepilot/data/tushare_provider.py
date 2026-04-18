"""Tushare-backed structured market data provider."""

from __future__ import annotations

import pandas as pd
from loguru import logger

from tradepilot.data.provider import DataProvider, sanitize_for_json
from tradepilot.data.tushare_client import TushareClient


class TushareProvider(DataProvider):
    """Structured data provider backed by Tushare.

    This provider prioritizes the subset of methods required by the current
    workflow-first product path. Methods that are not yet available through the
    current Tushare client return empty DataFrames instead of attempting to use
    unstable live fallbacks.
    """

    def __init__(self) -> None:
        self._client = TushareClient()
        logger.info("TushareProvider initialised")

    def get_stock_catalog(self) -> pd.DataFrame:
        return sanitize_for_json(self._client.get_stock_catalog())

    def get_index_catalog(self) -> pd.DataFrame:
        return sanitize_for_json(self._client.get_index_catalog())

    def get_stock_daily(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return sanitize_for_json(
            self._client.get_stock_daily(stock_code, start_date, end_date)
        )

    def get_stock_weekly(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return sanitize_for_json(
            self._client.get_stock_weekly(stock_code, start_date, end_date)
        )

    def get_stock_monthly(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return sanitize_for_json(
            self._client.get_stock_monthly(stock_code, start_date, end_date)
        )

    def get_index_daily(
        self, index_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return sanitize_for_json(
            self._client.get_index_daily(index_code, start_date, end_date)
        )

    def get_etf_flow(
        self, etf_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        logger.warning("tushare provider: etf flow not implemented for {}", etf_code)
        return pd.DataFrame(
            {
                "date": pd.Series(dtype="datetime64[ns]"),
                "etf_code": pd.Series(dtype="object"),
                "net_inflow": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="float64"),
            }
        )

    def get_margin_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        return sanitize_for_json(self._client.get_margin_data(start_date, end_date))

    def get_northbound_flow(self, start_date: str, end_date: str) -> pd.DataFrame:
        return sanitize_for_json(self._client.get_northbound_flow(start_date, end_date))

    def get_stock_valuation(
        self, stock_code: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        return sanitize_for_json(
            self._client.get_stock_valuation(stock_code, start_date, end_date)
        )

    def get_sector_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        logger.warning("tushare provider: sector data not implemented")
        return pd.DataFrame(
            {
                "sector": pd.Series(dtype="object"),
                "date": pd.Series(dtype="datetime64[ns]"),
                "avg_pe": pd.Series(dtype="float64"),
                "avg_pb": pd.Series(dtype="float64"),
                "change_1d": pd.Series(dtype="float64"),
                "change_5d": pd.Series(dtype="float64"),
                "change_20d": pd.Series(dtype="float64"),
                "change_60d": pd.Series(dtype="float64"),
            }
        )

    def get_sector_stocks(
        self, sector: str, as_of_date: str | None = None
    ) -> pd.DataFrame:
        logger.warning("tushare provider: sector stocks not implemented for {}", sector)
        return pd.DataFrame(
            {
                "stock_code": pd.Series(dtype="object"),
                "stock_name": pd.Series(dtype="object"),
            }
        )

    def get_stock_sector(
        self, stock_code: str, as_of_date: str | None = None
    ) -> pd.DataFrame:
        logger.warning(
            "tushare provider: stock sector not implemented for {}", stock_code
        )
        return pd.DataFrame(
            {
                "stock_code": pd.Series(dtype="object"),
                "sector": pd.Series(dtype="object"),
                "as_of_date": pd.Series(dtype="datetime64[ns]"),
            }
        )

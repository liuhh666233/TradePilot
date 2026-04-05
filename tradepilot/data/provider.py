"""Structured market data provider interfaces."""

from abc import ABC, abstractmethod
import math

import pandas as pd


def sanitize_for_json(df: pd.DataFrame) -> pd.DataFrame:
    sanitized = df.copy()
    for column in sanitized.columns:
        series = sanitized[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            continue
        sanitized[column] = series.map(
            lambda value: None
            if pd.isna(value) or (isinstance(value, float) and (math.isnan(value) or math.isinf(value)))
            else value
        )
    return sanitized


class DataProvider(ABC):
    """Contract for structured market data sources."""

    @abstractmethod
    def get_stock_catalog(self) -> pd.DataFrame:
        """Return available stock codes and names."""

    @abstractmethod
    def get_index_catalog(self) -> pd.DataFrame:
        """Return available index codes and names."""

    @abstractmethod
    def get_stock_daily(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return daily OHLCV data for a stock."""

    @abstractmethod
    def get_stock_weekly(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return weekly OHLCV data for a stock."""

    @abstractmethod
    def get_stock_monthly(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return monthly OHLCV data for a stock."""

    @abstractmethod
    def get_index_daily(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return daily OHLCV data for an index."""

    @abstractmethod
    def get_etf_flow(self, etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return ETF flow data."""

    @abstractmethod
    def get_margin_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return margin trading data."""

    @abstractmethod
    def get_northbound_flow(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return northbound capital flow data."""

    @abstractmethod
    def get_stock_valuation(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Return valuation data for a stock."""

    @abstractmethod
    def get_sector_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return aggregated sector metrics."""

    @abstractmethod
    def get_sector_stocks(self, sector: str, as_of_date: str | None = None) -> pd.DataFrame:
        """Return stock members for a sector."""

    @abstractmethod
    def get_stock_sector(self, stock_code: str, as_of_date: str | None = None) -> pd.DataFrame:
        """Return sector mappings for a stock."""

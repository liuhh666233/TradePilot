from abc import ABC, abstractmethod
import pandas as pd


class DataProvider(ABC):
    @abstractmethod
    def get_stock_daily(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        ...

    @abstractmethod
    def get_index_daily(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        ...

    @abstractmethod
    def get_etf_flow(self, etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        ...

    @abstractmethod
    def get_margin_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        ...

    @abstractmethod
    def get_northbound_flow(self, start_date: str, end_date: str) -> pd.DataFrame:
        ...

    @abstractmethod
    def get_stock_valuation(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        ...

    @abstractmethod
    def get_sector_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        ...

import numpy as np
import pandas as pd
from tradepilot.data.provider import DataProvider

_RNG = np.random.default_rng(42)

MOCK_STOCKS = {
    "600519": "贵州茅台", "000858": "五粮液", "601318": "中国平安",
    "300750": "宁德时代", "688111": "金山办公", "603501": "韦尔股份",
    "002415": "海康威视", "300033": "同花顺", "600570": "恒生电子",
    "300244": "迪安诊断",
}

MOCK_INDICES = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指", "000688": "科创50"}

MOCK_ETFS = ["510050", "510300", "510500", "512100"]

MOCK_SECTORS = ["AI应用", "金融科技", "光伏", "半导体", "消费", "医药", "新能源", "网络安全", "数据要素", "白酒"]


def _gen_ohlcv(dates: pd.DatetimeIndex, base_price: float) -> pd.DataFrame:
    n = len(dates)
    returns = _RNG.normal(0.0005, 0.02, n)
    close = base_price * np.cumprod(1 + returns)
    high = close * (1 + _RNG.uniform(0, 0.03, n))
    low = close * (1 - _RNG.uniform(0, 0.03, n))
    open_ = low + (high - low) * _RNG.uniform(0.2, 0.8, n)
    volume = _RNG.integers(1_000_000, 50_000_000, n)
    amount = close * volume
    return pd.DataFrame({
        "date": dates, "open": open_, "high": high, "low": low, "close": close,
        "volume": volume, "amount": amount,
    })


class MockProvider(DataProvider):
    def get_stock_daily(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        dates = pd.bdate_range(start_date, end_date)
        base = _RNG.uniform(10, 200)
        df = _gen_ohlcv(dates, base)
        df["stock_code"] = stock_code
        df["turnover"] = _RNG.uniform(0.5, 10.0, len(dates))
        return df

    def get_index_daily(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        dates = pd.bdate_range(start_date, end_date)
        base = {"000001": 3200, "399001": 10500, "399006": 2200, "000688": 1000}.get(index_code, 3000)
        df = _gen_ohlcv(dates, base)
        df["index_code"] = index_code
        return df

    def get_etf_flow(self, etf_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        dates = pd.bdate_range(start_date, end_date)
        n = len(dates)
        return pd.DataFrame({
            "date": dates, "etf_code": etf_code,
            "net_inflow": _RNG.normal(0, 5e8, n),
            "volume": _RNG.integers(1e7, 1e9, n),
        })

    def get_margin_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        dates = pd.bdate_range(start_date, end_date)
        rows = []
        for code in list(MOCK_STOCKS)[:5]:
            base = _RNG.uniform(1e9, 5e9)
            balance = base + np.cumsum(_RNG.normal(0, 1e7, len(dates)))
            for i, d in enumerate(dates):
                rows.append({"date": d, "stock_code": code, "margin_balance": balance[i], "margin_buy": _RNG.uniform(0, 1e8)})
        return pd.DataFrame(rows)

    def get_northbound_flow(self, start_date: str, end_date: str) -> pd.DataFrame:
        dates = pd.bdate_range(start_date, end_date)
        n = len(dates)
        buy = _RNG.uniform(50e8, 200e8, n)
        sell = _RNG.uniform(50e8, 200e8, n)
        return pd.DataFrame({"date": dates, "net_buy": buy - sell, "buy_amount": buy, "sell_amount": sell})

    def get_stock_valuation(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        dates = pd.bdate_range(start_date, end_date)
        n = len(dates)
        return pd.DataFrame({
            "date": dates, "stock_code": stock_code,
            "pe_ttm": _RNG.uniform(10, 80, n), "pb": _RNG.uniform(1, 15, n),
            "ps": _RNG.uniform(2, 30, n), "market_cap": _RNG.uniform(1e10, 1e12, n),
        })

    def get_sector_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        dates = pd.bdate_range(start_date, end_date)
        rows = []
        for sector in MOCK_SECTORS:
            for d in dates:
                rows.append({
                    "date": d, "sector": sector,
                    "avg_pe": _RNG.uniform(10, 60), "avg_pb": _RNG.uniform(1, 8),
                    "change_1d": _RNG.normal(0, 2), "change_5d": _RNG.normal(0, 4),
                    "change_20d": _RNG.normal(0, 8), "change_60d": _RNG.normal(0, 15),
                })
        return pd.DataFrame(rows)

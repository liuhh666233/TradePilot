from fastapi import APIRouter, Query
from tradepilot.data.mock_provider import MockProvider, MOCK_STOCKS, MOCK_INDICES, MOCK_ETFS

router = APIRouter()
_provider = MockProvider()


@router.get("/stocks")
def list_stocks():
    return [{"code": k, "name": v} for k, v in MOCK_STOCKS.items()]


@router.get("/stock_daily")
def stock_daily(stock_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _provider.get_stock_daily(stock_code, start_date, end_date)
    return df.to_dict(orient="records")


@router.get("/indices")
def list_indices():
    return [{"code": k, "name": v} for k, v in MOCK_INDICES.items()]


@router.get("/index_daily")
def index_daily(index_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _provider.get_index_daily(index_code, start_date, end_date)
    return df.to_dict(orient="records")


@router.get("/etf_flow")
def etf_flow(etf_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _provider.get_etf_flow(etf_code, start_date, end_date)
    return df.to_dict(orient="records")


@router.get("/northbound")
def northbound(start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _provider.get_northbound_flow(start_date, end_date)
    return df.to_dict(orient="records")


@router.get("/margin")
def margin(start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _provider.get_margin_data(start_date, end_date)
    return df.to_dict(orient="records")


@router.get("/valuation")
def valuation(stock_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _provider.get_stock_valuation(stock_code, start_date, end_date)
    return df.to_dict(orient="records")


@router.get("/sectors")
def sectors(start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _provider.get_sector_data(start_date, end_date)
    return df.to_dict(orient="records")

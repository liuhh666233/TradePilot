from fastapi import APIRouter
from tradepilot.data import get_provider
from tradepilot.data.provider import sanitize_for_json

router = APIRouter()


def _get_provider():
    return get_provider()


def _records(df):
    return sanitize_for_json(df).to_dict(orient="records")


@router.get("/stocks")
def list_stocks():
    return _records(_get_provider().get_stock_catalog())


@router.get("/stock_daily")
def stock_daily(
    stock_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"
):
    df = _get_provider().get_stock_daily(stock_code, start_date, end_date)
    return _records(df)


@router.get("/indices")
def list_indices():
    return _records(_get_provider().get_index_catalog())


@router.get("/index_daily")
def index_daily(
    index_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"
):
    df = _get_provider().get_index_daily(index_code, start_date, end_date)
    return _records(df)


@router.get("/etf_flow")
def etf_flow(
    etf_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"
):
    df = _get_provider().get_etf_flow(etf_code, start_date, end_date)
    return _records(df)


@router.get("/northbound")
def northbound(start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _get_provider().get_northbound_flow(start_date, end_date)
    return _records(df)


@router.get("/margin")
def margin(start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _get_provider().get_margin_data(start_date, end_date)
    return _records(df)


@router.get("/valuation")
def valuation(
    stock_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"
):
    df = _get_provider().get_stock_valuation(stock_code, start_date, end_date)
    return _records(df)


@router.get("/sectors")
def sectors(start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    df = _get_provider().get_sector_data(start_date, end_date)
    return _records(df)

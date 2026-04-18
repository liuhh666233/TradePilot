from fastapi import APIRouter

from tradepilot.analysis.sector_rotation import analyze_sectors
from tradepilot.analysis.technical import analyze_stock
from tradepilot.analysis.valuation import analyze_valuation
from tradepilot.data import get_provider

router = APIRouter()


def _get_provider():
    return get_provider()


@router.get("/technical")
def technical(
    stock_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"
):
    kline = _get_provider().get_stock_daily(stock_code, start_date, end_date)
    return analyze_stock(kline)


@router.get("/valuation")
def valuation(
    stock_code: str, start_date: str = "2024-01-01", end_date: str = "2025-12-31"
):
    val_df = _get_provider().get_stock_valuation(stock_code, start_date, end_date)
    kline = _get_provider().get_stock_daily(stock_code, start_date, end_date)
    return analyze_valuation(val_df, kline)


@router.get("/sector_rotation")
def sector_rotation(start_date: str = "2024-01-01", end_date: str = "2025-12-31"):
    sector_df = _get_provider().get_sector_data(start_date, end_date)
    return analyze_sectors(sector_df)

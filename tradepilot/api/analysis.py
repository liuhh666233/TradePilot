from fastapi import APIRouter

router = APIRouter()


@router.get("/technical")
def technical(stock_code: str, period: str = "daily"):
    return {"stock_code": stock_code, "period": period, "status": "not_implemented"}


@router.get("/valuation")
def valuation(stock_code: str):
    return {"stock_code": stock_code, "status": "not_implemented"}


@router.get("/sector_rotation")
def sector_rotation():
    return {"status": "not_implemented"}

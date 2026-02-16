from fastapi import APIRouter

router = APIRouter()


@router.get("/list")
def list_signals(stock_code: str = None):
    return {"stock_code": stock_code, "signals": [], "status": "not_implemented"}


@router.get("/score")
def composite_score(stock_code: str):
    return {"stock_code": stock_code, "score": None, "status": "not_implemented"}

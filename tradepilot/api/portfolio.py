from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from tradepilot.db import get_conn

router = APIRouter()


class PositionCreate(BaseModel):
    stock_code: str
    stock_name: str
    buy_date: str
    buy_price: float
    quantity: int


class TradeCreate(BaseModel):
    date: str
    stock_code: str
    stock_name: str
    direction: str
    price: float
    quantity: int
    reason: Optional[str] = None


@router.get("/positions")
def list_positions():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM portfolio WHERE status = 'open' ORDER BY buy_date DESC").fetchdf()
    return rows.to_dict(orient="records")


@router.post("/positions")
def add_position(pos: PositionCreate):
    conn = get_conn()
    conn.execute(
        "INSERT INTO portfolio (stock_code, stock_name, buy_date, buy_price, quantity) VALUES (?, ?, ?, ?, ?)",
        [pos.stock_code, pos.stock_name, pos.buy_date, pos.buy_price, pos.quantity],
    )
    return {"status": "ok"}


@router.delete("/positions/{position_id}")
def close_position(position_id: int):
    conn = get_conn()
    conn.execute("UPDATE portfolio SET status = 'closed' WHERE id = ?", [position_id])
    return {"status": "ok"}


@router.get("/trades")
def list_trades():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM trades ORDER BY date DESC").fetchdf()
    return rows.to_dict(orient="records")


@router.post("/trades")
def add_trade(trade: TradeCreate):
    conn = get_conn()
    conn.execute(
        "INSERT INTO trades (date, stock_code, stock_name, direction, price, quantity, reason) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [trade.date, trade.stock_code, trade.stock_name, trade.direction, trade.price, trade.quantity, trade.reason],
    )
    return {"status": "ok"}

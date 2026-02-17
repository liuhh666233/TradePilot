"""交易计划 CRUD + 评估接口"""
import json
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from tradepilot.db import get_conn
from tradepilot.data.mock_provider import MockProvider
from tradepilot.analysis.technical import analyze_stock
from tradepilot.analysis.valuation import analyze_valuation
from tradepilot.analysis.fund_flow import analyze_etf_flow, analyze_northbound, analyze_margin, compute_market_sentiment
from tradepilot.analysis.sector_rotation import analyze_sectors
from tradepilot.analysis.signal import compute_composite_score
from tradepilot.analysis.risk import evaluate_stop_loss, evaluate_take_profit

router = APIRouter()
_provider = MockProvider()


class PlanCreate(BaseModel):
    stock_code: str
    stock_name: str
    entry_target_price: Optional[float] = None
    entry_quantity: Optional[int] = None
    entry_reason: Optional[str] = None
    stop_loss_pct: float = -10
    take_profit_pct: float = 30


class PlanStatusUpdate(BaseModel):
    status: str
    entry_actual_price: Optional[float] = None
    entry_triggered_at: Optional[str] = None


def _evaluate_stock(stock_code: str) -> dict:
    """对一只股票做全面评估，返回建仓建议。"""
    kline = _provider.get_stock_daily(stock_code, "2024-01-01", "2025-12-31")
    valuation_df = _provider.get_stock_valuation(stock_code, "2024-01-01", "2025-12-31")
    sector_df = _provider.get_sector_data("2024-01-01", "2025-12-31")
    nb_df = _provider.get_northbound_flow("2024-01-01", "2025-12-31")
    margin_df = _provider.get_margin_data("2024-01-01", "2025-12-31")

    # 技术分析
    tech = analyze_stock(kline)
    all_tech_signals = tech["cross_signals"] + tech["divergence_signals"] + tech["volume_signals"]

    # 估值分析
    val = analyze_valuation(valuation_df, kline)

    # 资金面
    etf_results = {}
    for code in ["510050", "510300", "510500", "512100"]:
        etf_df = _provider.get_etf_flow(code, "2024-01-01", "2025-12-31")
        etf_results.update(analyze_etf_flow(etf_df))
    nb_result = analyze_northbound(nb_df)
    margin_result = analyze_margin(margin_df)
    sentiment = compute_market_sentiment(etf_results, nb_result, margin_result)

    # 行业轮动
    sector_result = analyze_sectors(sector_df)
    sector_position = None
    # 简化: 不知道个股属于哪个板块，暂不标记
    for h in sector_result.get("high_positions", []):
        sector_position = "high"
        break

    # 综合评分
    composite = compute_composite_score(all_tech_signals, val, sentiment, sector_position)

    # 建仓条件汇总
    entry_conditions = []
    for s in all_tech_signals:
        if s["type"] in ("golden_cross", "bull_divergence", "volume_breakout", "extreme_low_volume"):
            entry_conditions.append(f"✓ {s['name']}")
    for s in val.get("signals", []):
        if s.get("direction") == "buy":
            entry_conditions.append(f"✓ {s['name']}")
    if sentiment["score"] >= 60:
        entry_conditions.append(f"✓ 资金面{sentiment['label']}")
    for opp in sector_result.get("low_opportunities", []):
        entry_conditions.append(f"✓ {opp['sector']}板块低位")

    # 止损/止盈条件
    stop_conditions = ["周线MACD死叉", "跌破20日支撑位", "放量下跌"]
    profit_conditions = ["周线MACD死叉", "顶背离", "高位缩量", "市场情绪过热", "板块高位预警"]

    # 建议价位
    support = kline["low"].tail(20).min() if len(kline) >= 20 else kline["close"].iloc[-1] * 0.95
    current = kline["close"].iloc[-1]

    return {
        "current_price": round(float(current), 2),
        "support_price": round(float(support), 2),
        "composite_score": composite["score"],
        "score_label": composite["label"],
        "reasons": composite["reasons"],
        "entry_conditions": entry_conditions,
        "stop_loss_conditions": stop_conditions,
        "take_profit_conditions": profit_conditions,
        "risk_reward_ratio": val.get("risk_reward_ratio"),
        "pe_percentile": val.get("pe_percentile"),
        "pb_percentile": val.get("pb_percentile"),
        "market_sentiment": sentiment,
        "sector_rotation": {
            "high": sector_result.get("high_positions", []),
            "low": sector_result.get("low_opportunities", []),
            "suggestions": sector_result.get("switch_suggestions", []),
        },
    }


@router.get("/evaluate/{stock_code}")
def evaluate(stock_code: str):
    """评估一只股票的建仓条件。"""
    return _evaluate_stock(stock_code)


@router.get("/list")
def list_plans(status: Optional[str] = None):
    conn = get_conn()
    if status:
        rows = conn.execute("SELECT * FROM trade_plan WHERE status = ? ORDER BY created_at DESC", [status]).fetchdf()
    else:
        rows = conn.execute("SELECT * FROM trade_plan ORDER BY created_at DESC").fetchdf()
    return rows.to_dict(orient="records")


@router.post("/create")
def create_plan(plan: PlanCreate):
    evaluation = _evaluate_stock(plan.stock_code)
    conn = get_conn()

    entry_price = plan.entry_target_price or evaluation["support_price"]
    stop_loss_price = round(entry_price * (1 + plan.stop_loss_pct / 100), 2)
    take_profit_price = round(entry_price * (1 + plan.take_profit_pct / 100), 2)

    conn.execute("""
        INSERT INTO trade_plan (
            stock_code, stock_name,
            entry_target_price, entry_quantity, entry_reason, entry_conditions,
            stop_loss_price, stop_loss_pct, stop_loss_conditions,
            take_profit_price, take_profit_pct, take_profit_conditions,
            risk_reward_ratio, composite_score, signal_summary
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        plan.stock_code, plan.stock_name,
        entry_price, plan.entry_quantity, plan.entry_reason,
        json.dumps(evaluation["entry_conditions"], ensure_ascii=False),
        stop_loss_price, plan.stop_loss_pct,
        json.dumps(evaluation["stop_loss_conditions"], ensure_ascii=False),
        take_profit_price, plan.take_profit_pct,
        json.dumps(evaluation["take_profit_conditions"], ensure_ascii=False),
        evaluation.get("risk_reward_ratio"),
        evaluation["composite_score"],
        json.dumps(evaluation["reasons"], ensure_ascii=False),
    ])
    return {"status": "ok", "evaluation": evaluation}


@router.put("/{plan_id}/status")
def update_plan_status(plan_id: int, update: PlanStatusUpdate):
    conn = get_conn()
    if update.entry_actual_price and update.entry_triggered_at:
        conn.execute(
            "UPDATE trade_plan SET status = ?, entry_actual_price = ?, entry_triggered_at = ? WHERE id = ?",
            [update.status, update.entry_actual_price, update.entry_triggered_at, plan_id],
        )
    else:
        conn.execute("UPDATE trade_plan SET status = ? WHERE id = ?", [update.status, plan_id])
    return {"status": "ok"}


@router.get("/{plan_id}/monitor")
def monitor_plan(plan_id: int):
    """监控一个活跃计划的止盈止损状态。"""
    conn = get_conn()
    rows = conn.execute("SELECT * FROM trade_plan WHERE id = ?", [plan_id]).fetchdf()
    if rows.empty:
        return {"error": "plan not found"}
    plan = rows.iloc[0].to_dict()

    if plan["status"] != "active" or not plan.get("entry_actual_price"):
        return {"plan": plan, "stop_loss": None, "take_profit": None}

    kline = _provider.get_stock_daily(plan["stock_code"], "2024-01-01", "2025-12-31")
    current_price = float(kline["close"].iloc[-1])

    # 获取市场情绪和板块位置，传入止盈评估
    nb_df = _provider.get_northbound_flow("2024-01-01", "2025-12-31")
    margin_df = _provider.get_margin_data("2024-01-01", "2025-12-31")
    etf_results = {}
    for code in ["510050", "510300", "510500", "512100"]:
        etf_df = _provider.get_etf_flow(code, "2024-01-01", "2025-12-31")
        etf_results.update(analyze_etf_flow(etf_df))
    nb_result = analyze_northbound(nb_df)
    margin_result = analyze_margin(margin_df)
    sentiment = compute_market_sentiment(etf_results, nb_result, margin_result)

    sector_df = _provider.get_sector_data("2024-01-01", "2025-12-31")
    sector_result = analyze_sectors(sector_df)
    sector_position = None
    for h in sector_result.get("high_positions", []):
        sector_position = "high"
        break

    sl = evaluate_stop_loss(plan["entry_actual_price"], current_price, plan["stop_loss_pct"], kline)
    tp = evaluate_take_profit(plan["entry_actual_price"], current_price, plan["take_profit_pct"], kline, sentiment, sector_position)

    return {"plan": plan, "current_price": current_price, "stop_loss": sl, "take_profit": tp}


@router.delete("/{plan_id}")
def delete_plan(plan_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM trade_plan WHERE id = ?", [plan_id])
    return {"status": "ok"}

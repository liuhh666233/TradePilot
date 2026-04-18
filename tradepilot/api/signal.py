from fastapi import APIRouter
from tradepilot.analysis.fund_flow import (
    analyze_etf_flow,
    analyze_margin,
    analyze_northbound,
    compute_market_sentiment,
)
from tradepilot.analysis.signal import compute_composite_score
from tradepilot.analysis.technical import analyze_stock
from tradepilot.analysis.valuation import analyze_valuation
from tradepilot.data import get_provider

router = APIRouter()


def _get_provider():
    return get_provider()


@router.get("/list")
def list_signals(stock_code: str):
    provider = _get_provider()
    kline = provider.get_stock_daily(stock_code, "2024-01-01", "2025-12-31")
    tech = analyze_stock(kline)
    all_signals = (
        tech["cross_signals"] + tech["divergence_signals"] + tech["volume_signals"]
    )

    val_df = provider.get_stock_valuation(stock_code, "2024-01-01", "2025-12-31")
    val = analyze_valuation(val_df, kline)
    for s in val.get("signals", []):
        all_signals.append({"date": "", "type": s["type"], "name": s["name"]})

    return {"stock_code": stock_code, "signals": all_signals}


@router.get("/score")
def composite_score(stock_code: str):
    provider = _get_provider()
    kline = provider.get_stock_daily(stock_code, "2024-01-01", "2025-12-31")
    tech = analyze_stock(kline)
    all_tech = (
        tech["cross_signals"] + tech["divergence_signals"] + tech["volume_signals"]
    )

    val_df = provider.get_stock_valuation(stock_code, "2024-01-01", "2025-12-31")
    val = analyze_valuation(val_df, kline)

    nb_df = provider.get_northbound_flow("2024-01-01", "2025-12-31")
    margin_df = provider.get_margin_data("2024-01-01", "2025-12-31")
    etf_results = {}
    for code in ["510050", "510300", "510500", "512100"]:
        etf_df = provider.get_etf_flow(code, "2024-01-01", "2025-12-31")
        etf_results.update(analyze_etf_flow(etf_df))
    nb_result = analyze_northbound(nb_df)
    margin_result = analyze_margin(margin_df)
    sentiment = compute_market_sentiment(etf_results, nb_result, margin_result)

    result = compute_composite_score(all_tech, val, sentiment)
    return {"stock_code": stock_code, **result}


@router.get("/market_sentiment")
def market_sentiment():
    provider = _get_provider()
    nb_df = provider.get_northbound_flow("2024-01-01", "2025-12-31")
    margin_df = provider.get_margin_data("2024-01-01", "2025-12-31")
    etf_results = {}
    for code in ["510050", "510300", "510500", "512100"]:
        etf_df = provider.get_etf_flow(code, "2024-01-01", "2025-12-31")
        etf_results.update(analyze_etf_flow(etf_df))
    nb_result = analyze_northbound(nb_df)
    margin_result = analyze_margin(margin_df)
    sentiment = compute_market_sentiment(etf_results, nb_result, margin_result)
    return {
        "etf": etf_results,
        "northbound": nb_result,
        "margin": margin_result,
        "sentiment": sentiment,
    }

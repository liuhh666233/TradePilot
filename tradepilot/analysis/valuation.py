"""估值分析: PB/PE 分位数 + 值博率"""
import pandas as pd


def compute_percentile(series: pd.Series, window: int = 250) -> float:
    """计算当前值在近 window 个交易日中的百分位。"""
    if len(series) < 2:
        return 50.0
    recent = series.tail(window).dropna()
    if len(recent) < 2:
        return 50.0
    current = recent.iloc[-1]
    return float((recent < current).sum() / len(recent) * 100)


def analyze_valuation(valuation_df: pd.DataFrame, kline_df: pd.DataFrame) -> dict:
    """
    估值分析。
    valuation_df: date, pe_ttm, pb, ps, market_cap
    kline_df: date, close, high, low
    """
    result = {"pe_percentile": None, "pb_percentile": None, "risk_reward_ratio": None, "signals": []}

    if valuation_df.empty or kline_df.empty:
        return result

    pe_pct = compute_percentile(valuation_df["pe_ttm"])
    pb_pct = compute_percentile(valuation_df["pb"])
    result["pe_percentile"] = round(pe_pct, 1)
    result["pb_percentile"] = round(pb_pct, 1)

    # 值博率: upside / downside
    recent = kline_df.tail(60)
    if len(recent) >= 10:
        current = recent["close"].iloc[-1]
        support = recent["low"].min()
        resistance = recent["high"].max()
        downside = max((current - support) / current, 0.01)
        upside = max((resistance - current) / current, 0.01)
        rrr = round(upside / downside, 2)
        result["risk_reward_ratio"] = rrr

        if rrr > 3:
            result["signals"].append({"type": "high_rrr", "name": f"高值博率({rrr})", "direction": "buy"})

    if pe_pct < 30:
        result["signals"].append({"type": "low_pe", "name": f"PE低分位({pe_pct:.0f}%)", "direction": "buy"})
    if pb_pct < 30:
        result["signals"].append({"type": "low_pb", "name": f"PB低分位({pb_pct:.0f}%)", "direction": "buy"})
    if pe_pct > 80:
        result["signals"].append({"type": "high_pe", "name": f"PE高分位({pe_pct:.0f}%)", "direction": "sell"})
    if pb_pct > 80:
        result["signals"].append({"type": "high_pb", "name": f"PB高分位({pb_pct:.0f}%)", "direction": "sell"})

    return result

"""风控: 止盈止损条件评估"""
import pandas as pd
from tradepilot.analysis.technical import compute_macd, detect_cross, detect_divergence


def evaluate_stop_loss(
    entry_price: float,
    current_price: float,
    stop_loss_pct: float,
    kline_df: pd.DataFrame,
) -> dict:
    """
    止损评估。
    返回 {"triggered": bool, "conditions": [...], "pnl_pct": float}
    """
    pnl_pct = (current_price - entry_price) / entry_price * 100
    conditions = []
    triggered = False

    # 固定比例止损
    if pnl_pct <= stop_loss_pct:
        conditions.append({"type": "pct_stop", "name": f"亏损{pnl_pct:.1f}%达到止损线{stop_loss_pct}%", "triggered": True})
        triggered = True

    if len(kline_df) >= 30:
        df = compute_macd(kline_df)
        # 日线MACD死叉
        crosses = detect_cross(df.tail(10))
        for c in crosses:
            if c["type"] == "death_cross":
                conditions.append({"type": "death_cross", "name": "MACD死叉", "triggered": True})
                triggered = True

        # 跌破前低
        low_20 = kline_df["low"].tail(20).min()
        if current_price < low_20:
            conditions.append({"type": "break_support", "name": f"跌破20日支撑位{low_20:.2f}", "triggered": True})
            triggered = True

        # 放量下跌
        last = kline_df.iloc[-1]
        vol_ma5 = kline_df["volume"].tail(5).mean()
        if last["volume"] > vol_ma5 * 2 and last["close"] < last["open"]:
            conditions.append({"type": "volume_drop", "name": "放量下跌", "triggered": True})
            triggered = True

    return {"triggered": triggered, "conditions": conditions, "pnl_pct": round(pnl_pct, 2)}


def evaluate_take_profit(
    entry_price: float,
    current_price: float,
    take_profit_pct: float,
    kline_df: pd.DataFrame,
    market_sentiment: dict | None = None,
    sector_position: str | None = None,
) -> dict:
    """
    止盈评估。
    返回 {"triggered": bool, "conditions": [...], "pnl_pct": float}
    """
    pnl_pct = (current_price - entry_price) / entry_price * 100
    conditions = []
    triggered = False

    # 固定比例止盈
    if pnl_pct >= take_profit_pct:
        conditions.append({"type": "pct_profit", "name": f"盈利{pnl_pct:.1f}%达到止盈线{take_profit_pct}%", "triggered": True})
        triggered = True

    if len(kline_df) >= 30:
        df = compute_macd(kline_df)
        # 死叉
        crosses = detect_cross(df.tail(10))
        for c in crosses:
            if c["type"] == "death_cross":
                conditions.append({"type": "death_cross", "name": "MACD死叉", "triggered": True})
                triggered = True

        # 顶背离
        divergences = detect_divergence(df)
        for d in divergences:
            if d["type"] == "bear_divergence":
                conditions.append({"type": "bear_divergence", "name": "顶背离", "triggered": True})
                triggered = True

        # 高位缩量
        last = kline_df.iloc[-1]
        vol_ma5 = kline_df["volume"].tail(5).mean()
        high_20 = kline_df["close"].tail(20).max()
        low_20 = kline_df["close"].tail(20).min()
        position = (current_price - low_20) / (high_20 - low_20 + 1e-10)
        if last["volume"] < vol_ma5 * 0.5 and position > 0.8:
            conditions.append({"type": "high_shrink", "name": "高位缩量", "triggered": True})
            triggered = True

    # 市场情绪过热
    if market_sentiment and market_sentiment.get("score", 0) >= 80:
        conditions.append({"type": "overheated", "name": f"市场情绪过热({market_sentiment['score']:.0f})", "triggered": True})
        triggered = True

    # 板块高位
    if sector_position == "high":
        conditions.append({"type": "sector_high", "name": "所在板块处于高位", "triggered": True})
        triggered = True

    return {"triggered": triggered, "conditions": conditions, "pnl_pct": round(pnl_pct, 2)}

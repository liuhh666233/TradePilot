"""资金面分析: ETF资金流 / 两融 / 北向资金"""
import pandas as pd


def analyze_etf_flow(etf_df: pd.DataFrame) -> dict:
    """
    ETF 资金流分析。etf_df: date, etf_code, net_inflow, volume
    返回每个 ETF 的近 5 日净流入和趋势。
    """
    result = {}
    for code, group in etf_df.groupby("etf_code"):
        group = group.sort_values("date")
        recent5 = group.tail(5)
        net_5d = recent5["net_inflow"].sum()
        consecutive = 0
        for v in reversed(recent5["net_inflow"].values):
            if (v > 0 and consecutive >= 0) or (consecutive == 0):
                consecutive = consecutive + 1 if v > 0 else -1
            elif v < 0 and consecutive <= 0:
                consecutive -= 1
            else:
                break
        result[code] = {
            "net_5d": round(float(net_5d), 2),
            "latest": round(float(recent5["net_inflow"].iloc[-1]), 2) if len(recent5) > 0 else 0,
            "trend_days": consecutive,
        }
    return result


def analyze_northbound(nb_df: pd.DataFrame) -> dict:
    """北向资金分析。nb_df: date, net_buy, buy_amount, sell_amount"""
    if nb_df.empty:
        return {"net_5d": 0, "latest": 0, "trend_days": 0}
    nb_df = nb_df.sort_values("date")
    recent5 = nb_df.tail(5)
    net_5d = recent5["net_buy"].sum()
    consecutive = 0
    for v in reversed(recent5["net_buy"].values):
        if consecutive == 0:
            consecutive = 1 if v > 0 else -1
        elif (v > 0 and consecutive > 0) or (v < 0 and consecutive < 0):
            consecutive = consecutive + (1 if v > 0 else -1)
        else:
            break
    return {
        "net_5d": round(float(net_5d), 2),
        "latest": round(float(recent5["net_buy"].iloc[-1]), 2),
        "trend_days": consecutive,
    }


def analyze_margin(margin_df: pd.DataFrame) -> dict:
    """融资余额分析。margin_df: date, stock_code, margin_balance, margin_buy"""
    if margin_df.empty:
        return {"total_balance": 0, "daily_change": 0, "trend_days": 0}
    daily = margin_df.groupby("date")["margin_balance"].sum().sort_index()
    if len(daily) < 2:
        return {"total_balance": round(float(daily.iloc[-1]), 2), "daily_change": 0, "trend_days": 0}
    change = daily.diff()
    consecutive = 0
    for v in reversed(change.dropna().values):
        if consecutive == 0:
            consecutive = 1 if v > 0 else -1
        elif (v > 0 and consecutive > 0) or (v < 0 and consecutive < 0):
            consecutive = consecutive + (1 if v > 0 else -1)
        else:
            break
    return {
        "total_balance": round(float(daily.iloc[-1]), 2),
        "daily_change": round(float(change.iloc[-1]), 2),
        "trend_days": consecutive,
    }


def compute_market_sentiment(etf_result: dict, nb_result: dict, margin_result: dict) -> dict:
    """综合市场情绪评分 (0-100)。"""
    score = 50  # 中性起点

    # ETF 资金流
    total_etf_5d = sum(v["net_5d"] for v in etf_result.values())
    if total_etf_5d > 0:
        score += min(total_etf_5d / 1e9 * 5, 15)
    else:
        score += max(total_etf_5d / 1e9 * 5, -15)

    # 北向资金
    if nb_result["net_5d"] > 0:
        score += min(nb_result["net_5d"] / 1e10 * 5, 10)
    else:
        score += max(nb_result["net_5d"] / 1e10 * 5, -10)

    # 融资余额趋势
    score += margin_result["trend_days"] * 2

    score = max(0, min(100, score))
    if score >= 80:
        label = "过热"
    elif score >= 60:
        label = "偏热"
    elif score >= 40:
        label = "中性"
    else:
        label = "偏冷"

    return {"score": round(score, 1), "label": label}

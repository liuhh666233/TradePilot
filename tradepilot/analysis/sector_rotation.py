"""行业轮动 + 高切低"""
import pandas as pd


def analyze_sectors(sector_df: pd.DataFrame) -> dict:
    """
    行业轮动分析。sector_df: date, sector, avg_pe, avg_pb, change_1d, change_5d, change_20d, change_60d
    返回每个板块的动量排名、估值分位、高切低标记。
    """
    if sector_df.empty:
        return {"sectors": [], "high_positions": [], "low_opportunities": [], "switch_suggestions": []}

    # 取最新一天数据
    latest_date = sector_df["date"].max()
    latest = sector_df[sector_df["date"] == latest_date].copy()

    if latest.empty:
        return {"sectors": [], "high_positions": [], "low_opportunities": [], "switch_suggestions": []}

    # 动量排名 (60日涨幅)
    latest["momentum_rank"] = latest["change_60d"].rank(pct=True)

    # 估值分位 (用 PB 在当前截面的排名近似)
    latest["valuation_rank"] = latest["avg_pb"].rank(pct=True)

    sectors = latest.to_dict(orient="records")

    # 高位品种: 动量前20% 且 估值前20%
    high = latest[(latest["momentum_rank"] >= 0.8) & (latest["valuation_rank"] >= 0.8)]
    high_positions = [{"sector": r["sector"], "change_60d": round(r["change_60d"], 2), "avg_pb": round(r["avg_pb"], 2)} for _, r in high.iterrows()]

    # 低位潜力: 动量后30% 且 估值后30%
    low = latest[(latest["momentum_rank"] <= 0.3) & (latest["valuation_rank"] <= 0.3)]
    low_opportunities = [{"sector": r["sector"], "change_60d": round(r["change_60d"], 2), "avg_pb": round(r["avg_pb"], 2)} for _, r in low.iterrows()]

    # 高切低建议
    suggestions = []
    for _, h in high.iterrows():
        for _, l in low.iterrows():
            suggestions.append({
                "from_sector": h["sector"],
                "to_sector": l["sector"],
                "reason": f"{h['sector']}涨幅{h['change_60d']:.1f}%+PB{h['avg_pb']:.1f} → {l['sector']}涨幅{l['change_60d']:.1f}%+PB{l['avg_pb']:.1f}",
            })

    return {
        "sectors": sectors,
        "high_positions": high_positions,
        "low_opportunities": low_opportunities,
        "switch_suggestions": suggestions[:5],
    }

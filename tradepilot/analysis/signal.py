"""综合信号评分"""


def compute_composite_score(
    technical_signals: list[dict],
    valuation_result: dict,
    fund_flow_sentiment: dict,
    sector_position: str | None = None,
) -> dict:
    """
    综合评分 (0-100)。
    technical_signals: 技术面信号列表
    valuation_result: 估值分析结果
    fund_flow_sentiment: 市场情绪 {"score": 0-100, "label": "..."}
    sector_position: "high" / "low" / None
    """
    score = 50  # 中性起点
    reasons = []

    # 技术面 (权重 20%)
    tech_score = 0
    for s in technical_signals:
        if s["type"] in ("golden_cross", "bull_divergence", "volume_breakout", "extreme_low_volume"):
            tech_score += 15
            reasons.append(f"✓ {s['name']}")
        elif s["type"] in ("death_cross", "bear_divergence", "high_shrink"):
            tech_score -= 15
            reasons.append(f"✗ {s['name']}")
    tech_score = max(-20, min(20, tech_score))
    score += tech_score

    # 估值面 (权重 15%)
    val_signals = valuation_result.get("signals", [])
    for s in val_signals:
        if s.get("direction") == "buy":
            score += 5
            reasons.append(f"✓ {s['name']}")
        elif s.get("direction") == "sell":
            score -= 5
            reasons.append(f"✗ {s['name']}")

    # 资金面 (权重 25%)
    sentiment_score = fund_flow_sentiment.get("score", 50)
    score += (sentiment_score - 50) * 0.5
    if sentiment_score >= 60:
        reasons.append(f"✓ 资金面{fund_flow_sentiment['label']}({sentiment_score:.0f})")
    elif sentiment_score < 40:
        reasons.append(f"✗ 资金面{fund_flow_sentiment['label']}({sentiment_score:.0f})")

    # 行业轮动 (权重 10%)
    if sector_position == "low":
        score += 10
        reasons.append("✓ 板块处于低位(高切低机会)")
    elif sector_position == "high":
        score -= 10
        reasons.append("✗ 板块处于高位(注意兑现)")

    score = max(0, min(100, score))

    if score >= 80:
        label = "强烈看多"
    elif score >= 60:
        label = "看多"
    elif score >= 40:
        label = "中性"
    elif score >= 20:
        label = "看空"
    else:
        label = "强烈看空"

    return {"score": round(score, 1), "label": label, "reasons": reasons}

---
paths:
  - "tradepilot/analysis/**/*.py"
---

# Analysis 分析引擎模块

核心分析层，6 个模块全部已实现。

## Key Files

| File | Role |
|------|------|
| `tradepilot/analysis/technical.py` | MACD计算 + 金叉/死叉 + 顶背离/底背离 + 成交量异动 |
| `tradepilot/analysis/valuation.py` | PB/PE 分位数 + 值博率计算 |
| `tradepilot/analysis/fund_flow.py` | ETF资金流 + 北向资金 + 融资余额 + 综合市场情绪评分 |
| `tradepilot/analysis/sector_rotation.py` | 行业轮动排名 + 高位预警/低位机会 + 高切低建议 |
| `tradepilot/analysis/signal.py` | 综合信号评分 (技术20% + 估值15% + 资金25% + 轮动10%) |
| `tradepilot/analysis/risk.py` | 止盈止损评估 (固定比例 + 技术指标 + 市场情绪) |

## 信号体系

| 信号 | 来源 | 权重 |
|------|------|------|
| MACD 金叉/死叉 | technical.py | 20% (技术面) |
| MACD 背离 | technical.py | 20% (技术面) |
| 成交量异动 | technical.py | 20% (技术面) |
| 值博率 | valuation.py | 15% (估值面) |
| 高切低 | sector_rotation.py | 10% (轮动) |
| ETF 资金流 | fund_flow.py | 25% (资金面) |
| 融资余额 | fund_flow.py | 25% (资金面) |
| 抽离提醒 | risk.py | 风控独立触发 |

## Design Patterns

- 每个分析模块接收 DataFrame 输入，返回信号列表或评分 dict
- 综合信号 (signal.py) 汇总各模块结果，加权评分 (0-100)
- risk.py 独立评估止盈止损，返回 triggered + conditions 列表

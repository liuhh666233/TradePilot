---
paths:
  - "tradepilot/analysis/**/*.py"
---

# Analysis 分析引擎模块

核心分析层，实现技术指标计算、估值分析、资金面分析、行业轮动、综合信号生成和风控。

## Key Files

| File | Role |
|------|------|
| `tradepilot/analysis/technical.py` | MACD/背离/成交量异动 (待实现) |
| `tradepilot/analysis/valuation.py` | PB/PE 分位数 + 值博率 (待实现) |
| `tradepilot/analysis/fund_flow.py` | ETF/两融/北向资金分析 (待实现) |
| `tradepilot/analysis/sector_rotation.py` | 行业轮动 + 高切低 (待实现) |
| `tradepilot/analysis/signal.py` | 综合信号评分 (待实现) |
| `tradepilot/analysis/risk.py` | 风控 + 抽离提醒 (待实现) |

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

- 每个分析模块接收 DataFrame 输入，返回信号列表
- 综合信号 (signal.py) 汇总各模块结果，加权评分

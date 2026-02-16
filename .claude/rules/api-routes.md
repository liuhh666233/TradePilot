---
paths:
  - "tradepilot/api/**/*.py"
---

# API 路由模块

FastAPI REST API 层，前端通过 `/api/*` 访问。

## Key Files

| File | Role |
|------|------|
| `tradepilot/api/market.py` | 行情数据接口 (股票列表/K线/ETF/北向/两融/估值/行业) |
| `tradepilot/api/portfolio.py` | 持仓 CRUD + 交易记录 |
| `tradepilot/api/analysis.py` | 技术分析/估值/行业轮动 (接入分析引擎) |
| `tradepilot/api/signal.py` | 信号列表/综合评分/市场情绪 |
| `tradepilot/api/trade_plan.py` | 交易计划 (评估/CRUD/状态流转/监控止盈止损) |

## API 路径

| 路径 | 方法 | 说明 |
|------|------|------|
| `/api/health` | GET | 健康检查 |
| `/api/market/stocks` | GET | 股票列表 |
| `/api/market/stock_daily` | GET | 个股日K线 |
| `/api/market/index_daily` | GET | 指数日K线 |
| `/api/market/etf_flow` | GET | ETF 资金流 |
| `/api/market/northbound` | GET | 北向资金 |
| `/api/market/margin` | GET | 两融数据 |
| `/api/market/valuation` | GET | 个股估值 |
| `/api/market/sectors` | GET | 行业数据 |
| `/api/portfolio/positions` | GET/POST | 持仓列表/新增 |
| `/api/portfolio/positions/{id}` | DELETE | 关闭持仓 |
| `/api/portfolio/trades` | GET/POST | 交易记录 |
| `/api/analysis/technical` | GET | 技术分析 (MACD+信号) |
| `/api/analysis/valuation` | GET | 估值分析 (PB/PE分位+值博率) |
| `/api/analysis/sector_rotation` | GET | 行业轮动 (排名+高切低) |
| `/api/signal/list` | GET | 信号列表 |
| `/api/signal/score` | GET | 综合评分 |
| `/api/signal/market_sentiment` | GET | 市场情绪 (ETF+北向+两融) |
| `/api/trade_plan/evaluate/{code}` | GET | 评估个股建仓条件 |
| `/api/trade_plan/list` | GET | 交易计划列表 |
| `/api/trade_plan/create` | POST | 创建交易计划 |
| `/api/trade_plan/{id}/status` | PUT | 更新计划状态 |
| `/api/trade_plan/{id}/monitor` | GET | 监控止盈止损 |
| `/api/trade_plan/{id}` | DELETE | 删除计划 |

---
paths:
  - "tradepilot/api/**/*.py"
---

# API 路由模块

FastAPI REST API 层，当前前端主路径主要通过 `/api/workflow`、`/api/summary`、`/api/scheduler` 与 `/api/portfolio` 访问。

## Key Files

| File | Role |
|------|------|
| `tradepilot/api/workflow.py` | workflow latest/history/status/context/insight read-write |
| `tradepilot/api/summary.py` | richer watchlist + trading-status |
| `tradepilot/api/portfolio.py` | 持仓 CRUD + 交易记录 |
| `tradepilot/api/collector.py` | 数据接入手动 sync (market/news/bilibili) |
| `tradepilot/api/briefing.py` | legacy alerts / compatibility |

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
| `/api/collector/market/sync` | POST | 手动同步行情数据 |
| `/api/collector/news/sync` | POST | 手动同步新闻 |
| `/api/collector/bilibili/sync` | POST | 手动同步 B 站视频 |
| `/api/collector/runs` | GET | 同步运行历史 |
| `/api/collector/status` | GET | 接入状态总览 |
| `/api/summary/watchlist` | GET/PUT | richer watch config 读写（兼容旧平面结构） |
| `/api/workflow/latest` | GET | latest workflow run |
| `/api/workflow/history` | GET | workflow history |
| `/api/workflow/status` | GET | pre/post latest status |
| `/api/workflow/context/latest` | GET | latest structured context |
| `/api/workflow/insight/latest` | GET | latest insight + freshness state |
| `/api/workflow/insight` | PUT | The-One insight write-back |
| `/api/workflow/pre/run` | POST | 手动运行盘前 workflow |
| `/api/workflow/post/run` | POST | 手动运行盘后 workflow |

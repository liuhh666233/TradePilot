---
paths:
  - "tradepilot/**/*.py"
---

# TradePilot Backend

Python 后端，基于 FastAPI + DuckDB，提供行情数据、技术分析、持仓管理、交易计划等 REST API。

## Key Files

| File | Role |
|------|------|
| `tradepilot/main.py` | FastAPI 入口，挂载 6 组路由 + CORS |
| `tradepilot/config.py` | 配置 (DB路径、Provider 类型、存储路径) |
| `tradepilot/db.py` | DuckDB 连接管理 + 19 张表初始化 |

## Architecture

```
main.py (FastAPI app)
  ├── api/market.py      → data/get_provider() (行情读取)
  ├── api/portfolio.py   → db.py (持仓 CRUD)
  ├── api/analysis.py    → analysis/* + get_provider() (技术/估值/轮动分析)
  ├── api/signal.py      → analysis/* + get_provider() (综合信号+情绪)
  ├── api/trade_plan.py  → analysis/* + get_provider() (交易计划评估+监控)
  └── api/collector.py   → ingestion/service.py (手动 sync 入口)

data/__init__.py          → get_provider() 工厂
data/provider.py          → DataProvider ABC (11 methods)
data/mock_provider.py     → MockProvider 实现
data/akshare_provider.py  → AKShareProvider 实现

ingestion/models.py       → Pydantic v2 模型
ingestion/service.py      → 统一编排 (market/news/bilibili sync)

collector/news.py         → 新闻 collector
collector/bilibili.py     → B 站 collector
```

## Design Patterns

- **Provider 工厂**: `get_provider()` 按 config 返回 Mock 或 AKShare provider
- **三层接入框架**: Provider (结构化) + Collector (内容型) + Ingestion Service (编排)
- **模块化路由**: 6 个 router，通过 `include_router` 挂载
- **DuckDB 单例**: `get_conn()` 全局连接，首次调用时初始化表
- **分析引擎**: 6 个独立模块，接收 DataFrame 输入，返回信号/评分
- **运行历史**: 每次 sync 记录到 `ingestion_runs` 表

## Testing

- **Run**: `python -c "from tradepilot.main import app; print('OK')"`
- **API**: `python -m uvicorn tradepilot.main:app --reload` 后访问 `/docs`

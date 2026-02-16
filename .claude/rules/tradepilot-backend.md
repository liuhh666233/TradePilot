---
paths:
  - "tradepilot/**/*.py"
---

# TradePilot Backend

Python 后端，基于 FastAPI + DuckDB，提供行情数据、技术分析、持仓管理、交易计划等 REST API。

## Key Files

| File | Role |
|------|------|
| `tradepilot/main.py` | FastAPI 入口，挂载 5 组路由 + CORS |
| `tradepilot/config.py` | 配置 (DB路径、数据源切换) |
| `tradepilot/db.py` | DuckDB 连接管理 + 11 张表初始化 |

## Architecture

```
main.py (FastAPI app)
  ├── api/market.py      → data/mock_provider.py (行情数据)
  ├── api/portfolio.py   → db.py (持仓 CRUD)
  ├── api/analysis.py    → analysis/technical + valuation + sector_rotation
  ├── api/signal.py      → analysis/signal + fund_flow (综合信号+情绪)
  └── api/trade_plan.py  → analysis/* (交易计划评估+监控)

data/provider.py         → 抽象接口 (ABC)
data/mock_provider.py    → Mock 实现
data/akshare_provider.py → 真实数据 (待实现)
```

## Design Patterns

- **Provider 抽象**: `DataProvider` ABC 定义数据接口，Mock/AKShare 可切换
- **模块化路由**: 每个 API 域一个 router，通过 `include_router` 挂载
- **DuckDB 单例**: `get_conn()` 全局连接，首次调用时初始化表
- **分析引擎**: 6 个独立模块，接收 DataFrame 输入，返回信号/评分

## Testing

- **Run**: `python -c "from tradepilot.main import app; print('OK')"`
- **API**: `uvicorn tradepilot.main:app --reload` 后访问 `/docs`

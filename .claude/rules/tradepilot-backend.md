---
paths:
  - "tradepilot/**/*.py"
---

# TradePilot Backend

Python 后端，基于 FastAPI + DuckDB，当前主路径已收口到 daily workflow data platform：pre/post workflow、context/insight contract、watch config、新闻同步与 Dashboard 展示支撑。

## Key Files

| File | Role |
|------|------|
| `tradepilot/main.py` | FastAPI 入口，挂载 summary / portfolio / collector / briefing / workflow / scheduler |
| `tradepilot/config.py` | 配置 (DB路径、Provider 类型、存储路径) |
| `tradepilot/db.py` | DuckDB 连接管理 + workflow_runs / workflow_insights / fact tables 初始化 |
| `tradepilot/workflow/models.py` | context / insight schema、状态模型、The-One section schema |
| `tradepilot/workflow/service.py` | pre/post workflow orchestration、context builder、news mapping、insight 读写 |
| `tradepilot/summary/models.py` | richer watch config + backward-compatible normalizer |
| `tradepilot/collector/news.py` | 真实新闻采集（财联社 / 东方财富） |

## Architecture

```
main.py (FastAPI app)
  ├── api/summary.py     → watchlist / trading-status
  ├── api/portfolio.py   → db.py (持仓 CRUD)
  ├── api/collector.py   → ingestion/service.py (手动 sync 入口)
  ├── api/briefing.py    → legacy alerts / compatibility surface
  ├── api/workflow.py    → workflow/service.py (latest/history/status/context/insight)
  └── api/scheduler_api.py → scheduler 状态与历史

workflow/models.py        → WorkflowContextPayload / WorkflowInsightPayload / section schema
workflow/service.py       → pre/post workflow orchestration + context builders + news mapping
summary/models.py         → richer watch config + backward compatibility
summary/api.py            → watchlist JSON read/write + normalizer
collector/news.py         → 财联社 / 东方财富新闻采集
ingestion/service.py      → market/news/bilibili sync 编排
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

---
paths:
  - "tradepilot/data/**/*.py"
  - "tradepilot/ingestion/**/*.py"
  - "tradepilot/collector/**/*.py"
---

# Data Provider & Ingestion 模块

统一数据接入框架，分三层：Provider（结构化行情）、Collector（非结构化内容）、Ingestion Service（编排入口）。

## Key Files

| File | Role |
|------|------|
| `tradepilot/data/provider.py` | `DataProvider` ABC，定义 11 个数据接口 |
| `tradepilot/data/__init__.py` | `get_provider()` 工厂，按 config 返回 Mock 或 AKShare |
| `tradepilot/data/mock_provider.py` | Mock 实现，生成模拟 A 股数据 |
| `tradepilot/data/akshare_provider.py` | AKShare 实现，从东方财富取真实数据 |
| `tradepilot/ingestion/models.py` | Pydantic v2 模型 (IngestionRun, SyncRequest, etc.) |
| `tradepilot/ingestion/service.py` | 统一编排入口 (market/news/bilibili sync) |
| `tradepilot/collector/news.py` | 新闻 collector 骨架 |
| `tradepilot/collector/bilibili.py` | B 站 collector 骨架 |

## Provider 接口 (11 methods)

| 方法 | 返回 | 用途 |
|------|------|------|
| `get_stock_daily(code, start, end)` | DataFrame | 个股日K线 |
| `get_stock_weekly(code, start, end)` | DataFrame | 个股周K线 |
| `get_stock_monthly(code, start, end)` | DataFrame | 个股月K线 |
| `get_index_daily(code, start, end)` | DataFrame | 指数日K线 |
| `get_etf_flow(code, start, end)` | DataFrame | ETF 资金流 |
| `get_margin_data(start, end)` | DataFrame | 两融数据 |
| `get_northbound_flow(start, end)` | DataFrame | 北向资金 |
| `get_stock_valuation(code, start, end)` | DataFrame | 个股估值 |
| `get_sector_data(start, end)` | DataFrame | 行业数据 |
| `get_sector_stocks(sector, as_of_date)` | DataFrame | 板块成分股 |
| `get_stock_sector(code, as_of_date)` | DataFrame | 个股所属板块 |

## Ingestion 编排

- `IngestionService.sync_market()` — 结构化行情入库
- `IngestionService.sync_news()` — 新闻采集入库
- `IngestionService.sync_bilibili()` — B 站视频采集入库
- 每次 sync 都记录 `IngestionRun` 到 `ingestion_runs` 表

## How to Extend

- 添加新 Provider: 继承 `DataProvider`，实现 11 个方法，更新 `get_provider()` 工厂
- 添加新 Collector: 新建 `tradepilot/collector/xxx.py`，在 `IngestionService` 加 `sync_xxx()`
- 配置切换: 修改 `tradepilot/config.py` 中的 `DATA_PROVIDER`

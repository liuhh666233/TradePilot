---
paths:
  - "tradepilot/data/**/*.py"
---

# Data Provider 模块

数据采集层，定义统一的数据接口并提供 Mock/真实数据实现。

## Key Files

| File | Role |
|------|------|
| `tradepilot/data/provider.py` | `DataProvider` ABC，定义 7 个数据接口 |
| `tradepilot/data/mock_provider.py` | Mock 实现，生成模拟 A 股数据 |

## API 接口

| 方法 | 返回 | 用途 |
|------|------|------|
| `get_stock_daily(code, start, end)` | DataFrame | 个股日K线 |
| `get_index_daily(code, start, end)` | DataFrame | 指数日K线 |
| `get_etf_flow(code, start, end)` | DataFrame | ETF 资金流 |
| `get_margin_data(start, end)` | DataFrame | 两融数据 |
| `get_northbound_flow(start, end)` | DataFrame | 北向资金 |
| `get_stock_valuation(code, start, end)` | DataFrame | 个股估值 |
| `get_sector_data(start, end)` | DataFrame | 行业数据 |

## How to Extend

添加新数据源: 继承 `DataProvider`，实现所有抽象方法，在 `config.py` 中切换。

---
paths:
  - "webapp/src/**/*.tsx"
  - "webapp/src/**/*.ts"
  - "webapp/package.json"
  - "webapp/vite.config.ts"
---

# TradePilot Frontend

React + TypeScript + Vite 前端，使用 Ant Design 组件库 + @ant-design/charts 图表。

## Key Files

| File | Role |
|------|------|
| `webapp/src/App.tsx` | 根组件，侧边栏导航 + 5 页面路由 |
| `webapp/src/main.tsx` | React 入口 |
| `webapp/src/services/api.ts` | API 调用封装 (market/analysis/signal/portfolio/trade_plan) |
| `webapp/vite.config.ts` | Vite 配置 + API 代理 |

## 页面

| 路由 | 页面 | 功能 |
|------|------|------|
| `/` | Dashboard | 仪表盘: 大盘K线 + 资金面仪表盘 + 行业板块 + 持仓总览 |
| `/analysis` | StockAnalysis | 个股分析: K线+MACD图 + 估值(PE/PB/值博率) + 信号列表 |
| `/sectors` | SectorMap | 行业地图: 涨跌排名(5/20/60日) + 高位预警/低位机会 + 高切低建议 |
| `/portfolio` | Portfolio | 持仓管理: 持仓CRUD + 交易记录 |
| `/plans` | TradePlan | 交易计划: 评估→创建→监控止盈止损 |

## Tech Stack

- React 18 + TypeScript + Vite (SWC)
- Ant Design + @ant-design/icons + @ant-design/charts
- react-router-dom
- API 代理: `/api` → `http://localhost:8000`

## Dev Commands

- `yarn dev` — 启动开发服务器
- `yarn build` — 编译生产版本

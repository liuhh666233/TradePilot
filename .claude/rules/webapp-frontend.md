---
paths:
  - "webapp/src/**/*.tsx"
  - "webapp/src/**/*.ts"
  - "webapp/package.json"
  - "webapp/vite.config.ts"
---

# TradePilot Frontend

React + TypeScript + Vite 前端，使用 Ant Design 组件库。

## Key Files

| File | Role |
|------|------|
| `webapp/src/App.tsx` | 根组件，侧边栏导航 + 路由 |
| `webapp/src/main.tsx` | React 入口 |
| `webapp/src/services/api.ts` | API 调用封装 |
| `webapp/vite.config.ts` | Vite 配置 + API 代理 |

## 页面

| 路由 | 页面 | 功能 |
|------|------|------|
| `/` | Dashboard | 仪表盘: 大盘概览 + 波浪定位 + 资金面 |
| `/analysis` | StockAnalysis | 个股分析: 技术面 + 估值 + 盘面语言 |
| `/sectors` | SectorMap | 行业地图: 板块热度 + 轮动 + 高切低 |
| `/portfolio` | Portfolio | 持仓管理: 当前持仓 + 盈亏 + 抽离提醒 |
| `/signals` | Signals | 信号中心: 买卖信号列表 + 筛选 |

## Tech Stack

- React 18 + TypeScript + Vite (SWC)
- Ant Design + @ant-design/icons + @ant-design/charts
- react-router-dom
- API 代理: `/api` → `http://localhost:8000`

## Dev Commands

- `yarn dev` — 启动开发服务器
- `yarn build` — 编译生产版本

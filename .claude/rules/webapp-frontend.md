---
paths:
  - "webapp/src/**/*.tsx"
  - "webapp/src/**/*.ts"
  - "webapp/package.json"
  - "webapp/vite.config.ts"
---

# TradePilot Frontend

React + TypeScript + Vite 前端，当前已收口为 workflow-first shell：Dashboard 以 The-One insight-first / TradePilot context-fallback 为主，Portfolio 保留为持仓输入界面。

## Key Files

| File | Role |
|------|------|
| `webapp/src/App.tsx` | 路由入口，已基本收口到 Dashboard + Portfolio |
| `webapp/src/main.tsx` | React 入口 |
| `webapp/src/pages/Dashboard/index.tsx` | insight-first / context-fallback 主页面 |
| `webapp/src/services/api.ts` | workflow / summary / scheduler / portfolio API typed contract |
| `webapp/vite.config.ts` | Vite 配置 + API 代理 |

## 页面

| 路由 | 页面 | 功能 |
|------|------|------|
| `/` | Dashboard | Daily Workflow 主页面：The-One insight、TradePilot context、workflow/scheduler 状态与历史 |
| `/portfolio` | Portfolio | 持仓管理：持仓 CRUD + 交易记录，继续作为 workflow position 输入源 |

其余 legacy 页面已不是当前主产品路径，不应作为新功能实现的默认落点。

## Tech Stack

- React 18 + TypeScript + Vite (SWC)
- Ant Design + @ant-design/icons + @ant-design/charts
- react-router-dom
- API 代理: `/api` → `http://localhost:8000`

## Dev Commands

- `yarn dev` — 启动开发服务器
- `yarn build` — 编译生产版本

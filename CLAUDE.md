# TradePilot

A股辅助决策看板系统，前后端分离架构。

## Tech Stack

- 后端: Python + FastAPI + DuckDB
- 前端: React 18 + TypeScript + Vite + Ant Design
- 开发环境: Nix Flakes

## Quick Start

```bash
# 进入开发环境
nix develop

# 启动后端
uvicorn tradepilot.main:app --reload

# 启动前端 (另一个终端)
cd webapp && yarn dev
```

## Project Structure

```
tradepilot/          # Python 后端
  main.py            # FastAPI 入口
  config.py          # 配置
  db.py              # DuckDB 连接 + 表初始化
  data/              # 数据采集层 (Mock/AKShare)
  analysis/          # 分析引擎 (技术/估值/资金/轮动/信号/风控)
  api/               # REST API 路由
  portfolio/         # 组合管理
  scheduler/         # 定时任务

webapp/              # React 前端
  src/pages/         # 5 个页面 (Dashboard/StockAnalysis/SectorMap/Portfolio/TradePlan)
  src/services/      # API 调用封装
  src/components/    # 通用组件

docs/                # 文档
  系统设计.md         # 架构 + 数据需求 + 信号逻辑
  投资策略.md         # 投资策略原文
  worklog.md         # 工作日志
```

## Modules

| Module | Rules file | Description |
|--------|-----------|-------------|
| Backend | `.claude/rules/tradepilot-backend.md` | FastAPI 后端整体架构 |
| API Routes | `.claude/rules/api-routes.md` | REST API 路由定义 |
| Data Provider | `.claude/rules/data-provider.md` | 数据采集层 (Mock/真实) |
| Analysis Engine | `.claude/rules/analysis-engine.md` | 分析引擎 (技术/估值/资金/轮动) |
| Frontend | `.claude/rules/webapp-frontend.md` | React 前端 |

## Adding a New Module

1. 创建 `.claude/rules/<module>.md`，包含 `paths:` frontmatter 列出相关文件
2. 在上方 Modules 表中添加条目
3. 文档内容: key files, architecture, design patterns, testing

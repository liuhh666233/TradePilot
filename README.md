# TradePilot

A股辅助决策看板系统 — 市场信息面板 + 交易计划管理（建仓/止损/止盈）

## Tech Stack

- 后端: Python + FastAPI + DuckDB
- 前端: React 18 + TypeScript + Vite + Ant Design + @ant-design/charts
- 开发环境: Nix Flakes

## Quick Start

```bash
# 进入开发环境
nix develop

# 启动后端 (终端1)
uvicorn tradepilot.main:app --reload

# 启动前端 (终端2)
cd webapp && yarn install && yarn dev
```

后端 API 文档: http://localhost:8000/docs
前端页面: http://localhost:5173

## Project Structure

```
tradepilot/              # Python 后端
  main.py                # FastAPI 入口
  config.py              # 配置 (DB路径/数据源切换)
  db.py                  # DuckDB 连接 + 11张表初始化
  data/                  # 数据采集层
    provider.py          # 抽象接口 (ABC)
    mock_provider.py     # Mock 数据实现
  analysis/              # 分析引擎 (6个模块)
    technical.py         # MACD/金叉死叉/背离/成交量异动
    valuation.py         # PB/PE分位数 + 值博率
    fund_flow.py         # ETF/北向/两融 + 市场情绪评分
    sector_rotation.py   # 行业轮动 + 高切低建议
    signal.py            # 综合信号评分
    risk.py              # 止盈止损评估
  api/                   # REST API 路由
    market.py            # 行情数据
    portfolio.py         # 持仓 CRUD
    analysis.py          # 技术/估值/轮动分析
    signal.py            # 信号/评分/市场情绪
    trade_plan.py        # 交易计划 (评估/CRUD/监控)
  portfolio/             # 组合管理
  scheduler/             # 定时任务 (待实现)

webapp/                  # React 前端
  src/
    App.tsx              # 路由 + 侧边栏布局
    pages/
      Dashboard/         # 仪表盘: 大盘K线 + 资金面 + 行业板块 + 持仓总览
      StockAnalysis/     # 个股分析: K线+MACD + 估值 + 信号
      SectorMap/         # 行业地图: 涨跌排名 + 高切低建议
      Portfolio/         # 持仓管理: 持仓CRUD + 交易记录
      TradePlan/         # 交易计划: 评估→创建→监控止盈止损
    services/api.ts      # API 调用封装

docs/
  系统设计.md             # 架构 + 数据需求 + 信号逻辑 + V1定义
  投资策略.md             # 投资策略原文
  worklog.md             # 工作日志
```

## Features (V1)

- **市场信息面板**: 大盘指数K线、资金面仪表盘(北向/ETF/两融/情绪)、行业板块热力图、持仓盈亏总览
- **个股分析**: MACD指标、金叉/死叉/背离检测、成交量异动、PB/PE分位数、值博率
- **行业轮动**: 板块涨跌排名(5/20/60日)、高位预警/低位机会标记、高切低建议
- **交易计划**: 建仓评估(技术+估值+资金+轮动)、止损(固定比例+技术指标)、止盈(固定比例+技术+抽离纪律)
- **持仓管理**: 持仓CRUD、交易记录

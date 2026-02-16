# TradePilot 工作日志

## 已完成

### Phase 1: 基础骨架 (commit: 75fd641)

**后端 (tradepilot/)**
- FastAPI 项目结构搭建 (main.py 入口 + CORS + 路由挂载)
- DuckDB 初始化 (db.py, 10 张表: stock_daily/index_daily/etf_flow/margin_data/northbound_flow/stock_valuation/sector_data/portfolio/trades/signals)
- Mock 数据 Provider (data/provider.py 抽象接口 + data/mock_provider.py 模拟实现)
- API 路由:
  - market.py: 股票列表/日K线/指数/ETF资金流/北向资金/两融/估值/行业数据
  - portfolio.py: 持仓 CRUD + 交易记录
  - analysis.py / signal.py: 骨架 (待实现)

**前端 (webapp/)**
- 安装依赖: react-router-dom, antd, @ant-design/icons, @ant-design/charts
- App.tsx: 侧边栏导航 + 5 页面路由
- 页面骨架: Dashboard / StockAnalysis / SectorMap / Portfolio / Signals
- Vite 代理配置 (/api → localhost:8000)
- 编译验证通过

**文档 (docs/)**
- 投资策略.md: 策略原文
- 系统设计.md: 架构 + 策略需求 + 数据需求 + 8 个信号计算逻辑 + 表结构 + 实施计划

---

## 待执行

### Phase 2: 技术分析引擎
- [ ] analysis/technical.py: MACD 计算 (EMA12/26, DIF, DEA, MACD柱)
- [ ] analysis/technical.py: 金叉/死叉检测, 顶背离/底背离检测
- [ ] analysis/technical.py: 成交量异动信号 (放量突破/高位缩量/地量)
- [ ] api/analysis.py: 接入技术分析引擎, 返回 MACD + 信号
- [ ] 前端 Dashboard: 指数概览卡片 + K线图 + MACD 指标图
- [ ] 前端 StockAnalysis: 股票选择器 + K线+MACD图 + 信号列表

### Phase 3: 估值 + 行业轮动
- [ ] analysis/valuation.py: PB/PE 分位数 + 值博率计算
- [ ] analysis/sector_rotation.py: 行业涨幅排名 + 高切低逻辑
- [ ] 前端 SectorMap: 行业热度地图 + 高切低建议

### Phase 4: 资金面 + 综合信号
- [ ] analysis/fund_flow.py: ETF 资金流/两融/北向资金分析
- [ ] analysis/signal.py: 综合信号评分系统
- [ ] analysis/risk.py: 抽离提醒 (风控)
- [ ] 前端 Signals: 信号中心
- [ ] 前端 Portfolio: 持仓管理 + 盈亏 + 抽离提醒

### Phase 5: 接入真实数据
- [ ] data/akshare_provider.py: 替换 Mock 为 AKShare
- [ ] scheduler/jobs.py: 定时任务 (每日收盘后更新)

# TradePilot 工作日志

## 已完成

### Phase 1: 基础骨架 (commit: 75fd641)

**后端 (tradepilot/)**
- FastAPI 项目结构搭建 (main.py 入口 + CORS + 路由挂载)
- DuckDB 初始化 (db.py, 10 张表)
- Mock 数据 Provider (data/provider.py 抽象接口 + data/mock_provider.py)
- API 路由: market.py / portfolio.py / analysis.py / signal.py 骨架

**前端 (webapp/)**
- 安装依赖: react-router-dom, antd, @ant-design/icons, @ant-design/charts
- App.tsx: 侧边栏导航 + 5 页面路由骨架
- Vite 代理配置 (/api → localhost:8000)

**文档 (docs/)**
- 投资策略.md / 系统设计.md / .claude/rules/*.md / CLAUDE.md

---

### V1: 分析引擎 + 交易计划 + 前端页面

**Step 1: 分析引擎 (6 个模块)**
- [x] analysis/technical.py: MACD (EMA12/26, DIF, DEA, MACD柱) + 金叉/死叉 + 顶背离/底背离 + 成交量异动 (放量突破/高位缩量/地量)
- [x] analysis/valuation.py: PB/PE 分位数 + 值博率计算
- [x] analysis/fund_flow.py: ETF 资金流 + 北向资金 + 融资余额 + 综合市场情绪评分
- [x] analysis/sector_rotation.py: 行业轮动排名 + 高位预警/低位机会 + 高切低建议
- [x] analysis/signal.py: 综合信号评分 (技术面20% + 估值面15% + 资金面25% + 轮动10%)
- [x] analysis/risk.py: 止盈止损评估 (固定比例 + 技术指标 + 市场情绪)

**Step 2: 交易计划后端**
- [x] db.py: 新增 trade_plan 表 (建仓/止损/止盈三阶段)
- [x] api/trade_plan.py: 评估接口 + CRUD + 状态流转 + 监控止盈止损

**Step 3: API 完善**
- [x] api/analysis.py: 接入 technical/valuation/sector_rotation 引擎
- [x] api/signal.py: 接入综合评分 + 市场情绪接口

**Step 4: 前端页面 (5 个页面)**
- [x] Dashboard: 4 区块 (大盘K线 + 资金面仪表盘 + 行业板块 + 持仓总览)
- [x] StockAnalysis: 股票选择器 + K线+MACD图 + 估值面板 + 信号列表
- [x] SectorMap: 行业涨跌幅排名 (5/20/60日) + 估值对比 + 高切低建议
- [x] Portfolio: 持仓管理 + 交易记录 + 新增持仓
- [x] TradePlan: 新建交易计划 (评估→设参→创建) + 计划列表 + 监控止盈止损

### Bug 修复 (3个)

- [x] Bug 1: `fund_flow.py:94` — `nb_result["trend_days"]` → `margin_result["trend_days"]` (情绪评分双计北向、漏计融资)
- [x] Bug 2: `risk.py:27` — 注释"周线死叉"改为"日线MACD死叉" (误导性注释)
- [x] Bug 3: `trade_plan.py:189` — `evaluate_take_profit()` 补传 `market_sentiment` 和 `sector_position` (止盈条件永远不触发)

### Dashboard 重设计

- [x] Row 1: 4个指数概览卡片(上证/深证/创业板/科创50) + 市场情绪进度条
- [x] Row 2: 大盘K线图 + 资金面(北向/融资/ETF)
- [x] Row 3: 行业板块(按涨幅排序+高切低建议) + 持仓盈亏(现价/盈亏%/盈亏额) + 活跃交易计划
- [x] api.ts: 新增 `getIndices` 接口

---

## 待执行

### Phase 2a: 自选股 + 基础设施

- [ ] watchlist 表 + CRUD API + 前端页面
- [ ] 日K→周K/月K聚合函数 + 周线MACD
- [ ] 申万行业分类数据 → 个股↔板块映射
- [ ] 政策面手动标注字段（全局评分 + 自选股级别标注）

### Phase 2b: 每日扫描引擎（核心）

- [ ] scan_watchlist() — 遍历自选股生成建仓建议
- [ ] scan_positions() — 遍历持仓生成止损/止盈建议
- [ ] daily_report 表 + API
- [ ] Dashboard 新增"今日建议"卡片

### Phase 2c: 精细化

- [ ] 分批建仓/止盈逻辑 → trade_plan 状态机扩展
- [ ] 时间止损（持仓天数监控）
- [ ] 定时任务（盘后自动扫描）
- [ ] 接入真实数据 (AKShare)

### 优化

- [ ] 前端 code-split (动态 import 减小 bundle)
- [ ] 删除 DuckDB 文件后重新初始化的处理

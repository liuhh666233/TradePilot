---
title: "TradePilot Briefing-First Refactor"
status: in-review
mode: refactoring
created: 2026-04-06
updated: 2026-04-06
modules:
  - workflow
  - scheduler
  - summary
  - briefing
  - ingestion
  - webapp-dashboard
---

# TradePilot Briefing-First Refactor

## Overview

TradePilot 已经完成了第一轮 workflow-first 重构，但内部仍然保留了旧时代的大量并行产品能力：`analysis`、`signal`、`trade_plan`、`summary/daily`、`summary/5m`、`SectorMap`、`StockAnalysis`、`TradePlan` 等仍然与新的 Daily Workflow 并存。与此同时，`DailyScanner` 仍然是一个重分析引擎，会在 workflow 执行时按股票拉取长区间数据、估值、两融、北向、ETF 流、板块映射与 sector rotation，导致系统复杂、缓慢、脆弱，也偏离“日常交易工作台”的目标。

本次重构的目标不是继续增强扫描能力，而是把 TradePilot 重构成一个面向**主观波段交易**的 **briefing-first workflow 系统**：每天稳定产出一份盘后复盘和一份盘前准备，让用户围绕市场环境、观察池、持仓健康度、风险提示与次日关注点开展日常交易，而不是在一堆分析页面中自行筛选信息。

参考 `/Users/lhh/Github/The-One/.claude/skills/the-one/skills/daily-workflow` 和 `/Users/lhh/Github/The-One/briefings` 的实际产物，本次重构将以以下原则收口：
- 第一优先服务对象：**主观波段**
- 盘后核心输出：**市场大势、板块定位、持仓健康度、明日准备**
- 盘前核心输出：**昨日复盘摘要、隔夜信息、今日关注清单、操作计划**
- 操作计划粒度：**只给观察框架，不直接给买卖动作**
- 板块组织方式：**当日主线 + 固定观察池**
- 第二阶段盘中观察：**以盘前判断验证为主、持仓监控为先、热点跟踪为辅**

## Goals

- [ ] 把 `workflow` 的产物从 `scan-driven payload` 改成 `briefing-driven payload`
- [ ] 让盘后 workflow 输出真正可用的 post briefing
- [ ] 让盘前 workflow 承接上一交易日 post briefing，而不是重复做重分析
- [ ] 让首页 Dashboard 只围绕 briefing 结构渲染
- [ ] 下线当前不属于 briefing 产品范围的旧页面与旧 API
- [ ] 去掉 workflow 主路径中固定 `2024-01-01 ~ 2025-12-31` 的历史窗口依赖
- [ ] 为第二阶段 intraday watch 预留清晰边界，但不在本阶段做成独立重产品

## Constraints

- 维持现有 `workflow` API 与 scheduler 的基本入口，避免从产品入口层面推翻重来
- 第一阶段优先改造主路径，不追求一次性删除所有旧代码
- `watchlist` 当前仍然由 `tradepilot/api/summary.py` 管理，短期内可继续复用
- `portfolio` 仍然是 position 输入源，短期不宜破坏其数据读写能力
- 新闻能力先保留“采集 + 分类/筛选”的轻能力，不做复杂研判系统
- 不引入回测系统，不让长期历史研究成为主路径依赖
- UI 和后端的语义要一致：既然产品叫 Daily Workflow，页面结构和后端数据结构都要对应 briefing 章节

## Scope

### Modules Involved

| Module | Current Role | Planned Changes |
|--------|-------------|-----------------|
| `tradepilot/workflow` | 盘前/盘后 workflow 编排与快照落库 | 成为 briefing-first 的核心编排层，摆脱 `DailyScanner.run()` 主路径依赖 |
| `tradepilot/scheduler` | 定时触发 workflow job | 保持入口不变，继续服务 pre/post workflow |
| `tradepilot/ingestion` | 行情/新闻同步 | 保留为数据接入层，不再主导 UI 语义 |
| `tradepilot/summary` | watchlist、daily summary、5m brief | 保留 `watchlist` / `trading-status`，降级 `daily` / `5m` 为非主产品能力 |
| `tradepilot/scanner` | 重分析扫描引擎 | 降级为历史遗留兼容能力，退出 workflow 主路径 |
| `webapp Dashboard` | 当前 workflow-first 首页 | 重写为纯 briefing UI |
| Legacy pages/routes | 旧分析、交易计划、市场概览 | 从主产品中下线 |

### Key Files

| File | Role | Impact |
|------|------|--------|
| `tradepilot/workflow/models.py` | Workflow summary/schema | 改为 briefing-first 结构 |
| `tradepilot/workflow/service.py` | pre/post workflow orchestration | 重写主逻辑，移除对 `DailyScanner.run()` 的主依赖 |
| `tradepilot/db.py` | DuckDB schema and tables | 新增 workflow briefing 快照相关表或补充现有模型 |
| `tradepilot/api/workflow.py` | Workflow API surface | 维持入口，返回新 summary 结构 |
| `tradepilot/main.py` | Router registration | 下线不再属于主产品的旧 router |
| `tradepilot/ingestion/service.py` | Sync market/news | 继续复用，成为 briefing 的输入层 |
| `tradepilot/api/summary.py` | Watchlist and summary routes | 保留 watchlist/trading-status，降级 daily/5m |
| `tradepilot/scanner/daily.py` | Old daily scanner | 退出主路径，部分读取能力可保留 |
| `tradepilot/scheduler/jobs.py` | Workflow jobs/history/alerts | 保持 workflow job 入口与失败预警 |
| `webapp/src/pages/Dashboard/index.tsx` | Main Daily Workflow UI | 改为 briefing sections |
| `webapp/src/App.tsx` | Routes and navigation | 隐藏/移除旧页面入口 |
| `webapp/src/services/api.ts` | Frontend API layer | 移除不再需要的旧 API exports |

### Out of Scope

- 全市场重分析与回测体系
- 长周期因子和多年度历史研究
- 自动买卖决策引擎
- 复杂 trade plan 生命周期管理
- 独立 MarketSummary / StockAnalysis / SectorMap / TradePlan 作为主产品继续演进
- 在本阶段完整实现 intraday watch

## Design

### Product Shape

TradePilot 未来的主产品形态应是：

1. **Post-market Briefing**
   - 回答：今天市场怎么样？主线是什么？持仓健康吗？明天要带什么结论？
2. **Pre-market Briefing**
   - 回答：昨天结论是什么？夜间新增了什么变量？今天最该看什么？今天的观察框架是什么？
3. **Intraday Watch**（第二阶段）
   - 回答：盘前判断是否被验证？持仓是否异常？主线是否强化/切换？

系统的最终产物不再是 scan table 或并行分析页面，而是 daily briefing。

### Briefing Schema v1

#### Post-market Briefing

```ts
type PostMarketBriefing = {
  phase: "post_market"
  workflow_date: string
  requested_date?: string | null
  resolved_date?: string | null
  date_resolution?: "exact" | "fallback_previous_trading_day" | "fallback_next_trading_day"
  status: "success" | "partial" | "failed" | "skipped"
  generated_at: string

  market_overview: MarketOverview
  sector_positioning: SectorPositioning
  position_health: PositionHealth
  next_day_prep: NextDayPreparation

  alerts: AlertItem[]
  watch_context: WatchContext
  metadata: BriefingMetadata
}
```

#### Pre-market Briefing

```ts
type PreMarketBriefing = {
  phase: "pre_market"
  workflow_date: string
  requested_date?: string | null
  resolved_date?: string | null
  date_resolution?: "exact" | "fallback_previous_trading_day" | "fallback_next_trading_day"
  status: "success" | "partial" | "failed" | "skipped"
  generated_at: string

  yesterday_recap: YesterdayRecap
  overnight_news: OvernightNews
  today_watchlist: TodayWatchlist
  action_frame: ActionFrame

  alerts: AlertItem[]
  watch_context: WatchContext
  metadata: BriefingMetadata
}
```

#### Intraday Watch (phase 2)

```ts
type IntradayWatch = {
  phase: "intraday"
  workflow_date: string
  generated_at: string

  validation: IntradayValidation
  position_monitor: IntradayPositionMonitor
  theme_monitor: IntradayThemeMonitor

  alerts: AlertItem[]
}
```

### Data Model Mapping

为避免 workflow 每次运行都在 provider 层做重分析，briefing 需要围绕交易日快照组织。推荐数据块如下：

- `market_overview`
  - 指数、breadth、涨停/跌停/炸板、风格、ETF 风险代理、市场总结
- `sector_positioning`
  - 当日主线、当日弱线、固定观察池板块、观察重点
- `position_health`
  - 持仓板块健康度、关注个股/持仓状态、风险标记
- `next_day_prep`
  - 明日偏向、重点方向、风险提示、明日 checkpoints
- `yesterday_recap`
  - 由上一条 post briefing 精简而来
- `overnight_news`
  - 新闻亮点、分类、可选板块映射
- `today_watchlist`
  - 市场 checkpoints、重点板块、持仓观察
- `action_frame`
  - posture、focus_directions、risk_warnings、notes

### Architecture

#### Current State

当前主路径：
- `run_post_market_workflow()` → `sync_market()` → `DailyScanner.run()` → scanner payload → Dashboard
- `run_pre_market_workflow()` → `sync_news()` + watchlist + alerts + previous post summary → Dashboard

问题：
- post workflow 仍由 scanner 主导
- scanner 使用了大量不稳定或非必要 provider 接口
- UI 语义与数据结构仍偏技术实现，不是 briefing 章节
- 旧页面/旧 API 与新 workflow 并存，产品边界不清晰

#### Target State

目标主路径：
- `run_post_market_workflow()`
  - `sync_market()`
  - `build_market_overview()`
  - `build_sector_positioning()`
  - `build_position_health()`
  - `build_next_day_prep()`
  - persist briefing summary
- `run_pre_market_workflow()`
  - load previous post briefing
  - `sync_news()`
  - `build_yesterday_recap()`
  - `build_overnight_news()`
  - `build_today_watchlist()`
  - `build_action_frame()`
  - persist briefing summary

`DailyScanner.run()` 不再是 workflow 主路径。

### Interfaces

#### Workflow Summary Interface

后端返回的 `summary` 必须直接对应前端 briefing sections，避免前端自己重组：
- post tab 直接消费：
  - `market_overview`
  - `sector_positioning`
  - `position_health`
  - `next_day_prep`
- pre tab 直接消费：
  - `yesterday_recap`
  - `overnight_news`
  - `today_watchlist`
  - `action_frame`

#### Router Interface

主产品保留：
- `/api/workflow/*`
- `/api/scheduler/*`
- `/api/summary/watchlist`
- `/api/summary/trading-status`

降级或卸载：
- `/api/trade_plan/*`
- `/api/analysis/*`
- `/api/signal/*`
- `/api/market/*`
- `/api/summary/daily`
- `/api/summary/5m`

### Data Flow

#### Post-market
1. Resolve trading date
2. Sync market data for current trade date
3. Load watchlist + positions
4. Build `market_overview`
5. Build `sector_positioning`
6. Build `position_health`
7. Build `next_day_prep`
8. Load alerts / generate warnings
9. Persist workflow briefing snapshot

#### Pre-market
1. Resolve trading date
2. Load latest post-market briefing
3. Sync/load overnight news
4. Load watchlist + positions + alerts
5. Build `yesterday_recap`
6. Build `overnight_news`
7. Build `today_watchlist`
8. Build `action_frame`
9. Persist workflow briefing snapshot

## Migration Strategy

### Step 1: Structure before logic
先改 `WorkflowSummary` 结构和 Dashboard 消费结构，允许部分字段占位，避免一开始就卡在全部数据源都必须完成。

### Step 2: Replace post-market core
优先把 post workflow 从 `DailyScanner.run()` 驱动改成 briefing builders 驱动。post briefing 是整个闭环的起点。

### Step 3: Rebuild pre-market on top of post carry-over
盘前不再重新推导市场，而是承接 post briefing 的 carry-over，再叠加夜间信息与今日观察框架。

### Step 4: Remove parallel products
在 briefing 主路径稳定后，再隐藏旧页面、卸载旧 router、清理 dead exports。

### Backward Compatibility

短期内可允许以下兼容策略：
- `WorkflowSummary` 保留旧字段一小段时间，但新字段为主
- `DailyScanner` 保留读取或 alerts 兼容能力，不再做 workflow 主路径
- `summary/5m` 和 `summary/daily` 先降级，不立即彻底删除内部实现

## Design Decisions

### Decision 1: TradePilot 首先服务主观波段，而不是研究/回测
**Date**: 2026-04-06
**Status**: Decided

**Context**: 用户明确表示不需要引入太多历史数据用于回测，更关注近期一段时间的信息，并希望 daily workflow 真正服务日常交易。

**Options considered**:
1. **保留分析平台定位**：继续保留大量分析页和历史分析能力 — Pros: 功能丰富 / Cons: 复杂、分散、难以真正服务日常交易
2. **转成 briefing-first workflow** — Pros: 聚焦、可执行、贴近日常使用 / Cons: 需要重写现有语义和下线旧功能

**Decision**: 选择 briefing-first workflow。

**Consequences**: 所有数据结构、页面结构和 API 暴露面都要围绕 pre/post/intraday briefing，而不是围绕 scanner/signal/analysis 产品线。

### Decision 2: 盘后 briefing 是整个闭环的起点
**Date**: 2026-04-06
**Status**: Decided

**Context**: pre briefing 的昨日摘要、今日观察清单和操作框架都应承接上一交易日的 post briefing。

**Options considered**:
1. **盘前独立生成** — Pros: 实现简单 / Cons: 丢失 workflow 闭环
2. **盘前承接盘后** — Pros: 真正形成 daily workflow / Cons: 需要更明确的 carry-over 结构

**Decision**: 盘前必须承接盘后。

**Consequences**: `next_day_prep` 和 `yesterday_recap` 成为关键结构化字段。

### Decision 3: 操作计划只给观察框架，不直接给买卖动作
**Date**: 2026-04-06
**Status**: Decided

**Context**: 用户希望系统真正服务主观波段交易，但不希望第一阶段做成直接给买卖动作的“假智能执行器”。

**Options considered**:
1. **直接输出买卖动作** — Pros: 更激进 / Cons: 噪音大、风险高、容易失真
2. **只给观察框架** — Pros: 稳定、克制、实用 / Cons: 自动化程度较低

**Decision**: 只给观察框架。

**Consequences**: `action_frame` / `next_day_prep` 输出 posture、focus、risk，而不是买卖指令。

### Decision 4: 板块定位采用“当日主线 + 固定观察池”
**Date**: 2026-04-06
**Status**: Decided

**Context**: 用户明确选择不继续走全市场排行榜产品，而是围绕真正可用的交易方向组织信息。

**Options considered**:
1. **全市场 top/bottom 排行主导** — Pros: 信息全 / Cons: 噪音大、与交易关注对象脱节
2. **固定观察池** — Pros: 聚焦 / Cons: 容易丢失市场当天新变化
3. **当日主线 + 固定观察池** — Pros: 兼顾市场变化与用户长期关注 / Cons: 需要双轨结构

**Decision**: 选择当日主线 + 固定观察池。

**Consequences**: `sector_positioning` 需要同时支持 `market_leaders/market_laggards` 和 `watch_sectors`。

### Decision 5: `DailyScanner.run()` 退出 workflow 主路径
**Date**: 2026-04-06
**Status**: Decided

**Context**: 当前 scanner 是性能瓶颈和复杂度源头，且依赖大量不稳定 provider 接口和固定历史区间。

**Options considered**:
1. **继续围绕 scanner 做包装** — Pros: 复用已有实现 / Cons: 继续背负复杂度
2. **workflow service 直接编排 briefing builders** — Pros: 语义正确、性能可控 / Cons: 需要重构主逻辑

**Decision**: scanner 退出主路径。

**Consequences**: 需要重写 post-market 主逻辑，并在必要时只保留 scanner 的少量兼容读取能力。

## Phases

### Phase 1: Reshape Workflow Summary Around Briefing Schema

**Required Reading:**
| File | Purpose | Why Needed |
|------|---------|------------|
| `tradepilot/workflow/models.py` | Current workflow models and summary structure | Need to replace scan-oriented summary fields with briefing-first fields |
| `tradepilot/workflow/service.py` | Current pre/post orchestration | Need to understand existing summary assembly and date fallback behavior |
| `tradepilot/api/workflow.py` | Workflow route contracts | Need to preserve API surface while changing payload semantics |
| `webapp/src/pages/Dashboard/index.tsx` | Current workflow-first dashboard | Need to know which summary fields the UI currently consumes |
| `webapp/src/services/api.ts` | Frontend workflow API wrappers | Need to confirm no extra route changes are required in this phase |

**Tasks:**
- [ ] Redesign `WorkflowSummary` to match briefing sections
- [ ] Introduce placeholder-friendly schema blocks for post and pre briefing
- [ ] Preserve minimal backward compatibility where necessary
- [ ] Ensure workflow API responses return the new summary shape

### Phase 2: Build Post-market Briefing

**Required Reading:**
| File | Purpose | Why Needed |
|------|---------|------------|
| `tradepilot/workflow/service.py` | Existing post-market workflow logic | Need to replace `DailyScanner.run()` as the main post-market path |
| `tradepilot/ingestion/service.py` | Market/news sync entrypoints | Need to keep market sync as upstream data input |
| `tradepilot/scanner/daily.py` | Existing scanner behavior and reusable helpers | Need to identify which scanner pieces can still be reused temporarily |
| `tradepilot/db.py` | Tables for workflow, alerts, portfolio, market stats | Need to know what persisted data is already available and what must be added |
| `tradepilot/api/summary.py` | Watchlist storage/loading | Need watch sectors and watch stocks as briefing inputs |
| `tradepilot/summary/service.py` | Existing market snapshot logic | Need to determine if parts can be reused for market_overview |
| `/Users/lhh/Github/The-One/briefings/2026-04-01-post.md` | Reference post briefing output | Need target structure and level of detail |
| `/Users/lhh/Github/The-One/briefings/2026-04-04-post.md` | Reference post briefing output | Need alternate regime example and section stability |

**Tasks:**
- [ ] Build `market_overview`
- [ ] Build `sector_positioning` using current leaders + watch sectors
- [ ] Build `position_health`
- [ ] Build `next_day_prep`
- [ ] Remove `DailyScanner.run()` from the main post workflow path
- [ ] Persist post briefing as the canonical next-day carry-over source

### Phase 3: Build Pre-market Briefing

**Required Reading:**
| File | Purpose | Why Needed |
|------|---------|------------|
| `tradepilot/workflow/service.py` | Existing pre-market workflow logic | Need to rebuild pre workflow around post carry-over |
| `tradepilot/ingestion/service.py` | News sync path | Need to reuse overnight news ingestion |
| `tradepilot/api/summary.py` | Watchlist API and persisted watchlist loader | Need today watchlist inputs |
| `tradepilot/scanner/daily.py` | Alerts and positions helper access | Need to reuse alerts and position-loading patterns if still needed |
| `/Users/lhh/Github/The-One/briefings/2026-04-01-pre.md` | Reference pre briefing output | Need target structure for recap/news/watchlist/action sections |
| `/Users/lhh/Github/The-One/briefings/2026-04-04-pre.md` | Reference pre briefing output | Need section stability and richer mapping example |

**Tasks:**
- [ ] Build `yesterday_recap` from the latest post briefing
- [ ] Build `overnight_news` from synced news
- [ ] Build `today_watchlist`
- [ ] Build `action_frame`
- [ ] Ensure pre briefing explicitly inherits previous post-market conclusions

### Phase 4: Remove Parallel Products and Rebuild Dashboard UI

**Required Reading:**
| File | Purpose | Why Needed |
|------|---------|------------|
| `webapp/src/App.tsx` | Route and navigation setup | Need to hide/remove legacy pages from navigation |
| `webapp/src/pages/Dashboard/index.tsx` | Main daily workflow page | Need to rewrite layout around briefing sections |
| `webapp/src/services/api.ts` | Frontend API surface | Need to remove dead exports and keep briefing-first API usage |
| `tradepilot/main.py` | Backend router registration | Need to unload legacy routers once briefing path is ready |
| `tradepilot/api/analysis.py` | Legacy analysis routes | Need to determine removal scope |
| `tradepilot/api/signal.py` | Legacy signal routes | Need to determine removal scope |
| `tradepilot/api/trade_plan.py` | Legacy trade plan routes | Need to determine removal scope |
| `tradepilot/api/market.py` | Legacy market routes | Need to determine removal scope |
| `tradepilot/api/summary.py` | Summary routes including daily/5m/watchlist/trading-status | Need to keep watchlist/trading-status while de-emphasizing daily/5m |
| `webapp/src/pages/MarketSummary/index.tsx` | Legacy market overview page | Need to remove or hide it |
| `webapp/src/pages/StockAnalysis/index.tsx` | Legacy stock analysis page | Need to remove or hide it |
| `webapp/src/pages/SectorMap/index.tsx` | Legacy sector page | Need to remove or hide it |
| `webapp/src/pages/TradePlan/index.tsx` | Legacy trade plan page | Need to remove or hide it |

**Tasks:**
- [ ] Rewrite Dashboard to render post/pre briefing sections directly
- [ ] Hide/remove legacy navigation entries
- [ ] Unload legacy routers from `main.py`
- [ ] Keep only `workflow`, `scheduler`, `watchlist`, and `trading-status` as main product APIs
- [ ] Clean dead frontend API exports

### Phase 5: Define and Introduce Lightweight Intraday Watch

**Required Reading:**
| File | Purpose | Why Needed |
|------|---------|------------|
| `tradepilot/api/summary.py` | Existing trading-status and 5m routes | Need to reuse or repurpose them as intraday inputs |
| `tradepilot/summary/service.py` | Current intraday 5m brief logic | Need to identify reusable intraday watch logic |
| `webapp/src/pages/Dashboard/index.tsx` | Main workflow page | Need to know where intraday tab/section will live later |
| `/Users/lhh/Github/The-One/.claude/skills/the-one/skills/daily-workflow/workflow/pipelines/pre.py` | Reference pre pipeline | Need to understand expected carry-over into intraday validation |
| `/Users/lhh/Github/The-One/.claude/skills/the-one/skills/daily-workflow/workflow/pipelines/post.py` | Reference post pipeline | Need to understand market context continuity |

**Tasks:**
- [ ] Define `intraday_watch` payload in code
- [ ] Reframe `summary/5m` as workflow-intraday data source rather than parallel product
- [ ] Focus intraday view on premarket validation, position monitoring, and theme tracking

## Open Questions

- [ ] 第一阶段是否隐藏 `Portfolio` 独立页面，只保留后端持仓数据读写？
- [ ] `briefing` API 是否保留仅 alerts/read 兼容，还是直接内收进 workflow？
- [ ] `market_overview` v1 是否先只做 index + breadth + market_daily_stats，再补 style/limit/risk proxies？
- [ ] `overnight_news` v1 是否先做 highlights + categorized，news→sector mapping 作为后续增强？
- [ ] Phase 2 中 `sector_positioning` 的数据源是否足够稳定支撑“当日主线 + 观察池”，还是需要先用占位实现？

## Session Log

- **2026-04-06**: 完成 briefing-first 重构方向讨论。明确 TradePilot 第一优先服务主观波段交易，不做回测/研究平台。确定 post/pre briefing 的最小可用章节结构。
- **2026-04-06**: 参考 `/Users/lhh/Github/The-One/briefings`，确认盘后核心输出为市场大势、板块定位、持仓健康度、明日准备；盘前核心输出为昨日复盘摘要、隔夜信息、今日关注清单、操作计划。
- **2026-04-06**: 形成完整实施切分：先改 summary/schema，再做 post briefing，再做 pre briefing，最后下线旧页面与旧路由。
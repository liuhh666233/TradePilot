# V1 Schema Design

## Purpose

This document turns the frozen ETF all-weather v1 research boundary into an implementation-ready schema plan.

中文说明：
- 这份文档不是研究讨论稿，而是 `ETF all-weather v1` 的正式数据设计说明。
- 它的职责是把已经冻结的研究边界，翻译成可以直接编码、落表、验数、回溯的数据合同。
- 后续实现、测试、回测、前端展示，都应该以下面的 schema 和数据分层为准，而不是在代码里各自发明一套口径。

It answers five questions:

1. what data entities must exist in stage 1
2. which tables belong in DuckDB vs Parquet
3. what the canonical grain and keys are for each dataset
4. how release and effective dates survive into storage
5. what the first executable DDL should create now

中文说明：
- 简单说，这份文档要回答五件事：
- 第一，阶段一到底要存哪些数据。
- 第二，哪些数据应该放在 DuckDB，哪些应该放在 Parquet。
- 第三，每一类数据的最小粒度和唯一键是什么。
- 第四，慢变量的发布时间和生效时间如何进入数据层。
- 第五，第一批真正要执行的 DDL 到底应建哪些表。

This document is downstream of the frozen research contract and should not casually reopen it.

中文说明：
- 这份设计稿是下游实现文档，不负责重新争论：
- sleeve 是否要换
- 再平衡日期是否要改
- ETF 回报口径是否改回 raw close
- 是否引入更多资产、更多宏观因子、更多 ML

Primary upstream contracts:

- `../etf-all-weather-data-sources/developer-handoff-summary.md`
- `../etf-all-weather-data-sources/v1-canonical-field-list.md`
- `../etf-all-weather-data-sources/release-date-rules-v1-slow-fields.md`
- `../etf-all-weather-data-sources/monthly-rebalance-date-rule-note.md`
- `implementation-blueprint.md`

## Non-Negotiable Inputs

The schema must preserve these already-earned boundaries:

1. Canonical v1 sleeves:
   - `510300.SH`
   - `159845.SZ`
   - `511010.SH`
   - `518850.SH`
   - `159001.SZ`
2. Monthly decision clock:
   - first open trading day on or after the 20th calendar day of each month
3. Adjustment-aware ETF return semantics:
   - raw close is not sufficient as the research basis
4. Slow-field timing discipline:
   - `release_date`
   - `effective_date`
   - `revision_note`
   - `definition_regime` and `regime_note` for M1-family data
5. Stage-1 architecture boundary:
   - `raw -> normalized -> derived`
   - immutable raw retention
   - canonical normalized storage before strategy logic

中文说明：
- 这些约束已经在 research 阶段付出过成本，不应在 schema 阶段被悄悄破坏。
- 尤其要注意三件事：
- 第一，5 个 sleeve 已经冻结，不能在实现里临时换成别的 ETF。
- 第二，月度调仓规则已经冻结，不能在 notebook 里各写各的 date rule。
- 第三，慢变量必须带时间语义，不能只存数值不存 `effective_date`。

## Design Principles

1. Separate control plane from fact storage.
   - DuckDB should hold small transactional metadata, dimensions, and stable lookup tables.
   - Large time-series facts should stay in partitioned Parquet and be queried from DuckDB.
2. Store slow fields as long-form facts.
   - Heterogeneous timing metadata makes a wide table too brittle for stage 1.
3. Use canonical dataset grains that match verification.
   - daily market data verifies at `instrument_code + trade_date`
   - slow fields verify at `field_name + period_label + source_name`
4. Preserve source lineage.
   - normalized rows must be traceable back to raw landed batches
5. Prefer explicit caveats over fake precision.
   - where source timing or revision history is imperfect, record the caveat in schema fields rather than hiding it in code comments

中文说明：

### Principle 1: control plane 和事实数据分离

- `DuckDB` 负责小而稳定、需要频繁更新和查询的控制平面数据。
- `Parquet` 负责大体量时序事实数据。
- 这样可以避免把所有历史行情、慢变量、曲线点都硬塞进一个本地 DB 文件里，导致后期迁移和重建困难。

### Principle 2: 慢变量优先长表，不优先宽表

- `official_pmi`、`ppi_yoy`、`m1_yoy`、`tsf_yoy` 这种字段，在 timing metadata 上差异很大。
- 如果一开始就做成宽表，后续会不断在列上打补丁。
- 长表更适合做：
  - `field_name`
  - `period_label`
  - `release_date`
  - `effective_date`
  - `revision_note`

### Principle 3: 粒度必须先服务于校验，再服务于分析

- 如果数据粒度无法支持重复值检查、缺口检查、时间泄漏检查，那么这个 schema 就不合格。
- 所以每类数据都要先定义：最小业务粒度是什么，唯一性检查如何做。

### Principle 4: 每条 canonical 数据都必须能追溯到 raw

- 后面如果发现某个字段不对，必须能回到 raw payload 复盘。
- 如果 normalized 层和 raw 层没有血缘关系，问题就只能靠猜。

### Principle 5: 明示 caveat，不要伪装成“精准历史真相”

- 比如 `latest_history_only`、`revision-risk-present`、`curve extraction risk present` 这些 caveat，应该进入字段或文档，而不是藏在注释里。

## System Modules

This schema is not just a table layout. It also maps to implementation modules.

中文说明：
- 下面这一节是给开发时对齐模块边界用的。
- 目标是让“数据放在哪里”和“代码逻辑放在哪里”一一对应，避免后期 service、notebook、脚本互相越界。

### Module 1: Raw ingestion module

Responsibility:
- fetch data from upstream source
- persist immutable landed payloads
- register raw batches

中文注释：
- 这个模块只负责“拿到原始数据并落地”。
- 不在这个模块里做策略解释、特征构造、权重计算。
- 输出应尽量保留源数据形态，方便复盘。

### Module 2: Normalization module

Responsibility:
- map raw source fields into canonical contracts
- attach identifiers, units, timing metadata, and quality flags
- write partitioned Parquet facts

中文注释：
- 这是整个数据系统的核心清洗层。
- 它负责把 Tushare、AKShare、官方源等不一致的字段，归一成统一合同。
- 这里可以做字段重命名、类型转换、时间语义补足、质量标记。
- 但这里不做策略判断。

### Module 3: Derived feature module

Responsibility:
- consume normalized facts
- build as-of feature panels and explainability payloads
- preserve source lineage to underlying runs and batches

中文注释：
- 这里是 “feature snapshot / regime snapshot / allocation snapshot” 所在层。
- 这一层应该是可审计的数据产品层，而不是隐藏在 notebook 里的临时逻辑。

### Module 4: Strategy state module

Responsibility:
- score regime
- compute confidence
- propose target budgets

中文注释：
- 这是规则化状态层。
- 只负责做透明的状态判断和预算建议。
- 不应直接跳到交易指令。

### Module 5: Weighting and backtest module

Responsibility:
- convert budgets into weights
- run backtests or shadow allocations

中文注释：
- 这一层还不是当前文档的主角，但 schema 设计要为它预留空间。

## Physical Storage Plan

Recommended root:

- `data/etf_all_weather/`

Recommended zones:

- `data/etf_all_weather/raw/`
- `data/etf_all_weather/normalized/`
- `data/etf_all_weather/derived/`

中文说明：
- `raw/` 存原始抓取结果，不可变。
- `normalized/` 存 canonical 合同数据，是研究和策略的正式输入层。
- `derived/` 存由 normalized 进一步构造出的月度面板、状态、预算、权重等结果。

Storage split:

| Dataset family | Physical home | Reason |
|---|---|---|
| ingestion manifests and quality logs | DuckDB | small, mutable, transactional |
| reference dimensions | DuckDB | tiny tables, frequent joins |
| trading and rebalance calendars | DuckDB | foundational lookup tables |
| daily market facts | Parquet | append-heavy history |
| daily rates facts | Parquet | append-heavy history |
| slow macro facts | Parquet | long-form heterogeneous timing metadata |
| curve points | Parquet | potentially large point history |
| monthly derived feature snapshots | Parquet | rebuildable research output |
| monthly derived regime snapshots | Parquet | rebuildable state layer output |

中文说明：

### 为什么 DuckDB 只放 control plane 和小维表

- 这类数据体量小，但查询频繁。
- 例如：run history、watermark、calendar、canonical sleeve registry。

### 为什么时序事实优先 Parquet

- 行情、慢变量、曲线点会越来越大。
- Parquet 更适合按时间分区、重建、回测扫描。
- DuckDB 适合作为查询入口，而不是长期承载所有大表的唯一存储。

## Directory-Level Annotation

Below is the recommended directory meaning.

### `data/etf_all_weather/raw/`

中文注释：
- 原始层。
- 每个文件都应该能回答“这是哪个源、哪个窗口、哪个批次抓回来的”。
- 命名中建议显式带：
  - dataset family
  - source
  - date window
  - raw batch id

Typical children:
- `trade_calendar/`
- `sleeve_daily_market/`
- `benchmark_index_daily/`
- `slow_macro/`
- `curve/`

### `data/etf_all_weather/normalized/`

中文注释：
- 规范化层。
- 所有后续研究默认只应读这一层，而不是直接读 raw。

Typical children:
- `daily_market/`
- `daily_rates/`
- `slow_fields/`
- `curve/`

### `data/etf_all_weather/derived/`

中文注释：
- 派生层。
- 这一层的每个数据集都应该是“可解释、可复用、可追溯”的中间产品。

Typical children:
- `monthly_feature_snapshot/`
- `monthly_regime_snapshot/`
- later: `monthly_allocation_snapshot/`

## Canonical Dataset Inventory

### A. DuckDB Control-Plane Tables

中文总注释：
- 这部分是“控制平面”，负责记录运行过程、批次血缘、校验结果、水位状态。
- 它不承载大体量历史行情，但承载整个系统的可追溯性。

#### 1. `etf_aw_ingestion_runs`

Role:
- one row per ingestion or normalization job execution

中文注释：
- 每次执行一个同步或构建任务，都应该有一条 run 记录。
- 不管任务成功失败，都应留下痕迹。

Primary key:
- `run_id`

Why it exists:
- job traceability
- success/failure accounting
- replay visibility

Recommended semantics:
- `job_name`: 任务名，例如 `trading_calendar_sync`
- `dataset_name`: 影响的数据集族，例如 `slow_macro`
- `source_name`: 上游数据源，例如 `tushare`
- `status`: 运行状态
- `records_discovered / inserted / failed`: 行数级运行摘要

#### 2. `etf_aw_raw_batches`

Role:
- manifest for immutable raw landed files

中文注释：
- 这是 raw 文件的登记簿。
- 原始文件真正存放在 filesystem 中，这张表负责记住：
  - 文件路径
  - 来源
  - 时间窗口
  - hash
  - 属于哪个 run

Primary key:
- `raw_batch_id`

Important uniqueness:
- storage path should be unique per landed artifact

Why it exists:
- raw lineage
- replay from landed payloads without refetching

#### 3. `etf_aw_validation_results`

Role:
- audit output for data checks at run, batch, and dataset level

中文注释：
- 所有数据检查结果都应显式落这里，而不是只打印日志。
- 例如：
  - duplicate check
  - null check
  - required tenor check
  - effective date presence check

Primary key:
- `validation_id`

Why it exists:
- quality status should be inspectable rather than implicit

#### 4. `etf_aw_source_watermarks`

Role:
- remembers latest available and latest fetched boundary per dataset/source

中文注释：
- 这张表用来回答“这个数据同步到哪一天了”。
- 后续做增量同步、定时任务和监控时非常关键。

Primary key:
- `(dataset_name, source_name)`

Why it exists:
- incremental sync control
- operational visibility

### B. DuckDB Reference Tables

中文总注释：
- 这部分是“小而稳定”的维表。
- 它们不是大历史事实表，但为所有查询提供统一引用关系。

#### 5. `canonical_sleeves`

Role:
- frozen v1 sleeve registry

中文注释：
- 这是 v1 sleeve 名册，是策略资产层的根定义。
- 所有与 sleeve 相关的逻辑，都应先能映射到这里。

Primary key:
- `sleeve_code`

Important fields:
- `sleeve_role`
- `benchmark_name`
- `exposure_note`
- `is_active`

Field annotation:
- `sleeve_role`: 角色，例如大盘权益、小盘权益、债券、黄金、现金
- `benchmark_name`: 该 ETF 对应的 benchmark/exposure 口径
- `exposure_note`: 对暴露特征的人工注释，用于提醒 instrument caveat

#### 6. `canonical_instruments`

Role:
- broader reference registry for sleeves and confirmation instruments

中文注释：
- 这张表比 `canonical_sleeves` 更泛化。
- sleeve、benchmark index、rate series、macro series 都可以在这里登记。

Primary key:
- `instrument_code`

Important fields:
- `instrument_type`
- `exchange`
- `benchmark_name`
- `metadata_json`

#### 7. `canonical_trading_calendar`

Role:
- canonical open-day table

中文注释：
- 这是整个系统的时间锚。
- 没有这张表，所有 `release_date -> effective_date -> rebalance_date` 的逻辑都会变脆。

Primary key:
- `(exchange, trade_date)`

Important fields:
- `is_open`
- `pretrade_date`
- `calendar_source`

#### 8. `canonical_rebalance_calendar`

Role:
- materialized v1 monthly decision schedule

中文注释：
- 这张表把研究中的“每月 20 日及之后的第一个开盘日”落成真实数据。
- 后续所有 snapshot、score、budget、weight 都应该依赖这张表，而不是在不同模块里重复算日期。

Primary key:
- `rebalance_date`

Important fields:
- `calendar_month`
- `rule_name`
- `anchor_day`
- `previous_rebalance_date`

### C. Parquet-Backed Canonical Facts

These are canonical normalized contracts. In stage 1 they should live as partitioned Parquet datasets and be queried through DuckDB views later.

中文总注释：
- 下面这些表名更准确地说是“逻辑表”，物理上先落在 Parquet 分区中。
- 它们是研究和派生层的正式输入合同。

#### 9. `canonical_daily_market_fact`

Grain:
- one instrument per trade date

中文注释：
- 最小粒度是“一个 instrument 在一个 trade_date 上的一行”。
- 这张表同时���载：
  - 5 个 sleeve
  - benchmark indexes
  - 以后如有需要，也可承载其他日频确认资产

Logical key:
- `(instrument_code, trade_date)`

Required columns:
- `instrument_code`
- `trade_date`
- `open`
- `high`
- `low`
- `close`
- `adj_close`
- `pct_chg`
- `adj_pct_chg`
- `vol`
- `amount`
- `source_name`
- `source_trade_date`
- `raw_batch_id`
- `quality_status`

Field annotation:
- `adj_close`: 调整后收盘价，是 ETF canonical return 口径的重要基础
- `adj_pct_chg`: adjustment-aware return 派生字段
- `source_trade_date`: 上游源自己声明的日期，用于和 canonical `trade_date` 核对
- `quality_status`: 当前行质量标记，不等于最终是否可用于策略

Partition recommendation:
- `dataset_year=YYYY/dataset_month=MM`

Module ownership:
- raw ingestion module: fetch sleeve/index source payloads
- normalization module: map to canonical daily market fact
- derived feature module: consume as-of market rows

#### 10. `canonical_daily_rates_fact`

Grain:
- one field per trade date per source

中文注释：
- 这张表面向日频利率、流动性类字段，例如 Shibor、LPR 等。
- 当前项目已为它预留合同，但可按实现顺序逐步接入。

Logical key:
- `(field_name, trade_date, source_name)`

Required columns:
- `field_name`
- `trade_date`
- `value`
- `unit`
- `source_name`
- `raw_batch_id`
- `revision_note`
- `quality_status`

Partition recommendation:
- `dataset_year=YYYY/dataset_month=MM`

Module ownership:
- raw ingestion module: fetch rates source
- normalization module: standardize units and dates
- derived feature module: select latest published value up to rebalance date

#### 11. `canonical_slow_field_fact`

Grain:
- one field per period label per source

中文注释：
- 这是慢变量核心长表。
- 每个字段、每个月份、每个来源，形成一行。
- 它不是宽表，不要求一行同时装下所有 macro fields。

Logical key:
- `(field_name, period_label, source_name)`

Required columns:
- `field_name`
- `period_label`
- `period_type`
- `value`
- `unit`
- `release_date`
- `effective_date`
- `revision_note`
- `definition_regime`
- `regime_note`
- `source_name`
- `raw_batch_id`
- `quality_status`

Field annotation:
- `period_label`: 这条数据所属的统计月份或季度，例如 `2026-03`
- `release_date`: 保守发布时间，不一定等于源里显式字段
- `effective_date`: 策略第一次允许使用这条数据的日期
- `revision_note`: 是否只有最新修订历史、是否存在 revision risk
- `definition_regime`: 例如 M1 在 2025 前后的语义制度分段
- `regime_note`: 对制度分段的文字说明

Partition recommendation:
- `field_name=<field>/dataset_year=YYYY`

Critical timing rule:
- a slow field may enter strategy features only when `effective_date <= rebalance_date`

Module ownership:
- raw ingestion module: fetch PMI/PPI/M1/M2/TSF payloads
- normalization module: attach conservative timing metadata
- derived feature module: choose latest effective row as of rebalance date

#### 12. `canonical_curve_fact`

Grain:
- one curve tenor point per curve date per source

中文注释：
- 这张表记录曲线点，而不是先只保留 1Y 和 10Y。
- 这样可以保留后续复盘、验证、重新抽取 slope 的能力。

Logical key:
- `(curve_code, curve_date, tenor_years, source_name)`

Required columns:
- `curve_code`
- `curve_date`
- `curve_type`
- `tenor_years`
- `yield_value`
- `source_name`
- `raw_batch_id`
- `quality_status`

Field annotation:
- `curve_code`: 上游曲线标识，如 `1001.CB`
- `curve_type`: 曲线类别，需要显式记录，不能默认忽略
- `tenor_years`: 曲线期限点，例如 `1.0`, `10.0`
- `yield_value`: 对应期限收益率

Partition recommendation:
- `dataset_year=YYYY/dataset_month=MM`

Module ownership:
- raw ingestion module: windowed or paged `yc_cb` extraction
- normalization module: canonicalize date, tenor, yield
- derived feature module: select latest 1Y/10Y and compute slope

### D. Derived Dataset Reservation

中文总注释：
- 这一层不是原始事实，而是“解释性中间产品”。
- 它们应该是可审计、可复用、可回放的数据产品，而不是 notebook 临时变量。

#### 13. `monthly_feature_snapshot`

Role:
- one explainability-ready monthly as-of feature payload per rebalance date

中文注释：
- 每个 `rebalance_date` 一行。
- 目标是回答：在这一天，系统实际看到了哪些已知特征。

Grain:
- one row per `rebalance_date`

Reserved fields:
- `rebalance_date`
- `schema_version`
- `feature_payload_json`
- `source_run_set_json`
- `created_at`

Field annotation:
- `feature_payload_json`: 真正的月度 as-of 特征载荷
- `source_run_set_json`: 这条 snapshot 依赖了哪些上游 batch/run

#### 14. `monthly_regime_snapshot`

Role:
- one rule-based regime and confidence payload per rebalance date

中文注释：
- 这是状态判断层。
- 目标是记录：
  - regime score
  - confidence
  - target risk budgets
- 它不是最终权重，也不是交易执行指令。

Grain:
- one row per `rebalance_date`

Reserved fields:
- `rebalance_date`
- `schema_version`
- `regime_payload_json`
- `source_run_set_json`
- `created_at`

#### 15. `monthly_allocation_snapshot` (reserved)

Role:
- one allocation decision row per rebalance date after weighting layer is added

中文注释：
- 这张表当前还未实现，但 schema 设计必须预留它的位置。
- 它应连接 `target_risk_budgets` 和最终 `weights`。

Expected fields later:
- `rebalance_date`
- `schema_version`
- `allocation_payload_json`
- `source_run_set_json`
- `created_at`

## Canonical Column Rules

### Enumerations That Should Stay Stable

Suggested constrained value families:

- `sleeve_role`
  - `equity_large`
  - `equity_small`
  - `bond`
  - `gold`
  - `cash`
- `instrument_type`
  - `etf`
  - `index`
  - `rate_series`
  - `macro_series`
  - `yield_curve`
- `quality_status`
  - `ok`
  - `warning`
  - `error`
  - `stale`
- `status` for jobs
  - `running`
  - `success`
  - `failed`
  - `partial`

中文说明：
- 这些枚举值应尽量保持稳定。
- 稳定枚举可以减少后续 notebook、API、前端、校验脚本的兼容负担。

### Slow-Field Timing Rules

Every slow-field record must preserve:

- `period_label`
- `release_date`
- `effective_date`
- `revision_note`

Additional requirement for M1-family data:

- `definition_regime`
- `regime_note`

Example interpretation:

- `period_label = 2026-03`
- `release_date = 2026-04-15`
- `effective_date = next open trading day on or after 2026-04-15`
- `definition_regime = post_2025_m1_definition`

中文说明：
- 这部分是全系统最不可放松的规则。
- 没有 `effective_date`，慢变量就无法防止未来泄漏。

### Return-Semantics Rule

Daily market facts must preserve enough information to support adjustment-aware return research.

Minimum expectation in normalized storage:

- `close`
- `adj_close`
- `pct_chg`
- `adj_pct_chg`

If the upstream source does not expose both raw and adjusted forms directly, the normalization code must document how the adjusted basis is derived.

中文说明：
- 对 ETF 系统来说，`adj_close` 不是可有可无。
- raw close 可以作为验证字段保留，但不能偷换成 canonical return 定义。

## Current Module-to-Dataset Mapping

This table describes where each current implemented module writes.

| Module / Job | Raw output | Normalized output | Derived output | 中文注释 |
|---|---|---|---|---|
| trading calendar sync | `raw/trade_calendar/` | DuckDB `canonical_trading_calendar` | DuckDB `canonical_rebalance_calendar` | 时间锚层 |
| sleeve daily market sync | `raw/sleeve_daily_market/` | `normalized/daily_market/` | none | 5-sleeve 日频合同 |
| benchmark index sync | `raw/benchmark_index_daily/` | `normalized/daily_market/` | none | 市场确认基础输入 |
| slow macro sync | `raw/slow_macro/` | `normalized/slow_fields/` | none | 带时间语义的慢变量长表 |
| curve sync | `raw/curve/` | `normalized/curve/` | none | windowed curve points |
| monthly feature snapshot build | none | consumes normalized only | `derived/monthly_feature_snapshot/` | 月度 as-of 特征面板 |
| monthly regime snapshot build | none | consumes derived feature snapshot | `derived/monthly_regime_snapshot/` | 月度状态与预算建议 |

## First Executable Scope

The first executable schema draft should create now:

1. DuckDB control-plane tables
2. DuckDB reference tables
3. optional DuckDB views that will later point at Parquet datasets when paths exist
4. canonical sleeve seed rows

It should not yet try to materialize all Parquet-backed fact datasets into DuckDB base tables.

Reason:

- stage 1 wants long-history facts in filesystem-backed partitions
- forcing those into app-local DuckDB tables too early would recreate the exact monolithic storage pattern the design is trying to avoid

中文说明：
- DDL 先只负责建 control plane 和 reference 层。
- normalized / derived 的大事实数据先落 Parquet，这是当前阶段最稳的做法。

## Recommended File Outputs

This milestone should produce two artifacts:

1. this design note
2. a DuckDB DDL draft:
   - `ddl-v1-schema.sql`

That SQL file should be safe to run repeatedly and should initialize the control-plane tables and reference dimensions.

中文说明：
- 一份是“解释为什么这样设计”的文档。
- 一份是“真正执行建表”的 SQL。
- 设计文档和 SQL 应长期同步演进，不应分叉。

## First Validation Checklist

Before coding the ingestion module, the schema draft is good enough if all of the following are true:

1. every frozen v1 sleeve has a stable home in `canonical_sleeves`
2. every slow field has a schema path for `release_date` and `effective_date`
3. raw batches can be traced to runs and normalized facts
4. rebalance dates are materialized explicitly rather than inferred ad hoc in notebooks
5. Parquet-backed fact datasets have defined grain, path, and partition strategy
6. curve data has a dedicated schema path instead of being hidden inside generic rates logic

中文说明：
- 如果上面 6 条做不到，这个 schema 还不算可落地。
- 尤其第 2 条和第 4 条，是防止未来泄漏的底线。

## Current Completion Status

As of the current implementation state, the following have already landed against this schema direction:

1. trading calendar and rebalance calendar
2. five-sleeve daily market facts
3. benchmark index daily facts
4. slow macro facts with timing metadata
5. curve facts with windowed extraction
6. monthly feature snapshot
7. monthly regime snapshot

中文说明：
- 这说明文档不再只是规划，它已经部分被代码实现验证过。
- 后续新增模块时，应继续沿用这份合同，而不是另起炉灶。

## Practical Next Steps

After this schema reference, the natural implementation order should be:

1. keep normalized contracts stable
2. add `monthly_allocation_snapshot`
3. add weighting logic with bounded risk budgets
4. add notebook and backtest scaffold consuming derived datasets
5. add dashboard read model only after the research outputs stabilize

中文说明：
- 下一阶段最自然的是从 `target_risk_budgets` 进入 `allocation snapshot`。
- 不建议先跳到复杂优化器或前端展示。

## Anti-Error Notes

These are the most likely schema misuse patterns.

1. reading raw directly in notebooks
   - wrong because raw is source-shaped, not canonical
2. ignoring `effective_date` and using `period_label` as if it were tradable date
   - wrong because this reintroduces future leakage
3. treating derived snapshots as if they were raw truth
   - wrong because they are explainability products, not source facts
4. silently changing field semantics in code without updating this document
   - wrong because downstream audits and tests lose contract stability

中文说明：
- 最常见错误不是 SQL 写错，而是层级边界被悄悄破坏。
- 后续任何模块，如果绕开 `normalized` 或绕开 `effective_date`，都应该视为高风险修改。

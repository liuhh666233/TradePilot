---
title: "Data Ingestion Stage C"
status: draft
mode: "design"
created: 2026-04-29
updated: 2026-04-29
modules: ["backend"]
---

# Data Ingestion Stage C

## Overview

本文档是 `data-ingestion-stage-b.md` 之后的下一阶段设计，结合了 Stage B 真实数据测试结果。

Stage B 已经证明 `tradepilot/etl/` 的首条真实纵切链路可用：Tushare source adapter、raw Parquet landing、normalized Parquet 分区重写、DuckDB metadata、dependency preflight、validation gating 和 watermark advancement 都已经在真实 Tushare 数据上跑通。

Stage C 的职责不是重新设计 ingestion engine，而是在 Stage B 已稳定的通用 ETL foundation 上，推进 ETF all-weather v1 所需的 serious data panel。

Stage C 的正确目标是：

> ETF all-weather v1 拥有一套可回放、可校验、无明显未来函数风险的 normalized / derived data panel；后续 notebook、backtest、shadow portfolio 只能从这套 panel 消费数据，不能直接调用 Tushare 或临时 source wrapper。

## Stage B Real-Data Findings Incorporated

Stage B 真实测试带来的设计约束如下。

1. Tushare catalog 不是严格干净的 canonical universe。
   - `fund_basic` 会返回 7 位 delisted fund code，例如 `5012011.SH`
   - raw 层必须保留这些 source rows
   - canonical 层必须过滤到 Stage B/C 支持的六位 SH/SZ code

2. Tushare endpoint schema 不是完全稳定的。
   - `index_basic` 可能缺少 `delist_date`
   - Stage C 新增 endpoint 必须显式区分 required columns 和 optional columns

3. Empty payload gating 已经验证有效。
   - 无 token 或 source 空响应时，raw batch 可保留
   - canonical write 和 watermark advancement 必须被阻断
   - profile runner 不应在无 token 情况下批量制造无意义 empty runs

4. Exchange-aware calendar validation 已经可用。
   - Stage C 的 ETF all-weather rebalance calendar 必须考虑 SH/SZ 共同开市日
   - 不能只用单交易所 calendar 推导跨交易所 ETF universe 的月度 rebalance date

5. Normalized partition rewrite 已经可用。
   - Stage C 应直接复用 Stage B 的 deterministic merge-and-rewrite 语义
   - 不新增一套 ETF all-weather 专用 fact writer

6. Stage B 只真实验证了部分 instrument。
   - 已验证样例包括 `510300.SH`、`159915.SZ`、`000300.SH`
   - Stage C 不能假设五个 frozen sleeves 和 ZZ1000 benchmark 都已经可运行
   - 必须先做 C0 frozen universe probe

## Relationship With Stage B

Stage C 继续沿用 Stage B 的边界：

- 继续使用 `tradepilot/etl/` 作为统一数据接入 foundation
- 继续使用 `ETLService.run_dataset_sync()` 作为单 dataset 执行入口
- raw 继续 immutable append-only
- normalized 继续按分区 deterministic rewrite
- validation `fail` 继续阻断 canonical write 和 watermark advancement
- metadata 继续写入 `etl_*`
- reference 小表继续写 DuckDB
- 大历史事实继续写 Parquet
- 旧 `tradepilot/ingestion/` 路径继续并存，不在 Stage C 替换

Stage C 的新增点：

- 增加 ETF all-weather v1 profile runner，但它只是有序执行 profile，不是通用 DAG scheduler
- 增加 strategy-specific reference：frozen sleeves、rebalance calendar
- 增加 adjustment-aware ETF return basis
- 增加 slow-field timing model：`period_label`、`release_date`、`effective_date`、`revision_note`
- 增加 rates / curve dataset family
- 增加最小 derived feature panel
- 增加 rebalance-date as-of snapshot

## Stage C Goals

- [ ] 交付 C0 frozen universe real-data probe
- [ ] 固化 ETF all-weather v1 五个 sleeves
- [ ] 生成 SH/SZ 共同开市日口径的 `reference.rebalance_calendar`
- [ ] 为五个 sleeves 建立 adjustment-aware daily panel
- [ ] 补齐 HS300 / ZZ1000 benchmark daily panel
- [ ] 建立 slow macro tall fact 表
- [ ] 建立 Shibor / LPR / government curve normalized facts
- [ ] 生成 v1 最小 derived features
- [ ] 生成 rebalance-date as-of snapshot
- [ ] 提供 `etf_all_weather_v1` profile runner，用于顺序 bootstrap / backfill

## Stage C Non-Goals

Stage C 不包含以下内容：

- ETF allocation engine
- ERC optimizer
- 正式 backtest framework
- 前端页面
- portfolio write / trade action
- 全局 scheduler
- 跨进程 job graph
- AKShare 作为 ETF price 自动 fallback
- options、海外 overlay、CTA、full credit spread、breadth root system
- 大规模 instrument universe 扩展

## Slice Strategy

Stage C 不应一次性推进到完整 macro/rates/curve/panel。结合 Stage B 真实测试结果，建议按以下 slice 推进。

### Slice C0 - Frozen Universe Real-Data Probe

C0 是 Stage C 的第一道门。它只验证 ETF all-weather frozen universe 能否在 Stage B foundation 上真实运行。

必须实测以下 7 个 instrument：

| Role | Instrument | Dataset |
|------|------------|---------|
| equity_large | `510300.SH` | `market.etf_daily` |
| equity_small | `159845.SZ` | `market.etf_daily` |
| bond | `511010.SH` | `market.etf_daily` |
| gold | `518850.SH` | `market.etf_daily` |
| cash | `159001.SZ` | `market.etf_daily` |
| benchmark_hs300 | `000300.SH` | `market.index_daily` |
| benchmark_zz1000 | `000852.SH` | `market.index_daily` |

C0 验收：

- 7 个 instrument 都能进入 `canonical_instruments`
- 5 个 ETF sleeves 都能跑通 `market.etf_daily`
- 2 个 benchmark 都能跑通 `market.index_daily`
- SH / SZ calendar dependency 都能覆盖请求窗口
- repeated sync 不产生重复 business key
- 每个失败都保留 raw batch 和 validation result
- Stage B 全部测试继续通过

### Slice C1 - Sleeve Reference And Rebalance Calendar

C1 固化 ETF all-weather strategy-specific reference。

新增：

- `reference.etf_aw_sleeves`
- `reference.rebalance_calendar`

这一步只依赖 Stage B reference datasets 和 C0 universe probe。

### Slice C2 - Adjustment-Aware Sleeve Daily Panel

C2 在 Stage B `market.etf_daily` 基础上新增调整因子与策略 daily panel。

新增：

- `market.etf_adj_factor`
- `derived.etf_aw_sleeve_daily`

核心要求：

- `market.etf_daily` 不改变语义，仍然是 source raw-close market fact
- `derived.etf_aw_sleeve_daily` 才是 ETF all-weather 的 canonical return source
- 不允许在 adjustment factor 缺失时静默退回 raw close return

### Slice C3 - Market Confirmation Features

C3 补齐 benchmark 和 market confirmation。

输入：

- `market.index_daily` for `000300.SH`
- `market.index_daily` for `000852.SH`
- `derived.etf_aw_sleeve_daily`

输出：

- `derived.etf_aw_market_features`

最小字段：

- `hs300_vs_zz1000_20d`
- `bond_trend_20d`
- `gold_trend_20d`
- `realized_vol_20d_*`

### Slice C4 - Macro / Rates / Curve

C4 才进入 slow macro、rates、curve。

新增：

- `macro.slow_fields`
- `rates.daily_rates`
- `rates.lpr`
- `rates.gov_curve_points`

这部分复杂度最高，不能阻塞 C0-C3 的 market foundation。

### Slice C5 - Rebalance Snapshot

C5 生成 strategy-serving boundary。

新增：

- `derived.etf_aw_rebalance_snapshot`

它是 notebook MVP、backtest、shadow portfolio 的输入边界，但仍不包含 allocation engine。

## Dataset Scope

| Dataset | Type | Destination | Stage |
|---------|------|-------------|-------|
| `reference.etf_aw_sleeves` | strategy reference | DuckDB `canonical_etf_aw_sleeves` | C1 |
| `reference.rebalance_calendar` | derived reference | DuckDB `canonical_rebalance_calendar` | C1 |
| `market.etf_adj_factor` | normalized fact | Parquet | C2 |
| `derived.etf_aw_sleeve_daily` | strategy daily panel | Parquet | C2 |
| `derived.etf_aw_market_features` | derived daily features | Parquet | C3 |
| `macro.slow_fields` | normalized tall fact | Parquet | C4 |
| `rates.daily_rates` | normalized tall fact | Parquet | C4 |
| `rates.lpr` | normalized slow rate fact | Parquet | C4 |
| `rates.gov_curve_points` | normalized curve fact | Parquet | C4 |
| `derived.etf_aw_rebalance_snapshot` | strategy-serving snapshot | Parquet canonical artifact | C5 |

Naming rule:

- `reference.*` names are dataset registry keys.
- `canonical_*` names are DuckDB physical tables.
- `derived.*` Parquet datasets may optionally have DuckDB views for querying, but the Parquet artifact remains the canonical output.

## Stage C Profile Parameters

Stage C profile runs must make the backfill window explicit. No dataset should infer a hidden strategy window from wall-clock time.

Required profile parameters:

| Parameter | Type | Required | Meaning |
|-----------|------|----------|---------|
| `backfill_start` | date | yes | first strategy business date to publish |
| `backfill_end` | date | yes | last strategy business date to publish |
| `calendar_start` | date | yes | first date required for calendar dependencies |
| `calendar_end` | date | yes | last date required for calendar dependencies |
| `feature_lookback_days` | integer | yes | initial v1 fixed `20` |
| `warmup_trading_days` | integer | yes | minimum upstream history before `backfill_start`; initial v1 fixed `40` |

Rules:

- `calendar_start` must be early enough to cover `warmup_trading_days` before `backfill_start`.
- market and adjustment factor datasets should fetch from the warmup start, but derived strategy outputs should publish only rows with `trade_date >= backfill_start`.
- `backfill_end` must not be later than the latest fully covered SH/SZ common calendar date.
- C0 probe may use a short explicit window, but it must still pass these parameter checks.

## Frozen V1 Sleeve Contract

Stage C 必须编码 exactly 5 个 sleeves。

| Sleeve ID | Instrument ID | Sleeve Role |
|-----------|---------------|-------------|
| `equity_large` | `510300.SH` | large-cap equity |
| `equity_small` | `159845.SZ` | small-cap equity |
| `bond` | `511010.SH` | bond defense |
| `gold` | `518850.SH` | gold hedge |
| `cash` | `159001.SZ` | cash / neutral buffer |

这些不是示例配置，而是 ETF all-weather v1 canonical universe。

### `reference.etf_aw_sleeves`

Canonical destination: DuckDB `canonical_etf_aw_sleeves`。

Stage C should add this table rather than overloading the legacy `canonical_sleeves` table. `canonical_sleeves` does not carry the frozen ETF all-weather instrument mapping or validation fields needed here.

业务键：`sleeve_id`。

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `sleeve_id` | string | yes | `equity_large` / `equity_small` / `bond` / `gold` / `cash` |
| `instrument_id` | string | yes | 必须为冻结 instrument ID |
| `sleeve_role` | string | yes | 策略语义角色 |
| `display_order` | integer | yes | 稳定展示顺序 |
| `is_active` | boolean | yes | 当前 v1 是否启用 |
| `start_date` | date/null | no | 该 sleeve 在 v1 中的生效日期 |
| `end_date` | date/null | no | 该 sleeve 在 v1 中的结束日期 |
| `exposure_note` | string/null | no | 暴露说明，bond sleeve 必须保留 caveat |
| `updated_at` | timestamp | yes | 本地 upsert 时间 |

Validation：

- 必须且只能有 5 个 active sleeve
- `instrument_id` 必须存在于 `canonical_instruments`
- 每个 instrument 必须是 `instrument_type = etf`
- `exchange` 必须与 code suffix 一致
- `list_date <= backfill_start`
- `delist_date IS NULL OR delist_date > backfill_end`
- 不支持的 catalog source rows 只作为 source drift metric，不阻断五个 sleeve

Stable check names：

- `etf_aw_sleeves.frozen_universe_exact`
- `etf_aw_sleeves.instrument_exists`
- `etf_aw_sleeves.instrument_type_etf`
- `etf_aw_sleeves.exchange_suffix_match`
- `etf_aw_sleeves.listed_before_backfill`
- `etf_aw_sleeves.not_delisted_in_window`

## Rebalance Calendar

Stage C 的 rebalance calendar 必须基于 SH/SZ 共同开市日。

Rule：

> `rebalance_date_monthly = first date on or after the 20th calendar day where both SH and SZ are open`

Canonical destination: DuckDB `canonical_rebalance_calendar`。

业务键：`(calendar_name, rebalance_month)`。

Stage C must refine the existing table shape if needed so that `rebalance_month` is stored and the primary business key is `(calendar_name, rebalance_month)`. `rebalance_date` should remain unique per `calendar_name` for the initial monthly calendar, but it is not the primary business key.

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `calendar_name` | string | yes | 初版固定 `etf_aw_v1_monthly` |
| `rebalance_month` | string | yes | `YYYY-MM` |
| `rebalance_date` | date | yes | 当月 rebalance execution date |
| `rule_name` | string | yes | `first_common_open_day_on_or_after_20th` |
| `rule_version` | string | yes | 初版 `v1` |
| `required_exchanges` | string | yes | 初版 `SH,SZ` |
| `generated_at` | timestamp | yes | 本地生成时间 |

Validation：

- 每月一行
- `rebalance_date.day >= 20`
- `rebalance_date` 必须落在 `rebalance_month` 当月内
- `rebalance_date` 必须同时满足 `SH.is_open = true` 和 `SZ.is_open = true`
- 不存在更早的共同开市日满足规则
- 当月 SH calendar 覆盖完整
- 当月 SZ calendar 覆盖完整
- SH/SZ 日历冲突时生成失败，不降级为单交易所日历
- 如果当月 20 日后不存在 SH/SZ 共同开市日，则当月生成失败，不能滚动到下月

Stable check names：

- `rebalance_calendar.month_coverage`
- `rebalance_calendar.common_open_day`
- `rebalance_calendar.on_or_after_20th`
- `rebalance_calendar.same_month`
- `rebalance_calendar.first_eligible_day`
- `rebalance_calendar.duplicate_month`

## Adjustment-Aware ETF Panel

Stage B 的 `market.etf_daily` 继续保持 raw-close market fact 语义。Stage C 不改写它。

Stage C 新增：

- `market.etf_adj_factor`
- `derived.etf_aw_sleeve_daily`

### `market.etf_adj_factor`

建议 source：

- primary: Tushare `fund_adj`

Canonical destination:

- `data/lakehouse/normalized/market.etf_adj_factor/YYYY/MM/part-00000.parquet`

业务键：`(instrument_id, trade_date)`。

Canonical fields：

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `instrument_id` | string | yes | ETF instrument ID |
| `trade_date` | date | yes | adjustment factor date |
| `adj_factor` | double | yes | Tushare adjustment factor |
| `source_name` | string | yes | source adapter |
| `raw_batch_id` | bigint | yes | raw batch manifest ID |
| `ingested_at` | timestamp | yes | canonical write time |
| `quality_status` | string | yes | validation summary |

Validation：

- duplicate `(instrument_id, trade_date)` 为 `fail`
- instrument 必须存在且为 ETF
- `trade_date` 必须是 open trading day
- `adj_factor` 非空且大于 0
- five frozen sleeves 在请求窗口内必须有 factor 覆盖

Stable check names：

- `etf_adj_factor.duplicate_business_key`
- `etf_adj_factor.instrument_exists`
- `etf_adj_factor.instrument_type_etf`
- `etf_adj_factor.trade_date_open`
- `etf_adj_factor.factor_required`
- `etf_adj_factor.factor_positive`
- `etf_adj_factor.frozen_sleeve_coverage`

### `derived.etf_aw_sleeve_daily`

This dataset is derived only. It must not call source adapters.

Inputs：

- `market.etf_daily`
- `market.etf_adj_factor`
- `reference.etf_aw_sleeves`
- `canonical_trading_calendar`

Canonical destination：

- `data/lakehouse/derived/derived.etf_aw_sleeve_daily/YYYY/MM/part-00000.parquet`

业务键：`(sleeve_id, trade_date)`。

Canonical fields：

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `sleeve_id` | string | yes | frozen sleeve ID |
| `instrument_id` | string | yes | ETF instrument ID |
| `trade_date` | date | yes | trading date |
| `raw_close` | double | yes | close from `market.etf_daily` |
| `adj_factor` | double | yes | factor from `market.etf_adj_factor` |
| `adj_close` | double | yes | adjustment-aware price proxy |
| `adj_return_1d` | double/null | no | daily adjusted return |
| `open` | double/null | no | raw open |
| `high` | double/null | no | raw high |
| `low` | double/null | no | raw low |
| `volume` | double/null | no | source unit retained |
| `amount` | double/null | no | source unit retained |
| `return_basis` | string | yes | fixed `adjustment_aware` |
| `quality_status` | string | yes | validation summary |
| `upstream_run_ids` | string | yes | JSON list of upstream ETL run IDs used to build the row |
| `input_max_trade_date` | date | yes | latest upstream market date visible to the builder |
| `derived_at` | timestamp | yes | derived write time |

Rules：

- `adj_close = raw_close * adj_factor`
- `adj_return_1d = adj_close / previous_available_adj_close - 1`
- the first available row per sleeve has `adj_return_1d = NULL`
- `previous_available_adj_close` must come from the previous open trading date for that sleeve; missing previous adjusted close inside the requested non-warmup window is blocking
- no raw-close fallback is allowed if `adj_factor` is missing
- `return_basis` must always be `adjustment_aware`
- `adj_close` is a price proxy for return construction; downstream code should use returns or ratios, not compare absolute adjusted price levels across providers

Validation：

- five sleeves coverage by date
- no duplicate `(sleeve_id, trade_date)`
- no non-trading day rows
- `raw_close > 0`
- `adj_factor > 0`
- `adj_close > 0`
- missing factor blocks publish
- extreme `adj_return_1d` is warning unless caused by impossible factor / price state

Stable check names：

- `etf_aw_sleeve_daily.five_sleeve_coverage`
- `etf_aw_sleeve_daily.duplicate_business_key`
- `etf_aw_sleeve_daily.trade_date_open`
- `etf_aw_sleeve_daily.raw_close_positive`
- `etf_aw_sleeve_daily.adj_factor_positive`
- `etf_aw_sleeve_daily.adj_close_positive`
- `etf_aw_sleeve_daily.return_basis_adjustment_aware`
- `etf_aw_sleeve_daily.no_raw_close_fallback`
- `etf_aw_sleeve_daily.extreme_adjusted_return`

## Market Confirmation Features

Dataset: `derived.etf_aw_market_features`。

Inputs：

- `market.index_daily` for `000300.SH`
- `market.index_daily` for `000852.SH`
- `derived.etf_aw_sleeve_daily`

Canonical destination：

- `data/lakehouse/derived/derived.etf_aw_market_features/YYYY/MM/part-00000.parquet`

业务键：`trade_date`。

Minimum fields：

| Field | Role | Source |
|-------|------|--------|
| `hs300_close` | validation_only | `market.index_daily` |
| `zz1000_close` | validation_only | `market.index_daily` |
| `hs300_vs_zz1000_20d` | confirmatory | derived from benchmark closes |
| `bond_trend_20d` | confirmatory | derived from bond adjusted close |
| `gold_trend_20d` | confirmatory | derived from gold adjusted close |
| `realized_vol_20d_equity_large` | execution_only | derived from adjusted returns |
| `realized_vol_20d_equity_small` | execution_only | derived from adjusted returns |
| `realized_vol_20d_bond` | execution_only | derived from adjusted returns |
| `realized_vol_20d_gold` | execution_only | derived from adjusted returns |
| `realized_vol_20d_cash` | execution_only | derived from adjusted returns |
| `feature_window_complete` | validation | derived completeness flag |
| `missing_input_flags` | validation | explicit missing input flags |
| `upstream_run_ids` | lineage | JSON list of upstream ETL run IDs |
| `input_max_trade_date` | lineage | latest upstream trade date visible to builder |
| `derived_at` | lineage | derived write time |

Rules：

- derived features must use prior-close-only construction
- feature window must not include future rows relative to `trade_date`
- market confirmation fields may throttle or confirm, but must not define macro state alone
- missing inputs should produce explicit missing flags, not silent zero fill
- 20D features require 20 prior available observations before or on `trade_date`, depending on the formula-specific convention documented in tests
- if warmup history is insufficient, publish explicit null features and `feature_window_complete = false` for warmup-affected rows; do not fabricate zero or shortened-window values
- rows before `backfill_start` are warmup-only inputs and should not be published to the canonical feature panel

Initial feature formulas:

- `hs300_vs_zz1000_20d = hs300_return_20d - zz1000_return_20d`
- `bond_trend_20d = bond_adj_close / bond_adj_close_lag_20 - 1`
- `gold_trend_20d = gold_adj_close / gold_adj_close_lag_20 - 1`
- `realized_vol_20d_* = stddev(adj_return_1d over last 20 available returns) * sqrt(252)`
- return windows use trading-day lags, not calendar-day offsets
- `realized_vol_20d_*` requires 20 non-null one-day returns, which usually requires 21 adjusted close observations

## Slow Macro Fact

Dataset: `macro.slow_fields`。

Stage C should use one tall schema instead of one table per macro field.

Primary fields:

- `official_pmi`
- `official_pmi_mom`
- `ppi_yoy`
- `m1_yoy`
- `m2_yoy`
- `m1_m2_spread`
- `tsf_yoy` or `credit_impulse_proxy`

Confirmatory fields:

- `cpi_yoy`
- `industrial_production_yoy`
- `retail_sales_yoy`
- `fixed_asset_investment_ytd`
- `exports_yoy`
- `new_loans_total`

Canonical destination:

- `data/lakehouse/normalized/macro.slow_fields/YYYY/MM/part-00000.parquet`

Canonical fields:

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `field_id` | string | yes | canonical field name |
| `period_label` | string | yes | source reporting period |
| `period_start` | date/null | no | period start |
| `period_end` | date/null | no | period end |
| `release_date` | date | yes | known or conservative release date |
| `effective_date` | date | yes | next open trading day on/after release date |
| `value` | double | yes | normalized value |
| `unit` | string | yes | value unit |
| `frequency` | string | yes | monthly / quarterly / daily-like |
| `source_name` | string | yes | source adapter |
| `source_field` | string | yes | provider field name |
| `revision_note` | string | yes | revision-risk note |
| `definition_regime` | string/null | conditional | required for M1/M2 family |
| `regime_note` | string/null | conditional | required for M1/M2 family |
| `quality_status` | string | yes | validation summary |

Critical rule:

> `effective_date = next open trading day on or after release_date`

A slow field can enter `derived.etf_aw_rebalance_snapshot` only when:

```text
effective_date <= rebalance_date
```

Validation：

- `field_id` must be registered
- `period_label` must be parseable by field family
- `release_date` required
- `effective_date` required
- `effective_date` must be open trading day
- `effective_date >= release_date`
- M1/M2 family requires `definition_regime` and `regime_note`
- derived fields such as `official_pmi_mom` and `m1_m2_spread` only use already-effective component fields

## Rates And Curve

### `rates.daily_rates`

Initial primary field:

- `shibor_1w`

Canonical grain:

- `(rate_id, quote_date)`

Canonical destination:

- `data/lakehouse/normalized/rates.daily_rates/YYYY/MM/part-00000.parquet`

Canonical fields:

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `rate_id` | string | yes | canonical rate id, initially `shibor_1w` |
| `quote_date` | date | yes | source quote date |
| `effective_date` | date | yes | date on which the quote is allowed to enter strategy features |
| `value` | double | yes | normalized rate value |
| `unit` | string | yes | fixed `pct_per_annum` unless source contract says otherwise |
| `source_name` | string | yes | source adapter |
| `source_field` | string | yes | provider field name |
| `raw_batch_id` | bigint | yes | raw batch manifest ID |
| `ingested_at` | timestamp | yes | canonical write time |
| `quality_status` | string | yes | validation summary |

Rules:

- Shibor quote date semantics must be explicit in the source adapter contract
- initial same-day usage policy: `effective_date = next open trading day on or after quote_date`
- null and duplicate keys are blocking failures
- `value` must be positive and below an explicit sanity ceiling
- no forward fill is performed in the normalized fact; downstream features may choose as-of lookup explicitly

Stable check names:

- `daily_rates.duplicate_business_key`
- `daily_rates.rate_id_registered`
- `daily_rates.quote_date_required`
- `daily_rates.effective_date_open`
- `daily_rates.value_required`
- `daily_rates.value_positive`

### `rates.lpr`

Fields:

- `lpr_1y`
- `lpr_5y`

LPR should behave closer to slow fields than daily market facts because it has explicit publication timing.

Canonical destination:

- `data/lakehouse/normalized/rates.lpr/YYYY/MM/part-00000.parquet`

Canonical grain:

- `(rate_id, period_label)`

Canonical fields:

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `rate_id` | string | yes | `lpr_1y` or `lpr_5y` |
| `period_label` | string | yes | source publication period, usually `YYYY-MM` |
| `release_date` | date | yes | known or conservative publication date |
| `effective_date` | date | yes | next open trading day on/after release date |
| `value` | double | yes | normalized LPR value |
| `unit` | string | yes | fixed `pct_per_annum` |
| `source_name` | string | yes | source adapter |
| `source_field` | string | yes | provider field name |
| `revision_note` | string | yes | publication/revision-risk note |
| `raw_batch_id` | bigint | yes | raw batch manifest ID |
| `ingested_at` | timestamp | yes | canonical write time |
| `quality_status` | string | yes | validation summary |

Rules:

- release date comes from source date if available
- otherwise conservative 20th rule
- `effective_date = next open trading day on or after release_date`
- `lpr_1y` is primary
- `lpr_5y` is confirmatory
- no same-month value may enter a snapshot until `effective_date <= rebalance_date`

Stable check names:

- `lpr.duplicate_business_key`
- `lpr.rate_id_registered`
- `lpr.period_label_parseable`
- `lpr.release_date_required`
- `lpr.effective_date_open`
- `lpr.value_required`
- `lpr.value_positive`

### `rates.gov_curve_points`

Primary source:

- Tushare `yc_cb`

Canonical grain:

- `(curve_date, curve_type, tenor_years)`

Canonical destination:

- `data/lakehouse/normalized/rates.gov_curve_points/YYYY/MM/part-00000.parquet`

Canonical fields:

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `curve_date` | date | yes | source curve date |
| `effective_date` | date | yes | next open trading day on/after curve date |
| `curve_type` | string | yes | source/canonical curve type |
| `tenor_years` | double | yes | canonical tenor in years |
| `yield_pct` | double | yes | yield in percent |
| `source_name` | string | yes | source adapter |
| `source_field` | string | yes | provider yield field |
| `window_start` | date | yes | bounded source request start |
| `window_end` | date | yes | bounded source request end |
| `raw_batch_id` | bigint | yes | raw batch manifest ID |
| `ingested_at` | timestamp | yes | canonical write time |
| `quality_status` | string | yes | validation summary |

Rules:

- use bounded windows only
- never run a giant multi-year request
- exact tenor extraction for 1Y and 10Y must be deterministic
- overlapping windows must reconcile duplicate points deterministically
- `effective_date = next open trading day on or after curve_date`
- duplicate reconciliation must prefer the row from the newest successful raw batch only when values are identical or within a documented tolerance; otherwise validation fails

Derived endpoints:

- `cn_gov_1y_yield`
- `cn_gov_10y_yield`
- `cn_yield_curve_slope_10y_1y`

Curve status:

- `cn_gov_10y_yield` is primary-designated
- operationally it remains caveated until windowed extraction passes historical completeness checks

Stable check names:

- `gov_curve_points.duplicate_business_key`
- `gov_curve_points.curve_type_registered`
- `gov_curve_points.tenor_required`
- `gov_curve_points.tenor_supported`
- `gov_curve_points.curve_date_required`
- `gov_curve_points.effective_date_open`
- `gov_curve_points.yield_required`
- `gov_curve_points.yield_positive`
- `gov_curve_points.overlap_reconciliation`

## Tushare Source Contract Refinement

Stage C must make source schema drift explicit.

Each new Tushare endpoint adapter must define:

- required source columns
- optional source columns
- default value for missing optional columns
- fail behavior for missing required columns
- endpoint schema version

Suggested source contract:

| Endpoint | Required Columns | Optional Fill |
|----------|------------------|---------------|
| `fund_daily` | `ts_code`, `trade_date`, `close` | `pre_close`, `change`, `pct_chg`, `vol`, `amount` may be null |
| `fund_adj` | `ts_code`, `trade_date`, `adj_factor` | none |
| `index_daily` | `ts_code`, `trade_date`, `close` | `vol`, `amount` may be null |
| `index_basic` | `ts_code`, `name` | `list_date = NULL`, `delist_date = NULL` |
| macro endpoints | period/date + value fields | release date usually filled by rule |
| `shibor` | quote date + tenor value | non-primary tenors may be null |
| `shibor_lpr` | date + 1Y/5Y rate columns | non-requested tenor may be null |
| `yc_cb` | date + tenor + yield | source-specific extra columns ignored |

Stable check names:

- `source_contract.required_columns`
- `source_contract.optional_columns_filled`
- `source_contract.unsupported_rows_filtered`
- `source_contract.empty_payload`

## Profile Runner

Stage C introduces a thin profile runner:

- profile name: `etf_all_weather_v1_bootstrap`
- responsibility: call `run_dataset_sync()` in a fixed order
- non-goal: generic DAG scheduler

### Profile-Level Tushare Preflight

Stage B no-token test proved empty payload gating works. But Stage C profile should fail early when Tushare is required and no token is configured.

Rules:

- if profile requires Tushare and token is missing, profile fails before dataset runs
- no dataset raw batches are created by profile in this state
- write one profile-level failure summary
- manual single dataset runs still use Stage B behavior and may preserve empty raw batch for audit

### Bootstrap Order

Recommended order:

1. `reference.trading_calendar`
2. `reference.instruments`
3. `reference.etf_aw_sleeves`
4. `reference.rebalance_calendar`
5. `market.etf_daily` for five sleeves
6. `market.etf_adj_factor` for five sleeves
7. `derived.etf_aw_sleeve_daily`
8. `market.index_daily` for HS300 / ZZ1000
9. `derived.etf_aw_market_features`
10. `macro.slow_fields`
11. `rates.daily_rates`
12. `rates.lpr`
13. `rates.gov_curve_points`
14. `derived.etf_aw_rebalance_snapshot`

Profile behavior:

- stop on blocking validation failure
- record dependency auto-runs
- record dataset result summary
- preserve raw batches for failed dataset runs
- advance profile summary only after successful final dataset

## Rebalance Snapshot

Dataset: `derived.etf_aw_rebalance_snapshot`。

Canonical destination:

- `data/lakehouse/derived/derived.etf_aw_rebalance_snapshot/YYYY/MM/part-00000.parquet`

DuckDB may expose a read-only view over this Parquet dataset for notebooks or future UI queries, but the Parquet snapshot is the canonical artifact.

Purpose:

- notebook MVP
- backtest input
- explainability table
- future read-only frontend panel

Inputs:

- `reference.rebalance_calendar`
- `derived.etf_aw_sleeve_daily`
- `derived.etf_aw_market_features`
- `macro.slow_fields`
- `rates.daily_rates`
- `rates.lpr`
- `rates.gov_curve_points` derived endpoints

Rules:

- one snapshot per `rebalance_date`
- only include slow fields where `effective_date <= rebalance_date`
- LPR and curve/rate fields with `effective_date > rebalance_date` are excluded in the same way as slow macro fields
- market features must be computed using rows known before or on `rebalance_date`
- missing fields must be explicit flags
- no source adapter calls are allowed
- no allocation weights are produced in Stage C
- snapshot generation must be deterministic for the same upstream inputs
- snapshot rows should carry enough lineage to explain input freshness without reading raw batches
- Stage C snapshot timing is close-of-day as-of `rebalance_date`; it may use market closes for `trade_date <= rebalance_date`
- any future trade execution module must decide whether to execute on the next open day; Stage C does not imply same-day-before-close tradability

Minimum snapshot fields:

- `rebalance_date`
- `rebalance_month`
- sleeve adjusted prices / returns / realized vol inputs
- primary macro fields and availability flags
- rates fields and availability flags
- market confirmation fields
- source freshness summary
- quality status summary
- `upstream_run_ids`
- `input_max_trade_date`
- `input_effective_cutoff`
- `snapshot_generated_at`

## Testing Strategy

Stage C tests should build directly on Stage B tests.

### No-Live-Network Default

Automated tests must default to fixture DataFrames or mock Tushare clients. Real-data tests should be documented separately, as Stage B did.

### Required Test Groups

1. Stage B regression
   - all Stage B tests continue to pass
   - Stage C additions do not weaken Stage B validators

2. C0 frozen universe probe tests
   - five sleeves and two benchmarks are accepted
   - unsupported source catalog rows do not block frozen universe
   - missing frozen instrument blocks C0

3. Rebalance calendar tests
   - SH/SZ common open day rule
   - first eligible day after the 20th
   - rebalance date remains inside `rebalance_month`
   - no rollover into next month when no eligible day exists
   - missing SH/SZ coverage blocks generation

4. Adjustment factor tests
   - factor required
   - positive factor
   - duplicate key fail
   - missing factor blocks derived sleeve daily publish

5. Sleeve daily derived tests
   - no raw-close fallback
   - `adj_close = raw_close * adj_factor`
   - adjusted return deterministic from previous available adjusted close
   - first available adjusted return is null
   - missing previous close inside non-warmup publish window blocks publish
   - five-sleeve coverage
   - upstream run lineage is written
   - repeated derived run does not duplicate rows

6. Market feature tests
   - 20D formulas use trading-day lags
   - insufficient warmup produces null features with explicit incomplete flags
   - realized volatility requires 20 non-null one-day returns
   - rows before `backfill_start` are not published

7. Source schema drift tests
   - missing optional columns are filled
   - missing required columns fail
   - stable source contract validation records are written

8. Profile runner tests
   - missing required profile window parameters fail before dataset runs
   - no-token profile preflight fails before dataset runs
   - successful profile executes in fixed order
   - blocking validation stops downstream datasets

9. Timing leakage tests
   - slow fields are excluded if `effective_date > rebalance_date`
   - LPR and curve/rate fields are excluded if `effective_date > rebalance_date`
   - derived macro fields only use effective component rows
   - rebalance snapshot uses close-of-day as-of policy consistently

10. Rates and curve tests
   - Shibor same-day policy maps to an explicit `effective_date`
   - LPR conservative release-date fallback is deterministic
   - government curve bounded windows reconcile overlaps deterministically
   - conflicting overlapping curve values fail validation

## Acceptance Criteria

Stage C C0-C2 is complete when:

1. Stage B automated tests continue to pass.
2. C0 seven-instrument real-data probe passes.
3. `reference.etf_aw_sleeves` exactly encodes the five frozen v1 sleeves.
4. `reference.rebalance_calendar` uses SH/SZ common-open-day rule and stores `rebalance_month` as part of the business key.
5. `market.etf_adj_factor` writes raw and normalized data for five sleeves.
6. `derived.etf_aw_sleeve_daily` uses the fixed adjustment-aware return formula and writes upstream lineage.
7. Missing adjustment factor blocks publish rather than falling back to raw close.
8. Tushare endpoint schema drift has structured validation.
9. Profile-level no-token preflight avoids batch empty runs.
10. Required profile window and warmup parameters are validated.
11. Repeated sync / derive runs do not create duplicate business keys.

Full Stage C is complete when:

1. `macro.slow_fields` carries release/effective timing metadata.
2. `rates.daily_rates`, `rates.lpr`, and `rates.gov_curve_points` carry canonical schemas, validation, and effective-date semantics.
3. `derived.etf_aw_market_features` is rebuildable from normalized facts with explicit warmup and missing-input flags.
4. `derived.etf_aw_rebalance_snapshot` excludes future-unavailable slow/rates/curve fields.
5. Notebook/backtest code can consume the Parquet snapshot without calling Tushare directly.

## Implementation Sequence

Recommended order:

1. Add Stage C dataset definitions, profile parameters, and registry registration.
2. Add source contract helpers for required/optional column validation.
3. Add DuckDB schema refinements for `canonical_etf_aw_sleeves` and `canonical_rebalance_calendar.rebalance_month`.
4. Implement C0 frozen universe probe path.
5. Implement `reference.etf_aw_sleeves`.
6. Implement SH/SZ common-open `reference.rebalance_calendar`.
7. Add `market.etf_adj_factor` Tushare source / normalizer / validator.
8. Add `derived.etf_aw_sleeve_daily` builder and validator.
9. Add `derived.etf_aw_market_features`.
10. Add profile runner with window-parameter validation and no-token preflight.
11. Add `macro.slow_fields`.
12. Add `rates.daily_rates` and `rates.lpr`.
13. Add bounded-window `rates.gov_curve_points`.
14. Add `derived.etf_aw_rebalance_snapshot`.

## Risks

### 1. Treating Source Catalog As Strategy Universe

Stage B proved source catalogs can include unsupported or delisted rows. Stage C must keep frozen strategy universe separate from provider catalog breadth.

### 2. Raw-Close Fallback

The ETF all-weather research already froze adjustment-aware return as the v1 convention. Missing adjustment factor must block publish, not silently downgrade to raw close.

### 3. Macro Timing Leakage

The most dangerous Stage C failure is using monthly macro values before they were knowable. Release/effective date metadata must be enforced before snapshots are usable.

### 4. Curve Scope Creep

Government curve fields are useful but operationally caveated. The bounded-window extractor should be proven before curve endpoints become trusted primary fields.

### 5. Profile Becoming Scheduler

The Stage C profile runner should stay a deterministic bootstrap helper. Scheduler, retry policy, and cross-process lock remain future work.

## Deferred Decisions

The following decisions remain deferred beyond Stage C C0-C2:

- cross-source automatic fallback beyond Tushare for ETF price data
- official-source direct ingestion as a routine bulk path
- generic validation DSL
- version-preserving normalized facts
- instrument snapshot history / SCD table
- UI surface for ETF all-weather
- allocation engine and backtest framework

## Final Judgment

Stage C should first harden the Stage B-proven ingestion path around the ETF all-weather frozen universe, not jump straight into broad macro/curve complexity.

The optimized path is:

1. prove the seven required instruments through real data,
2. encode the five-sleeve strategy universe,
3. generate common-exchange rebalance dates,
4. publish adjustment-aware sleeve daily facts,
5. derive market confirmation features,
6. then add slow macro, rates, curve, and rebalance snapshots.

This keeps Stage C aligned with the real behavior discovered in Stage B: source data is useful but imperfect, raw must be preserved, canonical must be strict, and derived strategy data must be built only on validated normalized inputs.

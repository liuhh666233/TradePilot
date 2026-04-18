# V1 Schema Design

## Purpose

This document turns the frozen ETF all-weather v1 research boundary into an implementation-ready schema plan.

It answers five questions:

1. what data entities must exist in stage 1
2. which tables belong in DuckDB vs Parquet
3. what the canonical grain and keys are for each dataset
4. how release and effective dates survive into storage
5. what the first executable DDL should create now

This document is downstream of the frozen research contract and should not casually reopen it.

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

## Physical Storage Plan

Recommended root:

- `data/etf_all_weather/`

Recommended zones:

- `data/etf_all_weather/raw/`
- `data/etf_all_weather/normalized/`
- `data/etf_all_weather/derived/`

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

## Canonical Dataset Inventory

### A. DuckDB Control-Plane Tables

#### 1. `etf_aw_ingestion_runs`

Role:
- one row per ingestion or normalization job execution

Primary key:
- `run_id`

Why it exists:
- job traceability
- success/failure accounting
- replay visibility

#### 2. `etf_aw_raw_batches`

Role:
- manifest for immutable raw landed files

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

Primary key:
- `validation_id`

Why it exists:
- quality status should be inspectable rather than implicit

#### 4. `etf_aw_source_watermarks`

Role:
- remembers latest available and latest fetched boundary per dataset/source

Primary key:
- `(dataset_name, source_name)`

Why it exists:
- incremental sync control
- operational visibility

### B. DuckDB Reference Tables

#### 5. `canonical_sleeves`

Role:
- frozen v1 sleeve registry

Primary key:
- `sleeve_code`

Important fields:
- `sleeve_role`
- `benchmark_name`
- `exposure_note`
- `is_active`

#### 6. `canonical_instruments`

Role:
- broader reference registry for sleeves and confirmation instruments

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

Primary key:
- `(exchange, trade_date)`

Important fields:
- `is_open`
- `pretrade_date`
- `calendar_source`

#### 8. `canonical_rebalance_calendar`

Role:
- materialized v1 monthly decision schedule

Primary key:
- `rebalance_date`

Important fields:
- `calendar_month`
- `rule_name`
- `anchor_day`
- `previous_rebalance_date`

### C. Parquet-Backed Canonical Facts

These are canonical normalized contracts. In stage 1 they should live as partitioned Parquet datasets and be queried through DuckDB views later.

#### 9. `canonical_daily_market_fact`

Grain:
- one instrument per trade date

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

Partition recommendation:
- `dataset_year=YYYY/dataset_month=MM`

#### 10. `canonical_daily_rates_fact`

Grain:
- one field per trade date per source

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

#### 11. `canonical_slow_field_fact`

Grain:
- one field per period label per source

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

Partition recommendation:
- `field_name=<field>/dataset_year=YYYY`

Critical timing rule:
- a slow field may enter strategy features only when `effective_date <= rebalance_date`

#### 12. `canonical_curve_fact`

Grain:
- one curve tenor point per curve date per source

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

Partition recommendation:
- `dataset_year=YYYY/dataset_month=MM`

### D. Derived Dataset Reservation

#### 13. `monthly_feature_snapshot`

Role:
- one explainability-ready monthly as-of feature payload per rebalance date

Grain:
- one row per `rebalance_date`

Reserved fields:
- `rebalance_date`
- `schema_version`
- `feature_payload_json`
- `source_run_set_json`
- `created_at`

This dataset is not part of the first executable DDL because it belongs to the derived layer, but the schema direction should be reserved now.

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

### Return-Semantics Rule

Daily market facts must preserve enough information to support adjustment-aware return research.

Minimum expectation in normalized storage:

- `close`
- `adj_close`
- `pct_chg`
- `adj_pct_chg`

If the upstream source does not expose both raw and adjusted forms directly, the normalization code must document how the adjusted basis is derived.

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

## Recommended File Outputs

This milestone should produce two artifacts:

1. this design note
2. a DuckDB DDL draft:
   - `ddl-v1-schema.sql`

That SQL file should be safe to run repeatedly and should initialize the control-plane tables and reference dimensions.

## First Validation Checklist

Before coding the ingestion module, the schema draft is good enough if all of the following are true:

1. every frozen v1 sleeve has a stable home in `canonical_sleeves`
2. every slow field has a schema path for `release_date` and `effective_date`
3. raw batches can be traced to runs and normalized facts
4. rebalance dates are materialized explicitly rather than inferred ad hoc in notebooks
5. Parquet-backed fact datasets have defined grain, path, and partition strategy
6. curve data has a dedicated schema path instead of being hidden inside generic rates logic

## Practical Next Step After This Document

After approving this schema draft, the next implementation move should be:

1. add the dedicated ETF all-weather ingestion module skeleton
2. create code that applies `ddl-v1-schema.sql`
3. implement `trading calendar sync` first
4. seed `canonical_sleeves`
5. then build daily market, slow-field, and curve jobs in that order

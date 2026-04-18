---
title: "Data Ingestion Architecture"
status: draft
mode: "design"
created: 2026-04-18
updated: 2026-04-18
modules: ["backend"]
---

# Data Ingestion Architecture

## Overview

Design a unified ingestion architecture for TradePilot that supports multiple data types, preserves raw-source traceability, standardizes canonical storage, and embeds data validation into the pipeline itself.

The initial implementation target is to support the ETF all-weather Stage 1 data foundation, but the architecture must remain general enough to serve future stock, index, macro, rates, news, and derived-feature datasets without redesigning the ingestion core.

## Goals

- [ ] Build one ingestion foundation that supports multiple dataset families
- [ ] Define a clear `raw -> normalized -> derived` storage boundary
- [ ] Make validation and lineage first-class concerns of the ingestion framework
- [ ] Fit the architecture into the current `DuckDB + Python` backend with minimal disruption to existing flows
- [ ] Provide a reliable Stage 1 foundation for ETF all-weather data without overfitting the framework to that single use case

## Constraints

- Preserve current application behavior for existing ingestion and API flows
- Prefer additive architecture over refactoring existing provider code in place
- Use `DuckDB` for metadata and query-serving support, not as the only storage layer for large raw history
- Keep Stage 1 focused on ingestion, normalization, validation, and traceability
- Support incremental extension to new dataset families without reopening the core architecture

## Scope

### Modules Involved

| Module |
|--------|
| Backend |

### Key Files

| File | Planned Role |
|------|--------------|
| `tradepilot/db.py` | Extend schema initialization with ingestion metadata and canonical reference tables |
| `tradepilot/data/` | Existing provider layer remains available for current app flows; selective logic may be reused by new source adapters |
| `tradepilot/ingestion/service.py` | Existing sync service remains intact; new ingestion foundation should live alongside it rather than replacing it |
| `tradepilot/etl/` | Proposed new generic ingestion module for dataset-oriented extraction, normalization, validation, and storage |
| `docs/etf-all-weather-data-sources/` | Source-of-truth research inputs for ETF all-weather Stage 1 dataset choices, timing rules, and validation needs |

### Out of Scope

- Replacing the current market/news/bilibili ingestion flows immediately
- Building the ETF allocation engine itself
- Building the full backtest layer
- Building the final strategy read model or wide feature snapshot layer in Stage 1
- Expanding to every optional or deferred dataset before the core foundation is stable

## Design

### Architecture Summary

The recommended architecture is a dataset-oriented ingestion platform with six layers:

1. source adapter layer
2. raw landing layer
3. normalization layer
4. validation layer
5. metadata and lineage layer
6. derived and read-model layer

This architecture separates three concerns that are currently mixed together in simpler sync flows:

- data fetching from external sources
- canonicalization for downstream use
- reliability verification and auditability

### Design Principles

#### 1. Additive first

The new ingestion foundation should be introduced as a parallel backend module. Existing app behavior should continue working while the new architecture matures.

#### 2. Dataset-oriented, not endpoint-oriented

The system should model ingestion around dataset families such as `market.etf_daily` or `macro.slow_fields`, not around specific provider methods or one-off endpoints.

#### 3. Immutable raw storage

Raw fetched payloads must be persisted in immutable storage so normalization and validation can be replayed without depending on live source fetches.

#### 4. Validation as a first-class capability

Every dataset must ship with explicit validation rules. Validation must produce structured outputs stored in the metadata layer, not only logs or prose notes.

#### 5. Time-aware semantics

Slow fields must explicitly carry release and effective timing semantics to prevent look-ahead leakage.

#### 6. Hybrid storage

Use filesystem Parquet datasets for large raw and normalized facts, and `DuckDB` for metadata tables, reference dimensions, and query-serving views.

## Supported Dataset Families

The ingestion foundation should support at least these dataset families:

### 1. Reference and dimensions

- instruments
- sleeves
- source registry
- trading calendar
- rebalance calendar

### 2. Daily market facts

- ETF daily prices
- stock daily prices
- index daily prices
- commodity or gold proxy daily prices

### 3. Daily rates and liquidity facts

- Shibor
- repo or liquidity rates
- daily government yields
- LPR publication series

### 4. Slow macro facts

- PMI
- CPI and PPI
- M1 and M2
- TSF and loans
- industrial production, retail sales, exports, and similar monthly fields

### 5. Curve and panel facts

- government yield curve points
- tenor-based rate panels
- other dense panel-style time series

### 6. Alternative and event data

- news items
- policy notices
- research reports
- video metadata

### 7. Derived datasets

- trend features
- realized volatility features
- relative-strength features
- monthly snapshots for strategy layers

## Logical Layers

### 1. Source Adapter Layer

This layer talks to upstream providers and official sources.

Responsibilities:

- fetch source data by dataset family
- expose primary, fallback, and validation-source roles explicitly
- keep source-specific details out of normalization logic

Recommended source roles:

- primary source
- fallback source
- validation source

The framework should avoid silently treating a wrapper and its upstream dependency as independent validation sources.

### 2. Raw Landing Layer

This layer stores untouched or minimally packaged fetch results.

Responsibilities:

- persist immutable raw payloads
- record batch metadata including row counts, hashes, fetch windows, and source identity
- provide replayable input for downstream normalization and validation

Recommended rule:

- raw data should never be the only copy available transiently in memory

### 3. Normalization Layer

This layer maps raw data into canonical schema definitions.

Responsibilities:

- standardize column names
- standardize code formats and date types
- normalize units and value fields
- attach timing metadata where required
- preserve source lineage fields

### 4. Validation Layer

This layer executes structured quality checks against raw and normalized outputs.

Responsibilities:

- run dataset-specific checks
- classify results with consistent status labels
- persist results with subject-level detail and thresholds

### 5. Metadata and Lineage Layer

This layer stores run history, raw batch manifests, validation results, and watermarks.

Responsibilities:

- support traceability from normalized data back to raw batches
- support incremental sync control
- support operational monitoring and auditability

### 6. Derived and Read-Model Layer

This layer is downstream of normalized storage.

Responsibilities:

- build feature tables
- build as-of snapshots
- build strategy-facing or API-facing read models

Stage 1 should stop before broad derived feature expansion.

## Proposed Module Structure

The ingestion foundation should be implemented in a new generic module:

- `tradepilot/etl/__init__.py`
- `tradepilot/etl/models.py`
- `tradepilot/etl/datasets.py`
- `tradepilot/etl/registry.py`
- `tradepilot/etl/storage.py`
- `tradepilot/etl/normalizers.py`
- `tradepilot/etl/validators.py`
- `tradepilot/etl/service.py`
- `tradepilot/etl/read_models.py`
- `tradepilot/etl/sources/base.py`
- `tradepilot/etl/sources/tushare.py`
- `tradepilot/etl/sources/akshare.py`
- `tradepilot/etl/sources/official.py`

The name `etl` is intentionally broader than `etf_aw` so the module can remain the common ingestion foundation for multiple future domains.

## Dataset Registry Design

### Dataset family examples

- `reference.instruments`
- `reference.trading_calendar`
- `reference.rebalance_calendar`
- `reference.sleeves`
- `market.etf_daily`
- `market.stock_daily`
- `market.index_daily`
- `rates.daily_rates`
- `macro.slow_fields`
- `rates.curve_points`
- `alt.news_items`
- `alt.video_content`
- `derived.monthly_feature_snapshot`

### Dataset definition fields

Each dataset definition should include at least:

- `dataset_name`
- `category`
- `grain`
- `primary_source`
- `fallback_sources`
- `validation_sources`
- `storage_zone`
- `partition_strategy`
- `canonical_schema`
- `validation_rules`
- `supports_incremental`
- `watermark_key`
- `timing_semantics`

This registry allows new data types to be added without changing the ingestion engine structure.

## Storage Strategy

### Recommended root

- `data/lakehouse/`

### Zones

- `data/lakehouse/raw/`
- `data/lakehouse/normalized/`
- `data/lakehouse/derived/`

### Storage rules

- raw data should preserve source fidelity and batch identity
- normalized data should preserve canonical queryability and lineage
- derived data should remain downstream and replaceable

### Recommended partitioning

- daily market and rates data: `dataset/year/month`
- monthly macro data: `dataset/year`
- curve data: `dataset/year/month`
- slow long-form field data: `field_name/year`
- derived snapshots: `rebalance_year`

## DuckDB Metadata and Reference Tables

### 1. `etl_ingestion_runs`

Role: one row per ingestion job execution.

Core fields:

- `run_id`
- `job_name`
- `dataset_name`
- `source_name`
- `trigger_mode`
- `status`
- `started_at`
- `finished_at`
- `request_start`
- `request_end`
- `records_discovered`
- `records_inserted`
- `records_updated`
- `records_failed`
- `partitions_written`
- `error_message`
- `code_version`

### 2. `etl_raw_batches`

Role: manifest of immutable raw landed files.

Core fields:

- `raw_batch_id`
- `run_id`
- `dataset_name`
- `source_name`
- `source_endpoint`
- `storage_path`
- `file_format`
- `compression`
- `partition_year`
- `partition_month`
- `window_start`
- `window_end`
- `row_count`
- `content_hash`
- `fetched_at`
- `schema_version`
- `is_fallback_source`

### 3. `etl_validation_results`

Role: structured output of validation checks.

Core fields:

- `validation_id`
- `run_id`
- `raw_batch_id`
- `dataset_name`
- `check_name`
- `check_level`
- `status`
- `subject_key`
- `metric_value`
- `threshold_value`
- `details_json`
- `created_at`

### 4. `etl_source_watermarks`

Role: remember latest successful fetch boundaries for incremental updates.

Core fields:

- `dataset_name`
- `source_name`
- `latest_available_date`
- `latest_fetched_date`
- `latest_successful_run_id`
- `updated_at`

### 5. Reference tables

Small dimension tables should remain in DuckDB for fast joins and operational access:

- `canonical_instruments`
- `canonical_trading_calendar`
- `canonical_rebalance_calendar`
- `canonical_sleeves`
- `source_registry`

## Canonical Normalized Fact Models

### Common normalized rules

All normalized datasets should:

- define an explicit grain
- include `source_name`
- include `raw_batch_id`
- include `ingested_at`
- include `quality_status`
- standardize dates and code formats
- preserve sufficient lineage to trace values back to raw fetches

### Multi-version record policy

For datasets that may be corrected or republished by upstream sources, the canonical layer may retain multiple versions of the same business key instead of destructively overwriting prior normalized values.

Required rule:

- each canonical record version must carry `ingested_at`

Recommended interpretation:

- business key columns identify the logical observation
- `ingested_at` identifies when that version entered canonical storage
- downstream consumers that need the latest known value should select the newest row by `ingested_at`

This policy is especially useful for:

- slow macro fields
- revisable rates or curve fields
- event datasets with late corrections

This architecture deliberately prefers preserving historical record versions over silent destructive replacement.

### `canonical_daily_market_fact`

Suggested fields:

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
- `ingested_at`
- `quality_status`

### `canonical_daily_rates_fact`

Suggested fields:

- `field_name`
- `trade_date`
- `value`
- `unit`
- `source_name`
- `raw_batch_id`
- `ingested_at`
- `revision_note`
- `quality_status`

### `canonical_slow_field_fact`

Suggested fields:

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
- `ingested_at`
- `quality_status`

### `canonical_curve_fact`

Suggested fields:

- `curve_code`
- `curve_date`
- `curve_type`
- `tenor_years`
- `yield_value`
- `source_name`
- `raw_batch_id`
- `ingested_at`
- `quality_status`

## Source Adapter Design

The new ingestion framework should not extend the existing `DataProvider` ABC directly for all needs. The current provider abstraction is shaped around the existing workflow and does not model many of the dataset-oriented needs for macro, timing-aware fields, curves, or lineage.

The preferred approach is to introduce source adapters that are aligned to dataset families.

### Adapter capabilities

Adapters should expose dataset-oriented fetch methods, such as:

- `fetch_trading_calendar(...)`
- `fetch_instrument_metadata(...)`
- `fetch_market_daily(...)`
- `fetch_daily_rates(...)`
- `fetch_slow_fields(...)`
- `fetch_curve_points(...)`
- `fetch_alt_events(...)`

### Source role expectations

#### Tushare

Preferred as the core implementation backbone for:

- ETF and index daily history
- trading calendar
- parts of macro and rates data
- fund metadata

#### Official-source adapters

Required for:

- authoritative macro and rates anchoring
- recovery or revalidation paths
- explicit timing semantics for slow fields where wrappers are incomplete

#### AKShare

Useful as:

- wrapper convenience
- selective fallback
- validation surface where operationally stable

But it must not be treated as an automatically independent cross-source validator.

## Normalization Rules

Normalization should be deterministic and reusable.

### Code normalization

- all instruments must be stored with exchange suffixes such as `510300.SH`
- bare codes and suffixed codes must not be mixed within normalized facts

### Date normalization

- trade dates use `trade_date`
- monthly or period data use `period_label`
- source release semantics use `release_date`
- strategy-usable timing uses `effective_date`

### Type normalization

- numeric values should be normalized to consistent numeric types
- missing and invalid values should be standardized explicitly

### Lineage injection

Normalized rows should carry:

- `source_name`
- `raw_batch_id`
- `quality_status`

### Slow-field timing discipline

If a source does not provide a trustworthy release date directly, the framework must assign one conservatively by field family. `effective_date` should be the first open trading day on or after the release date.

This rule must be implemented in normalization or a dedicated timing-enrichment step, not deferred informally to strategy logic.

## Validation Framework

Validation must be built into the ingestion engine.

### Validation categories

#### 1. Source-level checks

- availability
- repeatability
- schema drift
- historical reach

#### 2. Identity-level checks

- code-name consistency
- benchmark mapping
- exchange mapping
- list-date completeness

#### 3. Continuity and integrity checks

- duplicate rows
- gap detection
- null and zero anomalies
- unchanged-price streaks
- extreme-jump sanity

#### 4. Timing checks

- release-date capture
- effective-date alignment
- month-end leakage detection
- no-lookahead simulation

#### 5. Cross-source checks

- spot-check agreement
- direction consistency
- unit and scale audit
- tolerance-band evaluation

#### 6. Strategy-fit checks

- role clarity
- complexity-earned review
- interpretation fitness

### Validation result labels

Suggested framework-level statuses:

- `pass`
- `pass_with_caveat`
- `warning`
- `validation_only`
- `defer`
- `fail`

### Validation rule registry

Each validation rule should be defined explicitly with:

- `check_name`
- `dataset_name`
- `severity`
- `subject_grain`
- `metric_definition`
- `threshold`
- `status_mapping`

## Test and Validation Requirements By Data Type

This section defines the minimum validation expectations for each major data type supported by the framework.

### 1. Trading calendar

Must validate:

- availability
- repeatability
- duplicate key absence
- holiday mapping sanity
- continuity
- monthly rebalance-rule reproducibility

Failure consequence:

- all as-of logic and backtest timing become suspect

### 2. Instrument metadata

Must validate:

- code-name consistency
- exchange mapping
- benchmark mapping
- list-date completeness
- metadata drift

Failure consequence:

- exposures may be mislabeled or mapped to the wrong tradable object

### 3. Daily market prices

Must validate:

- duplicate row absence
- gap detection versus trading calendar
- zero-close and null-price anomalies
- low-liquidity detection
- unchanged-price streak detection
- extreme-return sanity
- cross-source close comparison
- return consistency

Failure consequence:

- trend, return, volatility, and risk metrics become unreliable

### 4. Daily rates and liquidity series

Must validate:

- duplicate key absence
- unit and scale audit
- point-in-time availability
- continuity
- direction consistency across sources

Failure consequence:

- rates context and defensive sleeve interpretation become unreliable

### 5. Slow macro fields

Must validate:

- release-date capture
- effective-date alignment
- no-lookahead simulation
- duplicate key absence
- revision policy recording
- semantic regime tagging where required
- cross-source direction consistency

Failure consequence:

- macro state and allocation logic can leak future information or interpret unstable fields incorrectly

### 6. Curve data

Must validate:

- curve-date and tenor duplicate absence
- tenor completeness
- continuity of extracted points
- unit and scale audit
- derived-slope reproducibility

Failure consequence:

- duration and curve-shape signals become operationally unsafe

### 7. Alternative and event data

Must validate:

- source identity completeness
- event timestamp completeness
- duplicate event detection
- parse completeness
- content schema stability

Failure consequence:

- event pipelines become noisy and difficult to trust downstream

### 8. Derived datasets

Must validate:

- reproducibility from normalized inputs
- no-lookahead behavior
- window completeness
- role clarity

Failure consequence:

- downstream strategy logic becomes hard to audit or explain

## ETF All-Weather Stage 1 Mapping

ETF all-weather Stage 1 should be treated as a concrete profile running on top of this generic ingestion foundation.

### Stage 1 core datasets

- `reference.trading_calendar`
- `reference.rebalance_calendar`
- `reference.sleeves`
- `reference.instruments`
- `market.etf_daily`
- `market.index_daily`
- `macro.slow_fields`
- `rates.daily_rates`
- `rates.curve_points`

### Frozen v1 canonical sleeves

- `510300.SH`
- `159845.SZ`
- `511010.SH`
- `518850.SH`
- `159001.SZ`

### Important boundary

Earlier research notes included a bond candidate `511020.SH`, but the frozen v1 sleeve definition and later validation addendum moved the canonical bond sleeve to `511010.SH`. The ingestion framework, canonical sleeve registry, and all related tests must use the frozen v1 sleeves only.

### Stage 1 timing requirement

Slow macro fields in ETF all-weather Stage 1 must carry:

- `period_label`
- `release_date`
- `effective_date`
- `revision_note`

And M1-family fields must additionally carry:

- `definition_regime`
- `regime_note`

## Service Layer Design

The ingestion engine should expose a service layer with reusable orchestration entry points.

### Core service operations

- `run_dataset_sync(dataset_name, request)`
- `run_multi_dataset_sync(dataset_names, request)`
- `run_bootstrap(profile_name)`
- `list_runs(dataset_name=None)`
- `list_validation_results(dataset_name=None, run_id=None)`
- `get_dataset_freshness()`
- `get_dataset_health(dataset_name)`

### Run modes

- `manual`
- `scheduled`
- `backfill`
- `retry_failed`
- `validation_only`

### Profiles

Profiles allow grouped execution for business use cases, such as:

- `etf_all_weather_stage1`
- `market_core_daily`
- `macro_core_monthly`
- `research_bootstrap`

## Task Orchestration And Concurrency Control

Task orchestration is part of the core ingestion design and is required for the bootstrap-then-incremental operating model.

### Orchestration responsibilities

The scheduler and service layer together should guarantee:

- dataset dependency order is respected
- the same dataset is not processed by multiple conflicting runs at the same time
- bootstrap, incremental, and repair backfill runs do not race each other
- failed tasks can be retried without corrupting canonical state or watermarks

### Dataset-level execution lock

The framework should enforce a dataset-level exclusive execution rule.

Rule:

- only one active write-producing run may exist for a given `dataset_name` at a time

This means the following combinations must not run concurrently for the same dataset:

- bootstrap + incremental
- bootstrap + repair backfill
- incremental + repair backfill
- two incrementals for the same dataset

Validation-only tasks may run separately only if they do not mutate canonical state or watermark state.

### Dependency-aware scheduling

Datasets should declare upstream dependencies in their registry definition.

Examples:

- `reference.rebalance_calendar` depends on `reference.trading_calendar`
- `market.etf_daily` depends on `reference.instruments` and `reference.trading_calendar`
- `macro.slow_fields` may depend on `reference.trading_calendar` for `effective_date` enrichment
- derived datasets depend on validated normalized inputs

The scheduler should only run a dataset when its required upstream datasets are in an acceptable state.

### State gating rules

The dataset lifecycle states defined in this document should gate scheduling decisions.

Recommended rules:

- `not_initialized`: only bootstrap is allowed
- `bootstrapping`: block incremental and repair backfill for the same dataset
- `active_incremental`: allow scheduled incremental; allow repair backfill only in a controlled, mutually exclusive mode
- `backfill_repair`: block scheduled incremental until repair completes or is explicitly cancelled
- `degraded`: allow validation and repair work; downstream derived jobs may be paused by policy

### Retry policy

Retry behavior should be explicit and conservative.

Recommended rules:

- transient source failures may be retried automatically with bounded retry count
- permanent schema or validation failures should not be retried blindly
- retries must preserve the same dataset-level lock semantics
- watermark advancement must remain success-only even across retries

### Profile execution order

When a profile contains multiple datasets, execution should follow dependency order rather than simple list order.

For the initial ETF all-weather bootstrap profile, this means the profile runner should enforce the documented sequence and stop or degrade cleanly when a required upstream dataset fails.

## Historical Backfill Strategy

Historical backfill should be treated as a first-class run mode of the ingestion system rather than an ad hoc script.

### Operating model

The intended operating model is:

1. when a dataset is introduced for the first time, run a historical bootstrap or backfill to build the full required history
2. after bootstrap completes successfully, switch that dataset to scheduled daily incremental sync
3. if validation later detects a historical gap or a source outage leaves a hole, run a targeted rolling backfill for the missing window

This model gives the pipeline a simple and durable lifecycle:

- first load full history
- then keep it fresh with daily incrementals
- repair gaps with explicit backfill jobs when needed

### Backfill objectives

The purpose of backfill is to populate missing historical coverage safely while preserving:

- raw-source replayability
- deterministic normalization
- validation traceability
- idempotent reruns
- correct point-in-time semantics for slow fields

### Backfill run mode

The service layer should support a dedicated `backfill` mode in addition to routine incremental sync. A backfill run should:

1. select a dataset family
2. compute the missing historical coverage window
3. split the request into bounded fetch windows
4. land each batch into immutable raw storage
5. normalize and validate each batch independently
6. upsert or append normalized outputs according to dataset grain
7. update watermarks only after a successful batch or completed backfill segment

Once the initial backfill finishes and validation passes, the dataset should be marked ready for scheduled incremental sync.

### Incremental daily sync after bootstrap

After a dataset has completed its initial historical bootstrap, the pipeline should switch to a daily scheduled task.

The daily task should:

1. read the dataset watermark
2. compute the next incremental fetch window
3. fetch only new or recently revisable records
4. land raw batches using the same raw-first path as backfill
5. normalize and validate the incremental batch
6. update canonical storage idempotently
7. advance the watermark only on success

For most daily datasets, the default incremental window should start from:

- `latest_fetched_date + 1 day`

For revisable or timing-sensitive datasets, the default incremental window should include a small replay buffer. Examples:

- monthly slow fields: refetch the current and previous relevant publication periods
- rates or curve data with occasional corrections: refetch a short recent window
- event data with delayed updates: refetch the latest page or recent day range

This replay-buffer pattern avoids missing late corrections while preserving predictable daily operations.

### How the current architecture fills historical data

#### 1. Determine the historical target range

Each dataset definition should carry a minimum required start boundary for research or production use. The backfill process compares:

- target start date
- earliest normalized date currently available
- source availability boundary

This gives the missing backfill window.

Examples:

- `market.etf_daily`: from instrument `list_date` or strategy-required start date
- `market.index_daily`: from benchmark-required start date
- `macro.slow_fields`: from field-level earliest reliable period
- `rates.curve_points`: from earliest operationally trustworthy source history

#### 2. Split history into bounded windows

Backfill should never request very long history in one live fetch when the source is rate-limited or fragile. The framework should support dataset-specific chunking rules.

Recommended examples:

- daily market data: chunk by month or quarter
- daily rates: chunk by month or quarter
- monthly macro data: chunk by year
- curve data: chunk by month
- alternative data: chunk by day, week, or source pagination boundary

This keeps retries small and makes partial progress durable.

#### 3. Land every historical fetch into raw storage

Each fetched window becomes a raw batch with its own manifest entry in `etl_raw_batches`. That means historical backfill is not special at the storage layer. It uses the same raw-first discipline as current-period sync.

This is important because historical recovery often needs reruns after:

- source schema drift
- normalization bug fixes
- new validation rules
- source replacement

#### 4. Normalize with deterministic lineage

Historical batches should be normalized using the same canonical rules as fresh data. Every normalized row should retain:

- `source_name`
- `raw_batch_id`
- `quality_status`

For slow fields, backfill must also attach:

- `release_date`
- `effective_date`
- `revision_note`
- regime metadata where required

This ensures that backfilled history remains safe for point-in-time queries later.

#### 5. Validate each historical batch before promoting it

Historical data should not be assumed valid just because it is old. Validation must run on each batch or backfill segment.

Typical checks during backfill:

- duplicate keys after load
- gap detection versus canonical calendar
- cross-source agreement on sampled windows
- null or zero anomalies
- release-date and effective-date presence for slow fields
- no-lookahead checks for timing-aware datasets

Validation results belong in `etl_validation_results`, exactly as they do for routine syncs.

#### 6. Load rules by dataset type

The framework should support two loading patterns.

##### Append-heavy facts

Use append when the dataset grain and partition layout make duplicates easy to detect and remove by key.

Typical datasets:

- raw landed files
- normalized market facts
- normalized rates facts
- normalized curve facts

##### Upsert-oriented dimensions and slow fields

Use upsert when corrections or reruns may replace previously normalized values for the same key.

Typical datasets:

- instrument metadata
- trading calendar
- rebalance calendar
- slow macro fields

The key rule is that rerunning the same historical window should be idempotent at the canonical layer.

##### Version-preserving canonical facts

For revisable datasets, the canonical layer may preserve multiple record versions for the same logical key, distinguished by `ingested_at`.

In this design:

- raw history is always immutable
- canonical history may also be version-preserving
- downstream queries must choose whether they want:
  - latest-known view by `ingested_at`
  - earlier historical ingestion versions for audit or replay analysis

This avoids destructive replacement while still letting downstream consumers retrieve the newest available normalized value deterministically.

#### 7. Advance watermarks conservatively

Watermarks should only move after a successful batch or logically complete segment. A failed backfill must not mark coverage as complete.

For historical backfill, it is often useful to track both:

- latest fetched date
- earliest fully validated date

If needed later, the watermark model can be expanded to include both forward-sync and backfill completeness state.

### Backfill ordering strategy

Historical backfill should follow dependency order.

Recommended order:

1. reference tables
2. trading calendar
3. instrument metadata
4. daily market facts
5. daily rates facts
6. slow macro fields
7. curve points
8. derived datasets

This order matters because later datasets may depend on earlier canonical tables for validation or timing enrichment.

### Initial bootstrap versus rolling backfill

The architecture should distinguish two historical fill scenarios.

#### Initial bootstrap

Used when a dataset has little or no historical coverage yet.

Characteristics:

- long target range
- many batches
- more validation work
- likely manual monitoring

#### Rolling backfill

Used when a gap is discovered after normal operations are already live.

Characteristics:

- narrow missing window
- targeted rerun
- often triggered by validation findings or source outage recovery

### Dataset lifecycle states

To support the bootstrap-then-daily model cleanly, each dataset should conceptually move through these states:

1. `not_initialized`
2. `bootstrapping`
3. `active_incremental`
4. `backfill_repair`
5. `degraded`

Suggested meaning:

- `not_initialized`: dataset exists in registry but has no usable history yet
- `bootstrapping`: full historical backfill is running
- `active_incremental`: bootstrap is complete and daily sync is the normal operating mode
- `backfill_repair`: a targeted historical repair job is filling gaps
- `degraded`: scheduled sync is running but validation has detected unresolved data quality issues

This state model is useful for scheduling, monitoring, and deciding whether downstream derived jobs should proceed.

### Dataset-specific backfill guidance

#### Daily market data

- start from canonical instrument list and `list_date`
- backfill by instrument and month or quarter
- validate against trading calendar after load
- for ETFs, preserve adjustment-aware return basis requirements at normalization time

#### Slow macro fields

- backfill by field family and year
- do not infer `effective_date` lazily at query time
- store conservative release rules during normalization
- reruns must preserve point-in-time safety

#### Rates and curve fields

- prefer smaller windows because historical source endpoints may be fragile
- validate units and tenor coverage repeatedly during backfill
- treat curve history as operationally caveated until extraction stability is proven

#### Alternative or event data

- backfill by source pagination windows
- use source item identifiers and content hashes for idempotency
- do not rely only on timestamps for de-duplication

### Idempotency requirements

Historical backfill must be safe to rerun.

That means:

- raw landing is immutable and additive
- canonical loads are key-based append, append-with-version, or upsert depending on dataset rules
- duplicate detection is part of validation
- batch manifests prevent losing track of what was fetched before

For version-preserving datasets, idempotency means rerunning the same batch should not create ambiguous duplicate versions. The framework should use batch identity plus business key plus `ingested_at` semantics to keep reruns deterministic.

### Practical implication for ETF all-weather Stage 1

For ETF all-weather Stage 1, historical fill should be implemented as a bootstrap profile such as `etf_all_weather_stage1_backfill`.

The first backfill sequence should be:

1. `reference.sleeves`
2. `reference.instruments`
3. `reference.trading_calendar`
4. `reference.rebalance_calendar`
5. `market.etf_daily`
6. `market.index_daily`
7. `rates.daily_rates`
8. `macro.slow_fields`
9. `rates.curve_points`

This ensures the strategy's minimum serious historical panel is built in dependency order and remains fully auditable.

After that initial sequence is complete, daily scheduled tasks should keep these datasets fresh:

- `reference.trading_calendar`
- `market.etf_daily`
- `market.index_daily`
- `rates.daily_rates`
- `macro.slow_fields`
- `rates.curve_points`

Reference dimensions such as sleeves and stable instrument metadata can run on a lower-frequency refresh cadence, but they should still use the same incremental ingestion path.

## Testing Strategy

The framework should use `unittest` and include tests at multiple levels.

### 1. Unit tests

Cover:

- code normalization
- date parsing
- timing-rule logic
- schema mapping behavior
- validation status mapping

### 2. Contract tests

Cover:

- adapter output schema expectations
- dataset registry completeness
- canonical schema compatibility

### 3. Integration tests

Cover:

- temporary DuckDB and data root setup
- raw landing
- normalization
- validation result persistence
- metadata and watermark updates

### 4. Regression data tests

Cover:

- key dataset quality assumptions over fixed windows
- timing and no-lookahead invariants
- canonical sleeve coverage and identity assumptions

### Suggested test paths

- `tests/etl/test_registry.py`
- `tests/etl/test_models.py`
- `tests/etl/test_storage.py`
- `tests/etl/test_normalizers.py`
- `tests/etl/test_validators.py`
- `tests/etl/test_service.py`
- `tests/etl/test_rebalance_calendar.py`
- `tests/etl/test_slow_field_timing.py`

## Delivery Phases

### Stage A: foundation skeleton

- create `tradepilot/etl/` module
- create core models and registry structures
- create metadata tables and reference-table initialization
- define storage layout

### Stage B: first generic datasets

- trading calendar ingestion
- instrument metadata ingestion
- core market-daily ingestion pattern
- initial validation engine

### Stage C: ETF all-weather minimum serious panel

- five canonical ETF sleeves
- benchmark index series
- core slow macro fields
- core rates and yield series

### Stage D: downstream support

- rebalance calendar materialization
- freshness and health read models
- validation result query surfaces
- derived features after Stage 1 normalized layer is stable

## Risks

### 1. Source stability is uneven

The architecture must distinguish primary, fallback, and validation roles explicitly and not assume all providers are equally reliable.

### 2. Slow-field timing is the highest leakage risk

Timing metadata must be attached before data reaches strategy-facing logic.

### 3. Different dataset families have different grains

One-table-fits-all design should be avoided. The registry and storage model must be dataset-family aware.

### 4. DuckDB should not be treated as the only historical lake

Large raw and normalized histories belong in Parquet storage.

### 5. Scope drift is easy with optional fields

Each dataset and field should have explicit role labels so optional fields do not quietly become permanent requirements.

## Deferred Topics

The following topics are intentionally not fully designed in the current phase.

### 1. Schema evolution as a framework feature

Current decision:

- do not design a general schema evolution subsystem yet
- use raw-to-schema mapping as the primary compatibility boundary
- if canonical schema changes materially, replay history from raw with the new mapping

### 2. Storage retention, archival, and compaction

Current decision:

- not part of the current Stage 1 design
- raw and normalized storage layout is defined, but lifecycle management is deferred

### 3. Observability, SLA, and alerting framework

Current decision:

- freshness and health read models remain in scope later
- full alerting and operational observability design is deferred for now

## Final Judgment

The best long-term ingestion architecture for TradePilot is a generic dataset-oriented foundation that treats raw landing, canonical normalization, validation, and lineage as separate but integrated concerns.

For implementation, the correct next step is not to further stretch the existing workflow-specific provider abstraction. It is to introduce a new ingestion foundation that can support ETF all-weather Stage 1 immediately while remaining reusable for future stock, macro, rates, and alternative-data expansion.

-- ETF All-Weather v1 schema draft
--
-- This DDL intentionally initializes the DuckDB control plane and reference
-- dimensions first. Large historical facts remain Parquet-backed datasets in
-- stage 1 and should be exposed later through DuckDB views.

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS etf_aw_ingestion_runs (
    run_id BIGINT PRIMARY KEY,
    job_name VARCHAR NOT NULL,
    dataset_name VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    trigger_mode VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    request_start DATE,
    request_end DATE,
    partitions_written INTEGER DEFAULT 0,
    records_discovered BIGINT DEFAULT 0,
    records_inserted BIGINT DEFAULT 0,
    records_updated BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    error_message TEXT,
    code_version VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etf_aw_raw_batches (
    raw_batch_id BIGINT PRIMARY KEY,
    run_id BIGINT NOT NULL,
    dataset_name VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    source_endpoint VARCHAR,
    storage_path VARCHAR NOT NULL,
    file_format VARCHAR NOT NULL,
    compression VARCHAR,
    partition_year INTEGER,
    partition_month INTEGER,
    window_start DATE,
    window_end DATE,
    row_count BIGINT DEFAULT 0,
    content_hash VARCHAR,
    fetched_at TIMESTAMP NOT NULL,
    schema_version VARCHAR NOT NULL,
    is_fallback_source BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dataset_name, source_name, storage_path)
);

CREATE TABLE IF NOT EXISTS etf_aw_validation_results (
    validation_id BIGINT PRIMARY KEY,
    run_id BIGINT NOT NULL,
    raw_batch_id BIGINT,
    dataset_name VARCHAR NOT NULL,
    check_name VARCHAR NOT NULL,
    check_level VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    subject_key VARCHAR,
    metric_value DOUBLE,
    threshold_value DOUBLE,
    details_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etf_aw_source_watermarks (
    dataset_name VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    latest_available_date DATE,
    latest_fetched_date DATE,
    latest_successful_run_id BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_name, source_name)
);

CREATE TABLE IF NOT EXISTS canonical_sleeves (
    sleeve_code VARCHAR PRIMARY KEY,
    sleeve_role VARCHAR NOT NULL,
    sleeve_name VARCHAR NOT NULL,
    listing_exchange VARCHAR NOT NULL,
    benchmark_name VARCHAR,
    list_date DATE,
    exposure_note TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS canonical_instruments (
    instrument_code VARCHAR PRIMARY KEY,
    instrument_name VARCHAR NOT NULL,
    instrument_type VARCHAR NOT NULL,
    exchange VARCHAR,
    benchmark_name VARCHAR,
    list_date DATE,
    delist_date DATE,
    source_name VARCHAR NOT NULL,
    metadata_json TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS canonical_trading_calendar (
    exchange VARCHAR NOT NULL,
    trade_date DATE NOT NULL,
    is_open BOOLEAN NOT NULL,
    pretrade_date DATE,
    calendar_source VARCHAR NOT NULL,
    PRIMARY KEY (exchange, trade_date)
);

CREATE TABLE IF NOT EXISTS canonical_rebalance_calendar (
    rebalance_date DATE PRIMARY KEY,
    calendar_month VARCHAR NOT NULL,
    rule_name VARCHAR NOT NULL,
    anchor_day INTEGER NOT NULL,
    previous_rebalance_date DATE,
    calendar_source VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_etf_aw_ingestion_runs_dataset_status
    ON etf_aw_ingestion_runs (dataset_name, status, started_at);

CREATE INDEX IF NOT EXISTS idx_etf_aw_raw_batches_run_id
    ON etf_aw_raw_batches (run_id);

CREATE INDEX IF NOT EXISTS idx_etf_aw_validation_results_run_dataset
    ON etf_aw_validation_results (run_id, dataset_name);

CREATE INDEX IF NOT EXISTS idx_canonical_sleeves_role
    ON canonical_sleeves (sleeve_role);

INSERT OR REPLACE INTO canonical_sleeves (
    sleeve_code,
    sleeve_role,
    sleeve_name,
    listing_exchange,
    benchmark_name,
    list_date,
    exposure_note,
    is_active
) VALUES
    (
        '510300.SH',
        'equity_large',
        '沪深300ETF',
        'SH',
        '沪深300',
        NULL,
        'Canonical v1 large-cap equity sleeve.',
        TRUE
    ),
    (
        '159845.SZ',
        'equity_small',
        '中证1000ETF',
        'SZ',
        '中证1000',
        NULL,
        'Canonical v1 small-cap equity sleeve.',
        TRUE
    ),
    (
        '511010.SH',
        'bond',
        '国债ETF',
        'SH',
        '上证5年期国债指数 or prospectus-validated benchmark',
        NULL,
        'Canonical v1 bond defense sleeve. Exact benchmark naming must be validated against prospectus and exchange metadata during instrument sync.',
        TRUE
    ),
    (
        '518850.SH',
        'gold',
        '黄金ETF',
        'SH',
        '黄金现货合约 or prospectus-validated benchmark',
        NULL,
        'Canonical v1 gold hedge sleeve. Exact benchmark naming must be validated during instrument sync.',
        TRUE
    ),
    (
        '159001.SZ',
        'cash',
        '现金管理ETF',
        'SZ',
        'cash-management proxy',
        NULL,
        'Canonical v1 cash or neutral buffer sleeve.',
        TRUE
    );

-- Optional future query surfaces for Parquet-backed normalized facts.
-- Create these views only after the matching partitions exist.
--
-- Example:
-- CREATE OR REPLACE VIEW vw_etf_aw_canonical_daily_market_fact AS
-- SELECT *
-- FROM read_parquet('data/etf_all_weather/normalized/daily_market/**/*.parquet', hive_partitioning = true);
--
-- CREATE OR REPLACE VIEW vw_etf_aw_canonical_daily_rates_fact AS
-- SELECT *
-- FROM read_parquet('data/etf_all_weather/normalized/daily_rates/**/*.parquet', hive_partitioning = true);
--
-- CREATE OR REPLACE VIEW vw_etf_aw_canonical_slow_field_fact AS
-- SELECT *
-- FROM read_parquet('data/etf_all_weather/normalized/slow_fields/**/*.parquet', hive_partitioning = true);
--
-- CREATE OR REPLACE VIEW vw_etf_aw_canonical_curve_fact AS
-- SELECT *
-- FROM read_parquet('data/etf_all_weather/normalized/curve/**/*.parquet', hive_partitioning = true);

COMMIT;

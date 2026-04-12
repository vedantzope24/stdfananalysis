-- =============================================================================
-- STDF Analytics — ClickHouse Schema
-- ONLY 4 TABLES EXIST IN CLICKHOUSE
-- =============================================================================

CREATE DATABASE IF NOT EXISTS stdf_analytics;
USE stdf_analytics;

-- ---------------------------------------------------------------------------
-- 1. parametric_results_ch
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS parametric_results_ch (
    result_id String,
    device_id String,
    wafer_id LowCardinality(String),
    file_id LowCardinality(String),
    lot_id LowCardinality(String),
    part_type LowCardinality(String),
    test_num Int32,
    canonical_name LowCardinality(String),
    pass_fail LowCardinality(String),
    passed UInt8,
    failed_low UInt8,
    failed_high UInt8,
    alarm UInt8,
    not_executed UInt8,
    result_raw Float64,
    result_scaled Float64,
    result_si Float64,
    lo_limit_scaled Nullable(Float64),
    hi_limit_scaled Nullable(Float64),
    ingestion_date Date,
    ingestion_ts DateTime('UTC'),
    attempt_index UInt8,
    head_num Int16,
    site_num Int16,

    INDEX idx_wafer_id wafer_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_passed passed TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingestion_date)
ORDER BY (lot_id, canonical_name, ingestion_date, device_id, test_num)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- 2. functional_results_ch
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS functional_results_ch (
    device_id String,
    file_id LowCardinality(String),
    wafer_id LowCardinality(String),
    lot_id LowCardinality(String),
    test_num Int32,
    test_name String,
    vector_name String,
    head_num Int16,
    site_num Int16,
    passed UInt8,
    alarm UInt8,
    alarm_id String,
    cycle_count Int64,
    fail_count Int32,
    repeat_count Int32,
    rel_vect_addr Int64,
    xfail_addr Int64,
    yfail_addr Int64,
    vect_offset Int64,
    time_set String,
    op_code String,
    attempt_index UInt8,
    parser_version String,
    ingestion_ts DateTime('UTC'),
    ingestion_date Date
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingestion_date)
ORDER BY (lot_id, wafer_id, device_id, test_num);

-- ---------------------------------------------------------------------------
-- 3. wafer_rollups_ch
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wafer_rollups_ch (
    wafer_id String,
    lot_id LowCardinality(String),
    yield_pct Float64,
    pass_count UInt32,
    fail_count UInt32,
    retest_rate Float64,
    compute_ts DateTime('UTC'),
    ingestion_date Date
)
ENGINE = ReplacingMergeTree(compute_ts)
PARTITION BY toYYYYMM(ingestion_date)
ORDER BY (lot_id, wafer_id, ingestion_date);

-- ---------------------------------------------------------------------------
-- 4. tsr_summary_ch
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tsr_summary_ch (
    file_id LowCardinality(String),
    lot_id LowCardinality(String),
    test_num Int32,
    test_name String,
    test_type String,
    head_num Int16,
    site_num Int16,
    exec_count Int32,
    fail_count Int32,
    alarm_count Int32,
    avg_exec_time Float64,
    min_result Float64,
    max_result Float64,
    sum_results Float64,
    ingestion_date Date,
    ingestion_ts DateTime('UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingestion_date)
ORDER BY (lot_id, test_num);

-- ---------------------------------------------------------------------------
-- SETTINGS & USERS
-- ---------------------------------------------------------------------------
CREATE USER IF NOT EXISTS stdf_analytics IDENTIFIED WITH plaintext_password BY 'stdf_analytics_123';

GRANT SELECT, INSERT ON stdf_analytics.* TO stdf_analytics;

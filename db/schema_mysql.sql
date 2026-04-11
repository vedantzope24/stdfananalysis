-- =============================================================================
-- STDF Analytics — MySQL Schema
-- Matches parser output from export_parquet.py (9 tables + tsr_summary)
-- Run once to initialise the database:
--   mysql -u <user> -p <dbname> < schema_mysql.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0.  Database & character-set
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS stdf_analytics
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE stdf_analytics;

-- ---------------------------------------------------------------------------
-- 1.  lots  (MIR + MRR — one row per STDF file / lot)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lots (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id          VARCHAR(64)     NOT NULL,
    file_hash        VARCHAR(128),
    file_name        VARCHAR(255),
    parser_version   VARCHAR(32),
    ingestion_ts     DATETIME,

    lot_id           VARCHAR(64),
    sublot_id        VARCHAR(64),
    part_type        VARCHAR(64),
    family_id        VARCHAR(64),
    pkg_type         VARCHAR(64),
    process_id       VARCHAR(64),
    design_rev       VARCHAR(32),
    date_code        VARCHAR(32),
    job_name         VARCHAR(128),
    job_rev          VARCHAR(32),
    test_code        VARCHAR(32),
    test_temp        VARCHAR(32),
    flow_id          VARCHAR(64),
    operator         VARCHAR(64),
    supervisor       VARCHAR(64),
    node_name        VARCHAR(128),
    tester_type      VARCHAR(64),
    exec_type        VARCHAR(64),
    exec_ver         VARCHAR(64),
    facility_id      VARCHAR(64),
    floor_id         VARCHAR(64),
    serial_num       VARCHAR(64),
    eng_id           VARCHAR(64),
    setup_time       DATETIME,
    start_time       DATETIME,
    finish_time      DATETIME,
    disp_code        VARCHAR(8),
    station_num      INT,
    burn_time_min    INT,
    mode_code        VARCHAR(8),
    user_text        TEXT,
    user_desc        TEXT,

    PRIMARY KEY (id),
    UNIQUE KEY uq_lots_file_id (file_id),
    KEY idx_lots_lot_id      (lot_id),
    KEY idx_lots_part_type   (part_type),
    KEY idx_lots_start_time  (start_time),
    KEY idx_lots_node_name   (node_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 2.  wafers  (WIR + WRR — one row per wafer per file)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wafers (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id          VARCHAR(64)     NOT NULL,
    lot_id           VARCHAR(64),
    wafer_id         VARCHAR(64),
    head_num         SMALLINT,
    site_grp         SMALLINT,
    start_time       DATETIME,
    finish_time      DATETIME,
    total_devices    INT,
    pass_count       INT,
    fail_count       INT,
    retest_count     INT,
    abort_count      INT,
    yield_pct        DOUBLE,
    dppm             DOUBLE,
    ingestion_ts     DATETIME,
    ingestion_date   DATE,

    PRIMARY KEY (id),
    UNIQUE KEY uq_wafers_file_wafer (file_id, wafer_id),
    KEY idx_wafers_lot_id    (lot_id),
    KEY idx_wafers_wafer_id  (wafer_id),
    KEY idx_wafers_yield     (yield_pct),
    KEY idx_wafers_ing_date  (ingestion_date),
    CONSTRAINT fk_wafers_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 3.  parts  (PRR — one row per device per test attempt)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS parts (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    device_id        VARCHAR(128)    NOT NULL,
    file_id          VARCHAR(64)     NOT NULL,
    wafer_id         VARCHAR(64),
    lot_id           VARCHAR(64),
    part_type        VARCHAR(64),
    x_coord          SMALLINT,
    y_coord          SMALLINT,
    site_num         SMALLINT,
    head_num         SMALLINT,
    pass_fail        CHAR(4),
    hard_bin         SMALLINT,
    soft_bin         SMALLINT,
    num_tests_run    INT,
    test_time_ms     INT,
    part_id          VARCHAR(64),
    attempt_index    TINYINT UNSIGNED DEFAULT 0,
    ingestion_ts     DATETIME,
    ingestion_date   DATE,

    PRIMARY KEY (id),
    UNIQUE KEY uq_parts_device_attempt (device_id, attempt_index),
    KEY idx_parts_file_id     (file_id),
    KEY idx_parts_wafer_id    (wafer_id),
    KEY idx_parts_lot_id      (lot_id),
    KEY idx_parts_xy          (x_coord, y_coord),
    KEY idx_parts_pass_fail   (pass_fail),
    KEY idx_parts_hard_bin    (hard_bin),
    KEY idx_parts_ing_date    (ingestion_date),
    CONSTRAINT fk_parts_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 4.  hardware_sites  (SDR)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hardware_sites (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id          VARCHAR(64)     NOT NULL,
    lot_id           VARCHAR(64),
    head_num         SMALLINT,
    site_grp         SMALLINT,
    site_count       SMALLINT,
    site_nums        VARCHAR(255),
    handler_type     VARCHAR(64),
    handler_id       VARCHAR(64),
    card_type        VARCHAR(64),
    card_id          VARCHAR(64),
    load_type        VARCHAR(64),
    load_id          VARCHAR(64),
    dib_type         VARCHAR(64),
    dib_id           VARCHAR(64),
    cable_type       VARCHAR(64),
    cable_id         VARCHAR(64),
    contactor_type   VARCHAR(64),
    contactor_id     VARCHAR(64),
    laser_type       VARCHAR(64),
    laser_id         VARCHAR(64),
    extra_type       VARCHAR(64),
    extra_id         VARCHAR(64),

    PRIMARY KEY (id),
    KEY idx_hw_sites_file_id  (file_id),
    KEY idx_hw_sites_lot_id   (lot_id),
    CONSTRAINT fk_hw_sites_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 5.  bin_dict  (HBR + SBR)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bin_dict (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id          VARCHAR(64)     NOT NULL,
    lot_id           VARCHAR(64),
    bin_type         CHAR(8),
    bin_num          SMALLINT,
    bin_name         VARCHAR(64),
    bin_count        INT,
    pass_fail        CHAR(4),
    head_num         SMALLINT,
    site_num         SMALLINT,

    PRIMARY KEY (id),
    KEY idx_bin_dict_file_id  (file_id),
    KEY idx_bin_dict_lot_id   (lot_id),
    KEY idx_bin_dict_bin_num  (bin_num),
    KEY idx_bin_dict_type     (bin_type),
    CONSTRAINT fk_bin_dict_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 5a. canonical_test_registry (unique test mappings)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS canonical_test_registry (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    test_name_raw    VARCHAR(255)    NOT NULL,
    canonical_name   VARCHAR(255)    NOT NULL,
    si_unit          VARCHAR(16),
    si_factor        DOUBLE,

    PRIMARY KEY (id),
    UNIQUE KEY uq_canonical_test_name (canonical_name),
    KEY idx_registry_raw (test_name_raw)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- 6.  test_limits  (PTR metadata — unique per file × test_num)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS test_limits (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id          VARCHAR(64)     NOT NULL,
    lot_id           VARCHAR(64),
    test_num         INT             NOT NULL,
    test_name_raw    VARCHAR(255),
    lo_limit_scaled  DOUBLE,
    hi_limit_scaled  DOUBLE,
    orig_unit        VARCHAR(16),
    lo_spec          DOUBLE,
    hi_spec          DOUBLE,
    parser_version   VARCHAR(32),
    ingestion_ts     DATETIME,

    PRIMARY KEY (id),
    UNIQUE KEY uq_test_limits_file_test (file_id, test_num),
    KEY idx_test_limits_raw        (test_name_raw),
    KEY idx_test_limits_lot_id     (lot_id),
    CONSTRAINT fk_test_limits_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 7.  parametric_results  (PTR values — one row per device × test × attempt)
--     Largest table — partitioned by ingestion_date for query performance
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS parametric_results (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    result_id        VARCHAR(128)    NOT NULL,
    device_id        VARCHAR(128),
    wafer_id         VARCHAR(64),
    file_id          VARCHAR(64)     NOT NULL,
    lot_id           VARCHAR(64),
    part_type        VARCHAR(64),
    test_num         INT,
    test_name_raw    VARCHAR(255),
    canonical_name   VARCHAR(255),
    result_raw       DOUBLE,
    result_scaled    DOUBLE,
    result_si        DOUBLE,
    si_unit          VARCHAR(16),
    orig_unit        VARCHAR(16),
    lo_limit_scaled  DOUBLE,
    hi_limit_scaled  DOUBLE,
    passed           TINYINT(1),
    failed_low       TINYINT(1),
    failed_high      TINYINT(1),
    alarm            TINYINT(1),
    not_executed     TINYINT(1),
    attempt_index    TINYINT UNSIGNED DEFAULT 0,
    head_num         SMALLINT,
    site_num         SMALLINT,
    parser_version   VARCHAR(32),
    ingestion_ts     DATETIME,
    ingestion_date   DATE            NOT NULL DEFAULT (CURDATE()),

    PRIMARY KEY (id, ingestion_date),
    UNIQUE KEY uq_param_result_id (result_id, ingestion_date),
    KEY idx_pr_device_id     (device_id),
    KEY idx_pr_file_id       (file_id),
    KEY idx_pr_wafer_id      (wafer_id),
    KEY idx_pr_lot_id        (lot_id),
    KEY idx_pr_test_num      (test_num),
    KEY idx_pr_canonical     (canonical_name),
    KEY idx_pr_passed        (passed),
    KEY idx_pr_ing_date      (ingestion_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE COLUMNS(ingestion_date) (
    PARTITION p2024     VALUES LESS THAN ('2025-01-01'),
    PARTITION p2025_h1  VALUES LESS THAN ('2025-07-01'),
    PARTITION p2025_h2  VALUES LESS THAN ('2026-01-01'),
    PARTITION p2026_h1  VALUES LESS THAN ('2026-07-01'),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);


-- ---------------------------------------------------------------------------
-- 8.  functional_results  (FTR)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS functional_results (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    device_id        VARCHAR(128),
    file_id          VARCHAR(64)     NOT NULL,
    wafer_id         VARCHAR(64),
    lot_id           VARCHAR(64),
    test_num         INT,
    test_name        VARCHAR(255),
    vector_name      VARCHAR(128),
    head_num         SMALLINT,
    site_num         SMALLINT,
    passed           TINYINT(1),
    alarm            TINYINT(1),
    alarm_id         VARCHAR(32),
    cycle_count      BIGINT,
    fail_count       INT,
    repeat_count     INT,
    rel_vect_addr    BIGINT,
    xfail_addr       BIGINT,
    yfail_addr       BIGINT,
    vect_offset      BIGINT,
    time_set         VARCHAR(64),
    op_code          VARCHAR(32),
    attempt_index    TINYINT UNSIGNED DEFAULT 0,
    parser_version   VARCHAR(32),
    ingestion_ts     DATETIME,
    ingestion_date   DATE,

    PRIMARY KEY (id),
    KEY idx_fr_device_id   (device_id),
    KEY idx_fr_file_id     (file_id),
    KEY idx_fr_wafer_id    (wafer_id),
    KEY idx_fr_lot_id      (lot_id),
    KEY idx_fr_passed      (passed),
    KEY idx_fr_ing_date    (ingestion_date),
    CONSTRAINT fk_fr_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 9.  dead_letter_queue  (parse errors)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id          VARCHAR(64)     NOT NULL,
    offset_bytes     BIGINT,
    rec_type         VARCHAR(32),
    reason           VARCHAR(255),
    raw_hex          TEXT,
    exception        TEXT,
    logged_at        DATETIME,

    PRIMARY KEY (id),
    KEY idx_dlq_file_id  (file_id),
    KEY idx_dlq_rec_type (rec_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 10. tsr_summary  (TSR — test summary record)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tsr_summary (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id          VARCHAR(64)     NOT NULL,
    lot_id           VARCHAR(64),
    test_num         INT,
    test_name        VARCHAR(255),
    test_type        CHAR(2),
    head_num         SMALLINT,
    site_num         SMALLINT,
    exec_count       INT,
    fail_count       INT,
    alarm_count      INT,
    avg_exec_time    DOUBLE,
    min_result       DOUBLE,
    max_result       DOUBLE,
    sum_results      DOUBLE,

    PRIMARY KEY (id),
    KEY idx_tsr_file_id    (file_id),
    KEY idx_tsr_lot_id     (lot_id),
    KEY idx_tsr_test_num   (test_num),
    CONSTRAINT fk_tsr_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ---------------------------------------------------------------------------
-- 11. etl_job_log  (idempotency + audit trail for ETL runs)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_job_log (
    id               BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id          VARCHAR(64)     NOT NULL,
    file_hash        VARCHAR(128),
    source_path      VARCHAR(512),
    status           ENUM('RUNNING','SUCCESS','FAILED','SKIPPED') NOT NULL DEFAULT 'RUNNING',
    started_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at      DATETIME,
    rows_lots        INT DEFAULT 0,
    rows_wafers      INT DEFAULT 0,
    rows_parts       INT DEFAULT 0,
    rows_param_res   INT DEFAULT 0,
    rows_func_res    INT DEFAULT 0,
    rows_dlq         INT DEFAULT 0,
    error_message    TEXT,

    PRIMARY KEY (id),
    KEY idx_etl_file_id  (file_id),
    KEY idx_etl_status   (status),
    KEY idx_etl_started  (started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

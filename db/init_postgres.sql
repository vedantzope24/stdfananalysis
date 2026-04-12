-- =============================================================================
-- STDF Analytics — PostgreSQL 15 Schema
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------------------
-- 1. canonical_test_registry
-- ---------------------------------------------------------------------------
CREATE TABLE canonical_test_registry (
    canonical_name VARCHAR(255) PRIMARY KEY,
    test_name_raw_example VARCHAR(255),
    si_unit VARCHAR(16) NOT NULL,
    si_factor DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    orig_unit_examples VARCHAR(255),
    description TEXT,
    pin_id VARCHAR(64),
    pin_role VARCHAR(16),
    map_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_canonical_si_factor CHECK (si_factor > 0),
    CONSTRAINT chk_canonical_pin_role CHECK (pin_role IN ('input','output','sense') OR pin_role IS NULL)
);

-- ---------------------------------------------------------------------------
-- 2. lots
-- ---------------------------------------------------------------------------
CREATE TABLE lots (
    id               BIGSERIAL PRIMARY KEY,
    file_id          VARCHAR(64) NOT NULL UNIQUE,
    file_hash        VARCHAR(128),
    file_name        VARCHAR(255),
    parser_version   VARCHAR(32),
    ingestion_ts     TIMESTAMPTZ,
    tester_timezone  VARCHAR(64),
    
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
    setup_time       TIMESTAMPTZ,
    start_time       TIMESTAMPTZ,
    finish_time      TIMESTAMPTZ,
    disp_code        VARCHAR(8),
    station_num      INTEGER,
    burn_time_min    INTEGER,
    mode_code        VARCHAR(8),
    user_text        TEXT,
    user_desc        TEXT
);

CREATE UNIQUE INDEX idx_lots_file_hash ON lots (file_hash);
CREATE INDEX idx_lots_lot_id ON lots (lot_id);
CREATE INDEX idx_lots_part_type ON lots (part_type);
CREATE INDEX idx_lots_start_time ON lots (start_time);

-- ---------------------------------------------------------------------------
-- 3. wafers
-- ---------------------------------------------------------------------------
CREATE TABLE wafers (
    id               BIGSERIAL PRIMARY KEY,
    file_id          VARCHAR(64) NOT NULL,
    lot_id           VARCHAR(64),
    wafer_id         VARCHAR(64),
    head_num         SMALLINT,
    site_grp         SMALLINT,
    start_time       TIMESTAMPTZ,
    finish_time      TIMESTAMPTZ,
    total_devices    INTEGER,
    pass_count       INTEGER,
    fail_count       INTEGER,
    retest_count     INTEGER,
    abort_count      INTEGER,
    yield_pct        DOUBLE PRECISION,
    dppm             DOUBLE PRECISION,
    ingestion_ts     TIMESTAMPTZ,
    ingestion_date   DATE,
    CONSTRAINT fk_wafers_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT uq_wafers_file_wafer UNIQUE (file_id, wafer_id)
);

CREATE INDEX idx_wafers_lot_id ON wafers (lot_id);
CREATE INDEX idx_wafers_yield_pct ON wafers (yield_pct);
CREATE INDEX idx_wafers_ingestion_date ON wafers (ingestion_date);

-- ---------------------------------------------------------------------------
-- 4. parts
-- ---------------------------------------------------------------------------
CREATE TABLE parts (
    id               BIGSERIAL PRIMARY KEY,
    device_id        VARCHAR(64) NOT NULL,
    file_id          VARCHAR(64) NOT NULL,
    wafer_id         VARCHAR(64),
    lot_id           VARCHAR(64),
    part_type        VARCHAR(64),
    x_coord          SMALLINT,
    y_coord          SMALLINT,
    site_num         SMALLINT,
    head_num         SMALLINT,
    pass_fail        VARCHAR(4),
    hard_bin         SMALLINT,
    soft_bin         SMALLINT,
    num_tests_run    INTEGER,
    test_time_ms     INTEGER,
    part_id          VARCHAR(64),
    attempt_index    SMALLINT DEFAULT 0,
    ingestion_ts     TIMESTAMPTZ,
    ingestion_date   DATE,
    CONSTRAINT fk_parts_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT uq_parts_device_attempt UNIQUE (device_id, attempt_index)
);

CREATE INDEX idx_parts_xy ON parts (x_coord, y_coord);
CREATE INDEX idx_parts_pass_fail ON parts (pass_fail);
CREATE INDEX idx_parts_hard_bin ON parts (hard_bin);
CREATE INDEX idx_parts_wafer_id ON parts (wafer_id);

-- ---------------------------------------------------------------------------
-- 5. hardware_sites
-- ---------------------------------------------------------------------------
CREATE TABLE hardware_sites (
    id               BIGSERIAL PRIMARY KEY,
    file_id          VARCHAR(64) NOT NULL,
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
    CONSTRAINT fk_hw_sites_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- ---------------------------------------------------------------------------
-- 6. test_limits
-- ---------------------------------------------------------------------------
CREATE TABLE test_limits (
    id               BIGSERIAL PRIMARY KEY,
    file_id          VARCHAR(64) NOT NULL,
    lot_id           VARCHAR(64),
    test_num         INTEGER NOT NULL,
    test_name_raw    VARCHAR(255),
    canonical_name   VARCHAR(255),
    lo_limit_raw     DOUBLE PRECISION,
    hi_limit_raw     DOUBLE PRECISION,
    orig_unit        VARCHAR(16),
    lo_limit_scaled  DOUBLE PRECISION,
    hi_limit_scaled  DOUBLE PRECISION,
    lo_spec          DOUBLE PRECISION,
    hi_spec          DOUBLE PRECISION,
    parser_version   VARCHAR(32),
    ingestion_ts     TIMESTAMPTZ,
    CONSTRAINT fk_test_limits_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_test_limits_canonical FOREIGN KEY (canonical_name)
        REFERENCES canonical_test_registry (canonical_name) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT uq_test_limits_file_test UNIQUE (file_id, test_num)
);

-- ---------------------------------------------------------------------------
-- 7. bin_dict
-- ---------------------------------------------------------------------------
CREATE TABLE bin_dict (
    id               BIGSERIAL PRIMARY KEY,
    file_id          VARCHAR(64) NOT NULL,
    lot_id           VARCHAR(64),
    bin_type         VARCHAR(8),
    bin_num          SMALLINT,
    bin_name         VARCHAR(64),
    bin_count        INTEGER,
    pass_fail        VARCHAR(4),
    head_num         SMALLINT,
    site_num         SMALLINT,
    CONSTRAINT fk_bin_dict_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- ---------------------------------------------------------------------------
-- 8. parametric_results
-- ---------------------------------------------------------------------------
CREATE TABLE parametric_results (
    id               BIGSERIAL PRIMARY KEY,
    result_id        VARCHAR(128) NOT NULL,
    device_id        VARCHAR(64),
    wafer_id         VARCHAR(64),
    file_id          VARCHAR(64) NOT NULL,
    lot_id           VARCHAR(64),
    part_type        VARCHAR(64),
    test_num         INTEGER,
    test_name_raw    VARCHAR(255),
    canonical_name   VARCHAR(255),
    result_raw       DOUBLE PRECISION,
    result_scaled    DOUBLE PRECISION,
    result_si        DOUBLE PRECISION,
    si_unit          VARCHAR(16),
    orig_unit        VARCHAR(16),
    lo_limit_raw     DOUBLE PRECISION,
    hi_limit_raw     DOUBLE PRECISION,
    lo_limit_scaled  DOUBLE PRECISION,
    hi_limit_scaled  DOUBLE PRECISION,
    passed           SMALLINT,
    failed_low       SMALLINT,
    failed_high      SMALLINT,
    alarm            SMALLINT,
    not_executed     SMALLINT,
    attempt_index    SMALLINT DEFAULT 0,
    head_num         SMALLINT,
    site_num         SMALLINT,
    parser_version   VARCHAR(32),
    ingestion_ts     TIMESTAMPTZ,
    ingestion_date   DATE NOT NULL DEFAULT CURRENT_DATE,
    CONSTRAINT fk_pr_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_pr_canonical FOREIGN KEY (canonical_name)
        REFERENCES canonical_test_registry (canonical_name) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT uq_param_result_id UNIQUE (result_id)
);

CREATE INDEX idx_pr_device_test ON parametric_results (device_id, test_num);
CREATE INDEX idx_pr_canonical ON parametric_results (canonical_name);
CREATE INDEX idx_pr_passed ON parametric_results (passed);
CREATE INDEX idx_pr_ingestion_date ON parametric_results (ingestion_date);

-- ---------------------------------------------------------------------------
-- 9. functional_results
-- ---------------------------------------------------------------------------
CREATE TABLE functional_results (
    id               BIGSERIAL PRIMARY KEY,
    device_id        VARCHAR(64),
    file_id          VARCHAR(64) NOT NULL,
    wafer_id         VARCHAR(64),
    lot_id           VARCHAR(64),
    test_num         INTEGER,
    test_name        VARCHAR(255),
    vector_name      VARCHAR(128),
    head_num         SMALLINT,
    site_num         SMALLINT,
    passed           SMALLINT,
    alarm            SMALLINT,
    alarm_id         VARCHAR(32),
    cycle_count      BIGINT,
    fail_count       INTEGER,
    repeat_count     INTEGER,
    rel_vect_addr    BIGINT,
    xfail_addr       BIGINT,
    yfail_addr       BIGINT,
    vect_offset      BIGINT,
    time_set         VARCHAR(64),
    op_code          VARCHAR(32),
    attempt_index    SMALLINT DEFAULT 0,
    parser_version   VARCHAR(32),
    ingestion_ts     TIMESTAMPTZ,
    ingestion_date   DATE,
    CONSTRAINT fk_fr_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- ---------------------------------------------------------------------------
-- 10. tsr_summary
-- ---------------------------------------------------------------------------
CREATE TABLE tsr_summary (
    id               BIGSERIAL PRIMARY KEY,
    file_id          VARCHAR(64) NOT NULL,
    lot_id           VARCHAR(64),
    test_num         INTEGER,
    test_name        VARCHAR(255),
    test_type        VARCHAR(2),
    head_num         SMALLINT,
    site_num         SMALLINT,
    exec_count       INTEGER,
    fail_count       INTEGER,
    alarm_count      INTEGER,
    avg_exec_time    DOUBLE PRECISION,
    min_result       DOUBLE PRECISION,
    max_result       DOUBLE PRECISION,
    sum_results      DOUBLE PRECISION,
    CONSTRAINT fk_tsr_file FOREIGN KEY (file_id)
        REFERENCES lots (file_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- ---------------------------------------------------------------------------
-- 11. dead_letter_queue
-- ---------------------------------------------------------------------------
CREATE TABLE dead_letter_queue (
    id               BIGSERIAL PRIMARY KEY,
    file_id          VARCHAR(64) NOT NULL,
    offset_bytes     BIGINT,
    rec_type         VARCHAR(32),
    reason           VARCHAR(255),
    raw_hex          TEXT,
    exception        TEXT,
    logged_at        TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- 12. etl_job_log
-- ---------------------------------------------------------------------------
CREATE TABLE etl_job_log (
    id               BIGSERIAL PRIMARY KEY,
    file_id          VARCHAR(64) NOT NULL,
    file_hash        VARCHAR(128),
    source_path      VARCHAR(512),
    status           VARCHAR(32) NOT NULL DEFAULT 'RUNNING',
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    rows_lots        INTEGER DEFAULT 0,
    rows_wafers      INTEGER DEFAULT 0,
    rows_parts       INTEGER DEFAULT 0,
    rows_param_res   INTEGER DEFAULT 0,
    rows_func_res    INTEGER DEFAULT 0,
    rows_dlq         INTEGER DEFAULT 0,
    error_message    TEXT,
    ch_load_status   VARCHAR(16)
);

CREATE UNIQUE INDEX idx_etl_success ON etl_job_log(file_hash) WHERE status = 'SUCCESS';
CREATE INDEX idx_etl_file_hash ON etl_job_log (file_hash);
CREATE INDEX idx_etl_status ON etl_job_log (status);

-- ---------------------------------------------------------------------------
-- 13. users
-- ---------------------------------------------------------------------------
CREATE TABLE users (
    user_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email            VARCHAR(255) UNIQUE NOT NULL,
    hashed_password  TEXT NOT NULL,
    role             VARCHAR(32) NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login       TIMESTAMPTZ,
    CONSTRAINT chk_users_role CHECK (role IN ('admin','yield_engineer','data_scientist','viewer'))
);

-- ---------------------------------------------------------------------------
-- 14. audit_log
-- ---------------------------------------------------------------------------
CREATE TABLE audit_log (
    id               BIGSERIAL PRIMARY KEY,
    user_id          UUID NOT NULL,
    action           VARCHAR(64),
    table_name       VARCHAR(64),
    record_id        VARCHAR(128),
    changed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ip_address       INET,
    query_text       TEXT,
    CONSTRAINT fk_audit_log_user FOREIGN KEY (user_id)
        REFERENCES users (user_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- ---------------------------------------------------------------------------
-- DB ROLES
-- ---------------------------------------------------------------------------
-- Note: Roles should ideally be created globally, but this handles initial init.
-- TODO: set sslmode=require for production

DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'stdf_app') THEN
        CREATE ROLE stdf_app;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'stdf_readonly') THEN
        CREATE ROLE stdf_readonly;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'stdf_etl') THEN
        CREATE ROLE stdf_etl;
    END IF;
END
$$;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO stdf_app;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO stdf_readonly;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO stdf_etl;
GRANT INSERT, UPDATE ON lots, wafers, parts, hardware_sites, test_limits, bin_dict, canonical_test_registry, parametric_results, functional_results, tsr_summary, dead_letter_queue, etl_job_log TO stdf_etl;

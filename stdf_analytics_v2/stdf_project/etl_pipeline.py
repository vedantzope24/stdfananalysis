"""
etl_pipeline.py
===============
ETL pipeline: STDF file  →  parse  →  MySQL

Reads one or more STDF/ATDF files, calls the existing parser (export_parquet.py
logic), then loads every table into MySQL using idempotent upsert logic.

USAGE
-----
  # Single file
  python etl_pipeline.py path/to/file.stdf

  # Whole directory (recursive)
  python etl_pipeline.py path/to/stdf_folder/ --recursive

  # Use a custom DB config file
  python etl_pipeline.py file.stdf --config db_config.json

  # Dry-run (parse + validate, no DB writes)
  python etl_pipeline.py file.stdf --dry-run

DATABASE CONFIG (db_config.json or env-vars)
--------------------------------------------
{
  "host":     "127.0.0.1",
  "port":     3306,
  "user":     "stdf_user",
  "password": "secret",
  "database": "stdf_analytics"
}

Environment variables override the JSON config:
  STDF_DB_HOST, STDF_DB_PORT, STDF_DB_USER, STDF_DB_PASSWORD, STDF_DB_NAME

REQUIREMENTS
------------
  pip install mysql-connector-python pandas pyarrow
  (pyarrow already required by the existing parser)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Make sure project root is importable ─────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("stdf_etl")


# =============================================================================
# CONFIG
# =============================================================================

DEFAULT_CONFIG: dict[str, Any] = {
    "host":     "127.0.0.1",
    "port":     3306,
    "user":     "root",
    "password": "root123",
    "database": "stdf_analytics",
}

STDF_EXTENSIONS = {".stdf", ".std", ".stdf.gz", ".std.gz", ".zip"}


def load_config(config_path: str | None = None) -> dict[str, Any]:
    """Load DB config from JSON file + override with env-vars."""
    cfg = DEFAULT_CONFIG.copy()

    if config_path and Path(config_path).exists():
        with open(config_path) as f:
            cfg.update(json.load(f))

    # Env-var overrides
    cfg["host"]     = os.environ.get("STDF_DB_HOST",     cfg["host"])
    cfg["port"]     = int(os.environ.get("STDF_DB_PORT", cfg["port"]))
    cfg["user"]     = os.environ.get("STDF_DB_USER",     cfg["user"])
    cfg["password"] = os.environ.get("STDF_DB_PASSWORD", cfg["password"])
    cfg["database"] = os.environ.get("STDF_DB_NAME",     cfg["database"])
    return cfg


# =============================================================================
# DB CONNECTION
# =============================================================================

def get_connection(cfg: dict):
    """Return a mysql-connector-python connection."""
    try:
        import mysql.connector
    except ImportError:
        log.error("mysql-connector-python not installed. Run: pip install mysql-connector-python")
        sys.exit(1)

    return mysql.connector.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        autocommit=False,
        connection_timeout=30,
    )


# =============================================================================
# HELPERS
# =============================================================================

def _safe(val):
    """Convert pandas NA / NaN / 'None' strings to Python None for MySQL."""
    import math
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    s = str(val)
    if s in ("", "None", "nan", "NaN", "<NA>", "NaT"):
        return None
    return val


def _parse_dt(val) -> str | None:
    """Return ISO datetime string or None."""
    v = _safe(val)
    if v is None:
        return None
    s = str(v).strip()
    # Already ISO-ish
    if "T" in s or "-" in s:
        return s[:19].replace("T", " ")  # MySQL DATETIME format
    return s or None


def _parse_date(val) -> str | None:
    """Return YYYY-MM-DD string or None."""
    v = _safe(val)
    if v is None:
        return None
    s = str(v).strip()[:10]
    return s if len(s) == 10 else None


def _int(val) -> int | None:
    v = _safe(val)
    if v is None:
        return None
    try:
        return int(float(str(v)))
    except (ValueError, TypeError):
        return None


def _float(val) -> float | None:
    v = _safe(val)
    if v is None:
        return None
    try:
        f = float(str(v))
        import math
        return None if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return None


def _bool(val) -> int | None:
    """Return 1/0/None (MySQL TINYINT)."""
    v = _safe(val)
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    s = str(v).lower().strip()
    if s in ("true", "1", "yes"):
        return 1
    if s in ("false", "0", "no"):
        return 0
    return None


def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# =============================================================================
# UPSERT FUNCTIONS  (one per table)
# =============================================================================

def _exec_many(cursor, sql: str, rows: list):
    """Execute many rows; return insert count."""
    if not rows:
        return 0
    cursor.executemany(sql, rows)
    return cursor.rowcount


# ─── 1. lots ─────────────────────────────────────────────────────────────────

LOTS_SQL = """
INSERT INTO lots (
    file_id, file_hash, file_name, parser_version, ingestion_ts,
    lot_id, sublot_id, part_type, family_id, pkg_type,
    process_id, design_rev, date_code, job_name, job_rev,
    test_code, test_temp, flow_id, operator, supervisor,
    node_name, tester_type, exec_type, exec_ver, facility_id,
    floor_id, serial_num, eng_id,
    setup_time, start_time, finish_time,
    disp_code, station_num, burn_time_min, mode_code, user_text, user_desc
) VALUES (
    %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,
    %s,%s,%s, %s,%s,%s,%s,%s,%s
)
ON DUPLICATE KEY UPDATE
    file_hash      = VALUES(file_hash),
    ingestion_ts   = VALUES(ingestion_ts),
    finish_time    = VALUES(finish_time),
    operator       = VALUES(operator)
"""


def load_lots(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("file_id")),
            _safe(r.get("file_hash")),
            _safe(r.get("file_name")),
            _safe(r.get("parser_version")),
            _parse_dt(r.get("ingestion_ts")),
            _safe(r.get("lot_id")),
            _safe(r.get("sublot_id")),
            _safe(r.get("part_type")),
            _safe(r.get("family_id")),
            _safe(r.get("pkg_type")),
            _safe(r.get("process_id")),
            _safe(r.get("design_rev")),
            _safe(r.get("date_code")),
            _safe(r.get("job_name")),
            _safe(r.get("job_rev")),
            _safe(r.get("test_code")),
            _safe(r.get("test_temp")),
            _safe(r.get("flow_id")),
            _safe(r.get("operator")),
            _safe(r.get("supervisor")),
            _safe(r.get("node_name")),
            _safe(r.get("tester_type")),
            _safe(r.get("exec_type")),
            _safe(r.get("exec_ver")),
            _safe(r.get("facility_id")),
            _safe(r.get("floor_id")),
            _safe(r.get("serial_num")),
            _safe(r.get("eng_id")),
            _parse_dt(r.get("setup_time")),
            _parse_dt(r.get("start_time")),
            _parse_dt(r.get("finish_time")),
            _safe(r.get("disp_code")),
            _int(r.get("station_num")),
            _int(r.get("burn_time_min")),
            _safe(r.get("mode_code")),
            _safe(r.get("user_text")),
            _safe(r.get("user_desc")),
        ))
    return _exec_many(cursor, LOTS_SQL, data)


# ─── 2. wafers ───────────────────────────────────────────────────────────────

WAFERS_SQL = """
INSERT INTO wafers (
    file_id, lot_id, wafer_id, head_num, site_grp,
    start_time, finish_time,
    total_devices, pass_count, fail_count, retest_count, abort_count,
    yield_pct, dppm, ingestion_ts, ingestion_date
) VALUES (
    %s,%s,%s,%s,%s, %s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s
)
ON DUPLICATE KEY UPDATE
    total_devices = VALUES(total_devices),
    pass_count    = VALUES(pass_count),
    fail_count    = VALUES(fail_count),
    yield_pct     = VALUES(yield_pct),
    dppm          = VALUES(dppm),
    finish_time   = VALUES(finish_time)
"""


def load_wafers(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("file_id")),
            _safe(r.get("lot_id")),
            _safe(r.get("wafer_id")),
            _int(r.get("head_num")),
            _int(r.get("site_grp")),
            _parse_dt(r.get("start_time")),
            _parse_dt(r.get("finish_time")),
            _int(r.get("total_devices")),
            _int(r.get("pass_count")),
            _int(r.get("fail_count")),
            _int(r.get("retest_count")),
            _int(r.get("abort_count")),
            _float(r.get("yield_pct")),
            _float(r.get("dppm")),
            _parse_dt(r.get("ingestion_ts")),
            _parse_date(r.get("ingestion_date")),
        ))
    return _exec_many(cursor, WAFERS_SQL, data)


# ─── 3. parts ────────────────────────────────────────────────────────────────

PARTS_SQL = """
INSERT INTO parts (
    device_id, file_id, wafer_id, lot_id, part_type,
    x_coord, y_coord, site_num, head_num,
    pass_fail, hard_bin, soft_bin, num_tests_run, test_time_ms,
    part_id, attempt_index, ingestion_ts, ingestion_date
) VALUES (
    %s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s
)
ON DUPLICATE KEY UPDATE
    pass_fail     = VALUES(pass_fail),
    hard_bin      = VALUES(hard_bin),
    soft_bin      = VALUES(soft_bin),
    num_tests_run = VALUES(num_tests_run),
    test_time_ms  = VALUES(test_time_ms)
"""


def load_parts(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("device_id")),
            _safe(r.get("file_id")),
            _safe(r.get("wafer_id")),
            _safe(r.get("lot_id")),
            _safe(r.get("part_type")),
            _int(r.get("x_coord")),
            _int(r.get("y_coord")),
            _int(r.get("site_num")),
            _int(r.get("head_num")),
            _safe(r.get("pass_fail")),
            _int(r.get("hard_bin")),
            _int(r.get("soft_bin")),
            _int(r.get("num_tests_run")),
            _int(r.get("test_time_ms")),
            _safe(r.get("part_id")),
            _int(r.get("attempt_index")) or 0,
            _parse_dt(r.get("ingestion_ts")),
            _parse_date(r.get("ingestion_date")),
        ))
    return _exec_many(cursor, PARTS_SQL, data)


# ─── 4. hardware_sites ───────────────────────────────────────────────────────

HW_SITES_SQL = """
INSERT INTO hardware_sites (
    file_id, lot_id, head_num, site_grp, site_count, site_nums,
    handler_type, handler_id, card_type, card_id,
    load_type, load_id, dib_type, dib_id,
    cable_type, cable_id, contactor_type, contactor_id,
    laser_type, laser_id, extra_type, extra_id
) VALUES (
    %s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,
    %s,%s,%s,%s, %s,%s,%s,%s
)
"""


def load_hardware_sites(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("file_id")),
            _safe(r.get("lot_id")),
            _int(r.get("head_num")),
            _int(r.get("site_grp")),
            _int(r.get("site_count")),
            _safe(r.get("site_nums")),
            _safe(r.get("handler_type")), _safe(r.get("handler_id")),
            _safe(r.get("card_type")),    _safe(r.get("card_id")),
            _safe(r.get("load_type")),    _safe(r.get("load_id")),
            _safe(r.get("dib_type")),     _safe(r.get("dib_id")),
            _safe(r.get("cable_type")),   _safe(r.get("cable_id")),
            _safe(r.get("contactor_type")),_safe(r.get("contactor_id")),
            _safe(r.get("laser_type")),   _safe(r.get("laser_id")),
            _safe(r.get("extra_type")),   _safe(r.get("extra_id")),
        ))
    return _exec_many(cursor, HW_SITES_SQL, data)


# ─── 5. bin_dict ─────────────────────────────────────────────────────────────

BIN_DICT_SQL = """
INSERT INTO bin_dict (
    file_id, lot_id, bin_type, bin_num, bin_name,
    bin_count, pass_fail, head_num, site_num
) VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s)
"""


def load_bin_dict(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("file_id")),
            _safe(r.get("lot_id")),
            _safe(r.get("bin_type")),
            _int(r.get("bin_num")),
            _safe(r.get("bin_name")),
            _int(r.get("bin_count")),
            _safe(r.get("pass_fail")),
            _int(r.get("head_num")),
            _int(r.get("site_num")),
        ))
    return _exec_many(cursor, BIN_DICT_SQL, data)


# ─── 6. test_limits ──────────────────────────────────────────────────────────

TEST_LIMITS_SQL = """
INSERT INTO test_limits (
    file_id, lot_id, test_num, test_name_raw, canonical_name,
    lo_limit_scaled, hi_limit_scaled, orig_unit, si_unit, si_factor,
    lo_spec, hi_spec, parser_version, ingestion_ts
) VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    lo_limit_scaled  = VALUES(lo_limit_scaled),
    hi_limit_scaled  = VALUES(hi_limit_scaled),
    canonical_name   = VALUES(canonical_name)
"""


def load_test_limits(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("file_id")),
            _safe(r.get("lot_id")),
            _int(r.get("test_num")),
            _safe(r.get("test_name_raw")),
            _safe(r.get("canonical_name")),
            _float(r.get("lo_limit_scaled")),
            _float(r.get("hi_limit_scaled")),
            _safe(r.get("orig_unit")),
            _safe(r.get("si_unit")),
            _float(r.get("si_factor")),
            _float(r.get("lo_spec")),
            _float(r.get("hi_spec")),
            _safe(r.get("parser_version")),
            _parse_dt(r.get("ingestion_ts")),
        ))
    return _exec_many(cursor, TEST_LIMITS_SQL, data)


# ─── 7. parametric_results ───────────────────────────────────────────────────

PARAM_RESULTS_SQL = """
INSERT INTO parametric_results (
    result_id, device_id, wafer_id, file_id, lot_id, part_type,
    test_num, test_name_raw, canonical_name,
    result_raw, result_scaled, result_si, si_unit, orig_unit,
    lo_limit_scaled, hi_limit_scaled,
    passed, failed_low, failed_high, alarm, not_executed,
    attempt_index, head_num, site_num,
    parser_version, ingestion_ts, ingestion_date
) VALUES (
    %s,%s,%s,%s,%s,%s,
    %s,%s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,%s,
    %s,%s,%s
)
ON DUPLICATE KEY UPDATE
    passed       = VALUES(passed),
    result_si    = VALUES(result_si),
    result_scaled= VALUES(result_scaled)
"""

_PARAM_BATCH = 2_000   # rows per executemany call (keeps memory bounded)


def load_parametric_results(cursor, rows: list) -> int:
    total = 0
    batch = []
    for r in rows:
        ing_date = _parse_date(r.get("ingestion_date")) or datetime.now().strftime("%Y-%m-%d")
        batch.append((
            _safe(r.get("result_id")),
            _safe(r.get("device_id")),
            _safe(r.get("wafer_id")),
            _safe(r.get("file_id")),
            _safe(r.get("lot_id")),
            _safe(r.get("part_type")),
            _int(r.get("test_num")),
            _safe(r.get("test_name_raw")),
            _safe(r.get("canonical_name")),
            _float(r.get("result_raw")),
            _float(r.get("result_scaled")),
            _float(r.get("result_si")),
            _safe(r.get("si_unit")),
            _safe(r.get("orig_unit")),
            _float(r.get("lo_limit_scaled")),
            _float(r.get("hi_limit_scaled")),
            _bool(r.get("passed")),
            _bool(r.get("failed_low")),
            _bool(r.get("failed_high")),
            _bool(r.get("alarm")),
            _bool(r.get("not_executed")),
            _int(r.get("attempt_index")) or 0,
            _int(r.get("head_num")),
            _int(r.get("site_num")),
            _safe(r.get("parser_version")),
            _parse_dt(r.get("ingestion_ts")),
            ing_date,
        ))
        if len(batch) >= _PARAM_BATCH:
            total += _exec_many(cursor, PARAM_RESULTS_SQL, batch)
            batch = []
    if batch:
        total += _exec_many(cursor, PARAM_RESULTS_SQL, batch)
    return total


# ─── 8. functional_results ───────────────────────────────────────────────────

FUNC_RESULTS_SQL = """
INSERT INTO functional_results (
    device_id, file_id, wafer_id, lot_id,
    test_num, test_name, vector_name, head_num, site_num,
    passed, alarm, alarm_id,
    cycle_count, fail_count, repeat_count,
    rel_vect_addr, xfail_addr, yfail_addr, vect_offset,
    time_set, op_code, attempt_index,
    parser_version, ingestion_ts, ingestion_date
) VALUES (
    %s,%s,%s,%s,
    %s,%s,%s,%s,%s,
    %s,%s,%s,
    %s,%s,%s,
    %s,%s,%s,%s,
    %s,%s,%s,
    %s,%s,%s
)
"""


def load_functional_results(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("device_id")),
            _safe(r.get("file_id")),
            _safe(r.get("wafer_id")),
            _safe(r.get("lot_id")),
            _int(r.get("test_num")),
            _safe(r.get("test_name")),
            _safe(r.get("vector_name")),
            _int(r.get("head_num")),
            _int(r.get("site_num")),
            _bool(r.get("passed")),
            _bool(r.get("alarm")),
            _safe(r.get("alarm_id")),
            _int(r.get("cycle_count")),
            _int(r.get("fail_count")),
            _int(r.get("repeat_count")),
            _int(r.get("rel_vect_addr")),
            _int(r.get("xfail_addr")),
            _int(r.get("yfail_addr")),
            _int(r.get("vect_offset")),
            _safe(r.get("time_set")),
            _safe(r.get("op_code")),
            _int(r.get("attempt_index")) or 0,
            _safe(r.get("parser_version")),
            _parse_dt(r.get("ingestion_ts")),
            _parse_date(r.get("ingestion_date")),
        ))
    return _exec_many(cursor, FUNC_RESULTS_SQL, data)


# ─── 9. dead_letter_queue ────────────────────────────────────────────────────

DLQ_SQL = """
INSERT INTO dead_letter_queue (
    file_id, offset_bytes, rec_type, reason, raw_hex, exception, logged_at
) VALUES (%s,%s,%s,%s,%s,%s,%s)
"""


def load_dead_letter_queue(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("file_id")),
            _int(r.get("offset")),
            _safe(r.get("rec_type")),
            _safe(r.get("reason")),
            _safe(r.get("raw_hex")),
            _safe(r.get("exception")),
            _parse_dt(r.get("logged_at")),
        ))
    return _exec_many(cursor, DLQ_SQL, data)


# ─── 10. tsr_summary ─────────────────────────────────────────────────────────

TSR_SQL = """
INSERT INTO tsr_summary (
    file_id, lot_id, test_num, test_name, test_type,
    head_num, site_num,
    exec_count, fail_count, alarm_count,
    avg_exec_time, min_result, max_result, sum_results
) VALUES (%s,%s,%s,%s,%s, %s,%s, %s,%s,%s, %s,%s,%s,%s)
"""


def load_tsr_summary(cursor, rows: list) -> int:
    data = []
    for r in rows:
        data.append((
            _safe(r.get("file_id")),
            _safe(r.get("lot_id")),
            _int(r.get("test_num")),
            _safe(r.get("test_name")),
            _safe(r.get("test_type")),
            _int(r.get("head_num")),
            _int(r.get("site_num")),
            _int(r.get("exec_count")),
            _int(r.get("fail_count")),
            _int(r.get("alarm_count")),
            _float(r.get("avg_exec_time")),
            _float(r.get("min_result")),
            _float(r.get("max_result")),
            _float(r.get("sum_results")),
        ))
    return _exec_many(cursor, TSR_SQL, data)


# =============================================================================
# JOB LOG HELPERS
# =============================================================================

JOB_INSERT_SQL = """
INSERT INTO etl_job_log (file_id, file_hash, source_path, status, started_at)
VALUES (%s, %s, %s, 'RUNNING', %s)
"""

JOB_UPDATE_SQL = """
UPDATE etl_job_log
SET status=%s, finished_at=%s,
    rows_lots=%s, rows_wafers=%s, rows_parts=%s,
    rows_param_res=%s, rows_func_res=%s, rows_dlq=%s,
    error_message=%s
WHERE id=%s
"""


def _log_job_start(cursor, file_id: str, file_hash: str, source_path: str) -> int:
    cursor.execute(JOB_INSERT_SQL, (file_id, file_hash, source_path, now_utc_str()))
    return cursor.lastrowid


def _log_job_finish(cursor, job_id: int, status: str, counts: dict, error: str | None = None):
    cursor.execute(JOB_UPDATE_SQL, (
        status,
        now_utc_str(),
        counts.get("lots", 0),
        counts.get("wafers", 0),
        counts.get("parts", 0),
        counts.get("parametric_results", 0),
        counts.get("functional_results", 0),
        counts.get("dead_letter_queue", 0),
        error,
        job_id,
    ))


# =============================================================================
# IDEMPOTENCY CHECK
# =============================================================================

def is_already_loaded(cursor, file_id: str) -> bool:
    """Return True if this file_id was previously loaded successfully."""
    cursor.execute(
        "SELECT id FROM etl_job_log WHERE file_id=%s AND status='SUCCESS' LIMIT 1",
        (file_id,)
    )
    return cursor.fetchone() is not None


# =============================================================================
# MAIN ETL FUNCTION
# =============================================================================

def run_etl_for_file(
    stdf_path: str,
    cfg: dict,
    dry_run: bool = False,
    force: bool = False,
) -> bool:
    """
    Parse one STDF file and load all tables into MySQL.

    Parameters
    ----------
    stdf_path : path to .stdf / .stdf.gz
    cfg       : DB config dict
    dry_run   : parse but do not write to DB
    force     : reload even if file_id already in DB

    Returns True on success.
    """
    from parser.stdf_parser import parse  # noqa: local import

    stdf_path = str(stdf_path)
    log.info("─" * 60)
    log.info(f"Processing: {stdf_path}")

    # ── 1. Parse ─────────────────────────────────────────────────────────────
    try:
        result = parse(stdf_path, verbose=False)
    except Exception as exc:
        log.error(f"PARSE FAILED: {exc}")
        return False

    file_id   = result["file_id"]
    file_hash = result.get("file_hash", "")
    log.info(f"file_id = {file_id}  |  hash = {file_hash[:16]}...")

    # ── 2. Dry-run ────────────────────────────────────────────────────────────
    if dry_run:
        _print_dry_run_summary(result)
        return True

    # ── 3. Connect ───────────────────────────────────────────────────────────
    conn = get_connection(cfg)
    cursor = conn.cursor()

    try:
        # ── 4. Idempotency check ──────────────────────────────────────────────
        if not force and is_already_loaded(cursor, file_id):
            log.info(f"SKIPPED — {file_id} already loaded (use --force to reload)")
            cursor.close()
            conn.close()
            return True

        # ── 5. Open job log ───────────────────────────────────────────────────
        job_id = _log_job_start(cursor, file_id, file_hash, stdf_path)
        conn.commit()

        counts: dict[str, int] = {}

        # ── 6. Load tables in FK order ────────────────────────────────────────
        log.info("  Loading lots …")
        counts["lots"] = load_lots(cursor, result["lots"])

        log.info("  Loading wafers …")
        counts["wafers"] = load_wafers(cursor, result["wafers"])

        log.info("  Loading parts …")
        counts["parts"] = load_parts(cursor, result["parts"])

        log.info("  Loading hardware_sites …")
        counts["hardware_sites"] = load_hardware_sites(cursor, result["hardware_sites"])

        log.info("  Loading bin_dict …")
        counts["bin_dict"] = load_bin_dict(cursor, result["bin_dict"])

        log.info("  Loading test_limits …")
        counts["test_limits"] = load_test_limits(cursor, result["test_limits"])

        log.info(f"  Loading parametric_results ({len(result['parametric_results']):,} rows) …")
        counts["parametric_results"] = load_parametric_results(cursor, result["parametric_results"])

        log.info("  Loading functional_results …")
        counts["functional_results"] = load_functional_results(cursor, result["functional_results"])

        log.info("  Loading dead_letter_queue …")
        counts["dead_letter_queue"] = load_dead_letter_queue(cursor, result["dead_letter_queue"])

        log.info("  Loading tsr_summary …")
        counts["tsr_summary"] = load_tsr_summary(cursor, result["tsr_summary"])

        # ── 7. Commit ─────────────────────────────────────────────────────────
        _log_job_finish(cursor, job_id, "SUCCESS", counts)
        conn.commit()

        log.info(f"  ✓ SUCCESS  {sum(counts.values()):,} total rows committed")
        for table, cnt in counts.items():
            log.info(f"      {table:<30} {cnt:>8,} rows")

        return True

    except Exception as exc:
        conn.rollback()
        err = traceback.format_exc()
        log.error(f"ETL FAILED for {file_id}: {exc}")
        try:
            _log_job_finish(cursor, job_id, "FAILED", {}, error=str(exc)[:4000])
            conn.commit()
        except Exception:
            pass
        return False

    finally:
        cursor.close()
        conn.close()


def _print_dry_run_summary(result: dict):
    log.info("  [DRY-RUN] Table row counts:")
    tables = [
        "lots","wafers","parts","hardware_sites","bin_dict",
        "test_limits","parametric_results","functional_results",
        "dead_letter_queue","tsr_summary"
    ]
    for t in tables:
        log.info(f"    {t:<30} {len(result.get(t,[])):>8,} rows")


# =============================================================================
# BATCH PROCESSING
# =============================================================================

def find_stdf_files(path: str, recursive: bool = False) -> list[Path]:
    """Return list of STDF file paths under a directory (or single file)."""
    p = Path(path)
    if p.is_file():
        return [p]
    if not p.is_dir():
        log.error(f"Path not found: {path}")
        return []

    files = []
    glob_fn = p.rglob if recursive else p.glob
    for ext in (".stdf", ".std"):
        files.extend(glob_fn(f"*{ext}"))
        files.extend(glob_fn(f"*{ext}.gz"))
    return sorted(set(files))


def run_batch(
    path: str,
    cfg: dict,
    dry_run: bool = False,
    force: bool = False,
    recursive: bool = False,
) -> dict[str, bool]:
    """Process all STDF files in a directory. Returns {filename: success}."""
    files = find_stdf_files(path, recursive=recursive)
    if not files:
        log.warning(f"No STDF files found under: {path}")
        return {}

    log.info(f"Found {len(files)} STDF file(s) to process")
    results = {}
    for f in files:
        ok = run_etl_for_file(str(f), cfg, dry_run=dry_run, force=force)
        results[str(f)] = ok

    passed = sum(v for v in results.values())
    log.info(f"\nBatch complete: {passed}/{len(files)} succeeded")
    return results


# =============================================================================
# CLI
# =============================================================================

def main():
    ap = argparse.ArgumentParser(
        description="STDF → MySQL ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python etl_pipeline.py demofile.stdf
  python etl_pipeline.py ./stdf_folder/ --recursive
  python etl_pipeline.py file.stdf --config db_config.json
  python etl_pipeline.py file.stdf --dry-run
  python etl_pipeline.py file.stdf --force
        """,
    )
    ap.add_argument("path",
                    help="Path to .stdf file or directory containing STDF files")
    ap.add_argument("--config",     default=None,
                    help="Path to JSON config file with DB credentials")
    ap.add_argument("--dry-run",    action="store_true",
                    help="Parse but do not write to database")
    ap.add_argument("--force",      action="store_true",
                    help="Re-load even if file already imported")
    ap.add_argument("--recursive",  action="store_true",
                    help="Search sub-directories for STDF files")
    ap.add_argument("--verbose",    action="store_true",
                    help="Enable DEBUG logging")
    args = ap.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cfg = load_config(args.config)

    if args.dry_run:
        log.info("DRY-RUN mode — no database writes")

    results = run_batch(
        path=args.path,
        cfg=cfg,
        dry_run=args.dry_run,
        force=args.force,
        recursive=args.recursive,
    )

    # Exit with non-zero if any file failed
    if results and not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()

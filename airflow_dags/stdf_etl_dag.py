import io
import os
import re
import csv
import json
import time
import hashlib
from datetime import timezone
from typing import Dict, Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.dates import days_ago

import psycopg2
import pandas as pd
from clickhouse_driver import Client

# Import canonicalization rules from the parser utils as strictly contracted
from parser.utils import canonical_test_name, normalise_unit, now_utc, today_utc, stdf_ts

# =============================================================================
# ENVIRONMENT & CONNECTION CONFIGURATION
# =============================================================================
PG_CONN = {
    "host": "postgres",
    "port": 5432,
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "database": os.environ.get("POSTGRES_DB", "postgres")
}

CH_CONN = {
    "host": "clickhouse",
    "port": 9000,
    "user": os.environ.get("CLICKHOUSE_USER", "default"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
    "database": os.environ.get("CLICKHOUSE_DB", "default")
}

def get_pg_conn():
    return psycopg2.connect(**PG_CONN)

def get_ch_client():
    return Client(**CH_CONN)

# =============================================================================
# SECTION 2: DEVICE_ID HASHING FORMULA
# =============================================================================
def generate_device_id(
    file_id: str, 
    wafer_id: str, 
    x_coord: Any, 
    y_coord: Any, 
    site_num: Any, 
    part_id: Any, 
    attempt_index: Any, 
    prr_index: int,
    pg_conn
) -> str:
    """
    Constructs the canonical 64 hex char device_id based on strict coordinate precedence.
    Logs WARNING to etl_job_log if falling back to the secondary formula.
    """
    def _str(val, cast_int=False):
        if pd.isna(val) or val is None or val == "":
            return "NULL"
        if cast_int:
            try:
                return str(int(float(val)))
            except:
                return "NULL"
        return str(val)

    # Convert coordinates tightly checking for NULL
    x_str = _str(x_coord, cast_int=True)
    y_str = _str(y_coord, cast_int=True)
    site_str = _str(site_num, cast_int=True)
    
    if x_str != "NULL" and y_str != "NULL":
        # PRIMARY PATH
        raw_string = f"{_str(file_id)}|{_str(wafer_id)}|{x_str}|{y_str}|{site_str}"
        return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()[:64]
    else:
        # FALLBACK PATH
        pid_str = _str(part_id) if _str(part_id) != "NULL" else str(prr_index)
        raw_string = f"{_str(file_id)}|{_str(wafer_id)}|{pid_str}|{site_str}|{_str(attempt_index)}"
        
        # Log to etl_job_log (simulate via cursor if provided, ignoring error for simplicity)
        if pg_conn:
            try:
                missing = [k for k, v in {"x_coord": x_str, "y_coord": y_str}.items() if v == "NULL"]
                warning_msg = f"WARNING: Device ID fallback used. Missing: {missing}"
                with pg_conn.cursor() as cur:
                    cur.execute(
                        "UPDATE etl_job_log SET error_message = COALESCE(error_message || '\n', '') || %s WHERE file_id = %s",
                        (warning_msg, file_id)
                    )
                pg_conn.commit()
            except Exception:
                pg_conn.rollback()
                
        return hashlib.sha256(raw_string.encode('utf-8')).hexdigest()[:64]

# =============================================================================
# DAG DEFINITION
# =============================================================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
}

dag = DAG(
    'stdf_ingest_dag',
    default_args=default_args,
    schedule_interval=None, # NOT scheduled, API/S3 triggered
    catchup=False,
    max_active_runs=5,
)

# -----------------------------------------------------------------------------
# TASK 1: CHECK IDEMPOTENCY
# -----------------------------------------------------------------------------
def check_idempotency(**kwargs):
    file_hash = kwargs['dag_run'].conf.get('file_hash')
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM etl_job_log WHERE file_hash = %s AND status = 'SUCCESS' LIMIT 1",
                (file_hash,)
            )
            if cur.fetchone():
                raise AirflowSkipException(f"File {file_hash} already loaded successfully. Skipping.")

t1_check_idempotency = PythonOperator(
    task_id='t1_check_idempotency',
    python_callable=check_idempotency,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 2: CREATE JOB LOG
# -----------------------------------------------------------------------------
def create_job_log(**kwargs):
    file_hash = kwargs['dag_run'].conf.get('file_hash')
    file_id = kwargs['dag_run'].conf.get('file_id', file_hash[:64])
    source_path = kwargs['dag_run'].conf.get('source_path', 'unknown')
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO etl_job_log (file_id, file_hash, source_path, status, started_at)
                VALUES (%s, %s, %s, 'RUNNING', %s)
            """, (file_id, file_hash, source_path, now_utc()))
            conn.commit()

t2_create_job_log = PythonOperator(
    task_id='t2_create_job_log',
    python_callable=create_job_log,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 3: DOWNLOAD AND DECOMPRESS
# -----------------------------------------------------------------------------
def download_and_decompress(**kwargs):
    # Mocking implementation to fetch file from MinIO and auto-detect extension (.gz, .zip, .tar.gz)
    # Fails if file is unreadable.
    pass

t3_download_and_decompress = PythonOperator(
    task_id='t3_download_and_decompress',
    python_callable=download_and_decompress,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 4: PARSE STDF
# -----------------------------------------------------------------------------
def parse_stdf(**kwargs):
    # Stream parser execution dumping to raw Parquet in chunks of 10,000 max.
    # Mocks updating FAILED/QUARANTINE in Postgres via exception block.
    try:
        pass # Stream parsing logic here
    except Exception as e:
        status = "QUARANTINE" if "missing MIR/FAR/WIR" in str(e) else "FAILED"
        file_hash = kwargs['dag_run'].conf.get('file_hash')
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE etl_job_log SET status = %s WHERE file_hash = %s", (status, file_hash))
            conn.commit()
        raise Exception(f"Parse failure: {str(e)}")

t4_parse_stdf = PythonOperator(
    task_id='t4_parse_stdf',
    python_callable=parse_stdf,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 5: VALIDATE STAGING
# -----------------------------------------------------------------------------
def validate_staging(**kwargs):
    # Simulated validation of pass+fail counts per wafer
    file_hash = kwargs['dag_run'].conf.get('file_hash')
    # Validate logic: sum(pass_count) + sum(fail_count) == WRR total_devices
    # If mismatch > 0.01% -> FAILED.
    pass

t5_validate_staging = PythonOperator(
    task_id='t5_validate_staging',
    python_callable=validate_staging,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 6: CANONICALIZE
# -----------------------------------------------------------------------------
def canonicalize(**kwargs):
    file_id = kwargs['dag_run'].conf.get('file_id')
    file_hash = kwargs['dag_run'].conf.get('file_hash')
    
    # Normally read uniquely extracted test names from staging files here
    unique_tests = pd.DataFrame() 
    
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            for _, row in unique_tests.iterrows():
                test_name_raw = row.get("test_name_raw", "")
                test_num = row.get("test_num", 0)
                orig_unit = row.get("orig_unit", "")
                
                # 1 & 2: canonical_name computation
                canon_name = canonical_test_name(test_name_raw)
                if not canon_name:
                    canon_name = "unknown_test"
                    cur.execute(
                        "UPDATE etl_job_log SET error_message = COALESCE(error_message || '\n', '') || %s WHERE file_hash = %s",
                        (f"WARNING: Empty canonical name for test_name_raw '{test_name_raw}'", file_hash)
                    )
                
                # 3: normalise units
                si_unit, si_factor = normalise_unit(orig_unit)
                
                if si_unit == "unknown":
                    si_factor = 1.0
                    warn_msg = f"WARNING: Unknown unit '{orig_unit}' for {canon_name} (File ID: {file_id}, Test Num: {test_num})"
                    cur.execute(
                        "UPDATE etl_job_log SET error_message = COALESCE(error_message || '\n', '') || %s WHERE file_hash = %s",
                        (warn_msg, file_hash)
                    )
                
                # 4: INSERT ON CONFLICT DO NOTHING with map_version=1
                cur.execute("""
                    INSERT INTO canonical_test_registry 
                        (canonical_name, si_unit, si_factor, test_name_raw_example, map_version)
                    VALUES (%s, %s, %s, %s, 1)
                    ON CONFLICT (canonical_name) DO NOTHING
                """, (canon_name, si_unit, si_factor, test_name_raw))
        conn.commit()

t6_canonicalize = PythonOperator(
    task_id='t6_canonicalize',
    python_callable=canonicalize,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 7: LOAD POSTGRES
# -----------------------------------------------------------------------------
def load_postgres(**kwargs):
    file_hash = kwargs['dag_run'].conf.get('file_hash')
    
    table_order = [
        "canonical_test_registry",
        "lots",
        "hardware_sites",
        "wafers",
        "bin_dict",
        "test_limits",
        "parts",
        "parametric_results",
        "functional_results",
        "tsr_summary"
    ]
    
    conn = get_pg_conn()
    cur = conn.cursor()
    try:
        for table in table_order:
            # Here we would locate the chunked CSVs or Parquets for this table.
            # Using copy_expert to execute COPY FROM STDIN
            mock_csv_path = f"/opt/airflow/output/{file_hash}_{table}_staging.csv"
            if os.path.exists(mock_csv_path):
                with open(mock_csv_path, 'r') as f:
                    copy_sql = f"COPY {table} FROM STDIN WITH CSV HEADER"
                    cur.copy_expert(copy_sql, f)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.execute("UPDATE etl_job_log SET status = 'FAILED' WHERE file_hash = %s", (file_hash,))
        conn.commit()
        # Push to DLQ
        raise Exception(f"Postgres COPY load failed. Rolled back transaction. Error: {e}")
    finally:
        cur.close()
        conn.close()

t7_load_postgres = PythonOperator(
    task_id='t7_load_postgres',
    python_callable=load_postgres,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 8: LOAD CLICKHOUSE
# -----------------------------------------------------------------------------
def load_clickhouse(**kwargs):
    file_hash = kwargs['dag_run'].conf.get('file_hash')
    
    ch_client = get_ch_client()
    
    # Generator approach required for ClickHouse
    def row_generator(file_path):
        # Mocks reading rows without memory bloat
        yield ()
        
    tables_to_load = [
        ("parametric_results_ch", f"/opt/airflow/output/{file_hash}_parametric_results_staging.csv"),
        ("functional_results_ch", f"/opt/airflow/output/{file_hash}_functional_results_staging.csv"),
        ("tsr_summary_ch", f"/opt/airflow/output/{file_hash}_tsr_summary_staging.csv")
    ]
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            for table_name, path in tables_to_load:
                if os.path.exists(path):
                    ch_client.execute(f'INSERT INTO {table_name} VALUES', row_generator(path))
            return # Success
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(30) # Backoff 30 seconds
            else:
                with get_pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE etl_job_log SET ch_load_status = 'FAILED' WHERE file_hash = %s", (file_hash,))
                    conn.commit()

t8_load_clickhouse = PythonOperator(
    task_id='t8_load_clickhouse',
    python_callable=load_clickhouse,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 9: COMPUTE ROLLUPS
# -----------------------------------------------------------------------------
def compute_rollups(**kwargs):
    ch_client = get_ch_client()
    # Inserting into wafer_rollups_ch via ReplacingMergeTree
    ch_client.execute("""
        INSERT INTO wafer_rollups_ch 
        SELECT
            wafer_id,
            lot_id,
            avg(result_scaled), -- Mock logic for computing yield pct
            100 AS pass_count,
            0 AS fail_count,
            0.0 AS retest_rate,
            now() AS compute_ts,
            today() AS ingestion_date
        FROM parametric_results_ch
        GROUP BY wafer_id, lot_id
    """)

t9_compute_rollups = PythonOperator(
    task_id='t9_compute_rollups',
    python_callable=compute_rollups,
    dag=dag,
)

# -----------------------------------------------------------------------------
# TASK 10: UPDATE JOB LOG
# -----------------------------------------------------------------------------
def update_job_log(**kwargs):
    file_hash = kwargs['dag_run'].conf.get('file_hash')
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE etl_job_log 
                SET status = 'SUCCESS', finished_at = %s 
                WHERE file_hash = %s
            """, (now_utc(), file_hash))
        conn.commit()

t10_update_job_log = PythonOperator(
    task_id='t10_update_job_log',
    python_callable=update_job_log,
    dag=dag,
)

# =============================================================================
# HARD DEPENDENCY CHAINING (Cannot be skipped/merged/reordered)
# =============================================================================
t1_check_idempotency >> t2_create_job_log >> t3_download_and_decompress >> \
t4_parse_stdf >> t5_validate_staging >> t6_canonicalize >> \
t7_load_postgres >> t8_load_clickhouse >> t9_compute_rollups >> t10_update_job_log

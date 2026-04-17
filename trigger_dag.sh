#!/usr/bin/env bash
set -e
CONF='{"stdf_path": "/opt/airflow/stdf/demofile.stdf"}'
airflow dags trigger stdf_ingest_dag --conf "$CONF"
echo "DAG triggered successfully."

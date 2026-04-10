# STDF Analytics — Objective A (Complete)

## 9-Table Normalised Schema

| # | Table | Source Record | Rows (demofile) |
|---|-------|--------------|-----------------|
| 1 | `lots` | MIR + MRR | 1 |
| 2 | `wafers` | WIR + WRR | 1 per wafer |
| 3 | `parts` | PRR | 1,619 |
| 4 | `hardware_sites` | SDR | 1 |
| 5 | `bin_dict` | HBR + SBR | 22 |
| 6 | `test_limits` | PTR metadata | 74 unique tests |
| 7 | `parametric_results` | PTR values | 54,123 |
| 8 | `functional_results` | FTR | 0 (not in file) |
| 9 | `dead_letter_queue` | parse errors | 0 |

## Usage

```bash
pip install pandas pyarrow

# Export all 9 tables
python export_parquet.py  file.stdf  --output ./output  --summary

# Compressed files supported
python export_parquet.py  file.stdf.gz  --output ./output
```

## Key Design Decisions

- **Table 6 (test_limits) split from Table 7 (parametric_results)**: avoids storing test name + limits 54,123 times — only 74 rows
- **attempt_index** in parts/parametric_results tracks retests (0=first, 1+=retest)
- **result_si** normalises all measurements to SI base units (mV→V, uA→A etc.)
- **canonical_name** strips pin suffixes and normalises for cross-lot comparison
- **yield computed from PRR** — WRR good_count is unreliable (often 0xFFFFFFFF)
- **Dead Letter Queue** captures all parse errors with offset, record type, and exception

---

## Step 2 — Load into MySQL (ETL Pipeline)

### 1. Create the database schema

```bash
mysql -u root -p -e "CREATE DATABASE stdf_analytics CHARACTER SET utf8mb4;"
mysql -u root -p stdf_analytics < db/schema_mysql.sql
```

### 2. Configure DB credentials

Copy and edit the example config:

```bash
cp db/db_config.example.json db/db_config.json
# Edit host / user / password in db_config.json
```

Or use environment variables: `STDF_DB_HOST`, `STDF_DB_USER`, `STDF_DB_PASSWORD`, `STDF_DB_NAME`

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the ETL

```bash
# Single file
python etl_pipeline.py demofile.stdf --config db/db_config.json

# Whole folder
python etl_pipeline.py ./stdf_folder/ --recursive --config db/db_config.json

# Dry-run (no DB writes)
python etl_pipeline.py demofile.stdf --dry-run

# Force re-load existing file
python etl_pipeline.py demofile.stdf --force --config db/db_config.json
```

### ETL Design Notes

| Feature | Details |
|---|---|
| **Idempotency** | SHA256 file hash tracked — same file never loaded twice |
| **Job log** | Every run recorded in `etl_job_log` with status + row counts |
| **FK order** | lots → wafers → parts → results |
| **Batched inserts** | parametric_results inserted in 2,000-row chunks |
| **Rollback** | Full rollback + error log on failure |
| **Partition** | parametric_results partitioned by ingestion_date |

"""
export_parquet.py
=================
Reads one STDF file, parses it into all 9 normalised tables,
and writes each table to its own Parquet file + staging JSON.

OUTPUTS  (inside --output folder)
  {file_id}_lots.parquet                 Table 1
  {file_id}_wafers.parquet               Table 2
  {file_id}_parts.parquet                Table 3
  {file_id}_hardware_sites.parquet       Table 4
  {file_id}_bin_dict.parquet             Table 5
  {file_id}_test_limits.parquet          Table 6  ← split from fat PTR table
  {file_id}_canonical_registry.parquet   Table 5a
  {file_id}_parametric_results.parquet   Table 7  ← split from fat PTR table
  {file_id}_functional_results.parquet   Table 8
  {file_id}_dead_letter_queue.parquet    Table 9
  {file_id}_tsr_summary.parquet          Bonus
  {file_id}_manifest.json               Ingestion manifest

USAGE
  python export_parquet.py  file.stdf
  python export_parquet.py  file.stdf  --output ./output  --summary
  python export_parquet.py  file.stdf.gz --output ./output
"""

import sys
import json
import argparse
from pathlib import Path

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_OK = True
except ImportError:
    PARQUET_OK = False
    print("WARNING: pyarrow not installed. Will write CSV fallback.")
    print("         Run:  pip install pandas pyarrow")

# Make sure project root is on path
sys.path.insert(0, str(Path(__file__).parent))

from parser.stdf_parser import parse
from parser.utils import now_utc


# ══════════════════════════════════════════════════════════════════════════════
# SCHEMA DEFINITIONS
# Explicit column order and types for every table.
# ══════════════════════════════════════════════════════════════════════════════

TABLE_SCHEMAS = {

    # Table 1: Lot
    "lots": [
        ("file_id",        "string"),  ("file_hash",     "string"),
        ("file_name",      "string"),  ("parser_version","string"),
        ("ingestion_ts",   "string"),
        ("lot_id",         "string"),  ("sublot_id",     "string"),
        ("part_type",      "string"),  ("family_id",     "string"),
        ("pkg_type",       "string"),  ("process_id",    "string"),
        ("design_rev",     "string"),  ("date_code",     "string"),
        ("job_name",       "string"),  ("job_rev",       "string"),
        ("test_code",      "string"),  ("test_temp",     "string"),
        ("flow_id",        "string"),
        ("operator",       "string"),  ("supervisor",    "string"),
        ("node_name",      "string"),  ("tester_type",   "string"),
        ("exec_type",      "string"),  ("exec_ver",      "string"),
        ("facility_id",    "string"),  ("floor_id",      "string"),
        ("serial_num",     "string"),  ("eng_id",        "string"),
        ("setup_time",     "string"),  ("start_time",    "string"),
        ("finish_time",    "string"),  ("disp_code",     "string"),
        ("station_num",    "int64"),   ("burn_time_min", "int64"),
        ("mode_code",      "string"),  ("user_text",     "string"),
        ("user_desc",      "string"),
    ],

    # Table 2: Wafer
    "wafers": [
        ("file_id",      "string"),   ("lot_id",       "string"),
        ("wafer_id",     "string"),   ("head_num",     "int64"),
        ("site_grp",     "int64"),
        ("start_time",   "string"),   ("finish_time",  "string"),
        ("total_devices","int64"),    ("pass_count",   "int64"),
        ("fail_count",   "int64"),    ("retest_count", "int64"),
        ("abort_count",  "int64"),
        ("yield_pct",    "float64"),  ("dppm",         "float64"),
        ("ingestion_ts", "string"),   ("ingestion_date","string"),
    ],

    # Table 3: Part/Chip
    "parts": [
        ("device_id",    "string"),   ("file_id",      "string"),
        ("wafer_id",     "string"),   ("lot_id",       "string"),
        ("part_type",    "string"),
        ("x_coord",      "int64"),    ("y_coord",      "int64"),
        ("site_num",     "int64"),    ("head_num",     "int64"),
        ("pass_fail",    "string"),   ("hard_bin",     "int64"),
        ("soft_bin",     "int64"),    ("num_tests_run","int64"),
        ("test_time_ms", "int64"),    ("part_id",      "string"),
        ("attempt_index","int64"),
        ("ingestion_ts", "string"),   ("ingestion_date","string"),
    ],

    # Table 4: Hardware Site
    "hardware_sites": [
        ("file_id",         "string"), ("lot_id",        "string"),
        ("head_num",        "int64"),  ("site_grp",      "int64"),
        ("site_count",      "int64"),  ("site_nums",     "string"),
        ("handler_type",    "string"), ("handler_id",    "string"),
        ("card_type",       "string"), ("card_id",       "string"),
        ("load_type",       "string"), ("load_id",       "string"),
        ("dib_type",        "string"), ("dib_id",        "string"),
        ("cable_type",      "string"), ("cable_id",      "string"),
        ("contactor_type",  "string"), ("contactor_id",  "string"),
        ("laser_type",      "string"), ("laser_id",      "string"),
        ("extra_type",      "string"), ("extra_id",      "string"),
    ],

    # Table 5: Bin Dictionary
    "bin_dict": [
        ("file_id",   "string"),  ("lot_id",   "string"),
        ("bin_type",  "string"),  ("bin_num",  "int64"),
        ("bin_name",  "string"),  ("bin_count","int64"),
        ("pass_fail", "string"),  ("head_num", "int64"),
        ("site_num",  "int64"),
    ],

    # Table 5a: Canonical Test Registry
    "canonical_test_registry": [
        ("test_name_raw",   "string"), ("canonical_name", "string"),
        ("si_unit",         "string"), ("si_factor",      "float64"),
    ],

    # Table 6: Test Limits  ← split from fat PTR table
    "test_limits": [
        ("file_id",         "string"), ("lot_id",         "string"),
        ("test_num",        "int64"),  ("test_name_raw",  "string"),
        ("lo_limit_scaled", "float64"),("hi_limit_scaled","float64"),
        ("orig_unit",       "string"), 
        ("lo_spec",         "float64"),("hi_spec",        "float64"),
        ("parser_version",  "string"), ("ingestion_ts",   "string"),
    ],

    # Table 7: Parametric Results  ← split from fat PTR table
    "parametric_results": [
        ("result_id",       "string"),  ("device_id",      "string"),
        ("wafer_id",        "string"),  ("file_id",        "string"),
        ("lot_id",          "string"),  ("part_type",      "string"),
        ("test_num",        "int64"),   ("test_name_raw",  "string"),
        ("canonical_name",  "string"),
        ("result_raw",      "float64"), ("result_scaled",  "float64"),
        ("result_si",       "float64"),
        ("si_unit",         "string"),  ("orig_unit",      "string"),
        ("lo_limit_scaled", "float64"), ("hi_limit_scaled","float64"),
        ("passed",          "bool"),    ("failed_low",     "bool"),
        ("failed_high",     "bool"),    ("alarm",          "bool"),
        ("not_executed",    "bool"),    ("attempt_index",  "int64"),
        ("head_num",        "int64"),   ("site_num",       "int64"),
        ("parser_version",  "string"),
        ("ingestion_ts",    "string"),  ("ingestion_date", "string"),
    ],

    # Table 8: Functional Results
    "functional_results": [
        ("device_id",     "string"), ("file_id",      "string"),
        ("wafer_id",      "string"), ("lot_id",       "string"),
        ("test_num",      "int64"),  ("test_name",    "string"),
        ("vector_name",   "string"), ("head_num",     "int64"),
        ("site_num",      "int64"),  ("passed",       "bool"),
        ("alarm",         "bool"),   ("alarm_id",     "string"),
        ("cycle_count",   "int64"),  ("fail_count",   "int64"),
        ("repeat_count",  "int64"),  ("rel_vect_addr","int64"),
        ("xfail_addr",    "int64"),  ("yfail_addr",   "int64"),
        ("vect_offset",   "int64"),  ("time_set",     "string"),
        ("op_code",       "string"), ("attempt_index","int64"),
        ("parser_version","string"),
        ("ingestion_ts",  "string"), ("ingestion_date","string"),
    ],

    # Table 9: Dead Letter Queue
    "dead_letter_queue": [
        ("file_id",   "string"), ("offset",    "int64"),
        ("rec_type",  "string"), ("reason",    "string"),
        ("raw_hex",   "string"), ("exception", "string"),
        ("logged_at", "string"),
    ],

    # Bonus: TSR Summary
    "tsr_summary": [
        ("file_id",      "string"), ("lot_id",       "string"),
        ("test_num",     "int64"),  ("test_name",    "string"),
        ("test_type",    "string"), ("head_num",     "int64"),
        ("site_num",     "int64"),  ("exec_count",   "int64"),
        ("fail_count",   "int64"),  ("alarm_count",  "int64"),
        ("avg_exec_time","float64"),("min_result",   "float64"),
        ("max_result",   "float64"),("sum_results",  "float64"),
    ],
}


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _to_df(rows: list[dict], schema: list[tuple]) -> "pd.DataFrame":
    """
    Convert list-of-dicts to a typed DataFrame using the explicit schema.
    - Missing columns are added as NA.
    - Extra columns (not in schema) are dropped.
    - Each column is cast to its declared dtype.
    """
    import pandas as pd
    import numpy as np

    if not rows:
        cols = [c for c, _ in schema]
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows)

    for col, dtype in schema:
        if col not in df.columns:
            df[col] = pd.NA

        try:
            if dtype == "string":
                df[col] = df[col].astype(str).replace(
                    {"None":"","nan":"","NaN":"","<NA>":""}
                ).replace("", pd.NA).astype("string")

            elif dtype in ("int64", "int32"):
                def _to_int(v):
                    if v is None or (isinstance(v, float) and v!=v): return pd.NA
                    try: return int(float(str(v)))
                    except: return pd.NA
                df[col] = df[col].apply(_to_int).astype("Int64")

            elif dtype == "float64":
                def _to_float(v):
                    if v is None or (isinstance(v, float) and v!=v): return float("nan")
                    try:
                        f = float(str(v))
                        return float("nan") if f!=f else f
                    except: return float("nan")
                df[col] = df[col].apply(_to_float).astype("float64")

            elif dtype == "bool":
                def _to_bool(v):
                    if v is None or (isinstance(v, float) and v!=v): return pd.NA
                    if isinstance(v, bool): return v
                    s = str(v).lower().strip()
                    if s in ("true","1","yes"): return True
                    if s in ("false","0","no"): return False
                    return pd.NA
                df[col] = df[col].apply(_to_bool).astype("boolean")

        except Exception as e:
            pass  # keep column as-is on cast failure

    # Keep only schema columns in schema order
    df = df[[c for c, _ in schema if c in df.columns]]
    return df


def _write_parquet(df, out_path: Path, compression: str = "snappy"):
    """Write DataFrame to Parquet. Returns file size bytes."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Convert timezone-aware datetimes to tz-naive UTC
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)

    # Convert pandas StringDtype to object for pyarrow
    for col in df.columns:
        if str(df[col].dtype) in ("string", "StringDtype"):
            df[col] = df[col].astype(object)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(
        table, str(out_path),
        compression=compression,
        use_dictionary=True,
        row_group_size=100_000,
    )
    return out_path.stat().st_size


def _write_csv_fallback(df, out_path: Path):
    """CSV fallback when pyarrow not available."""
    csv_path = out_path.with_suffix(".csv")
    df.to_csv(csv_path, index=False)
    return csv_path.stat().st_size, csv_path


# ══════════════════════════════════════════════════════════════════════════════
# MAIN EXPORT FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def export(
    stdf_path:   str,
    output_dir:  str = "./output",
    compression: str = "snappy",
    summary:     bool = False,
    verbose:     bool = True,
) -> dict:
    """
    Full pipeline: STDF file → all 9 Parquet tables + manifest.

    Parameters
    ----------
    stdf_path   : Path to .stdf / .stdf.gz / .zip
    output_dir  : Output folder (created if needed)
    compression : Parquet compression ('snappy', 'gzip', 'none')
    summary     : Print per-table summary after writing
    verbose     : Print parse progress

    Returns manifest dict.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"\n{'═'*58}")
        print(f"  STDF → 9-Table Parquet Export")
        print(f"{'═'*58}")

    # ── 1. Parse ──────────────────────────────────────────────────────────────
    result   = parse(stdf_path, verbose=verbose)
    file_id  = result["file_id"]
    stats    = result["stats"]

    # Map: table_name → rows list
    tables = {
        "lots":               result["lots"],
        "wafers":             result["wafers"],
        "parts":              result["parts"],
        "hardware_sites":     result["hardware_sites"],
        "bin_dict":           result["bin_dict"],
        "canonical_test_registry": result.get("canonical_test_registry", []),
        "test_limits":        result["test_limits"],
        "parametric_results": result["parametric_results"],
        "functional_results": result["functional_results"],
        "dead_letter_queue":  result["dead_letter_queue"],
        "tsr_summary":        result["tsr_summary"],
    }

    # ── 2. Write tables ───────────────────────────────────────────────────────
    if verbose:
        print(f"\n  Writing Parquet files → {out.resolve()}\n")

    manifest = {
        "file_id":       file_id,
        "file_hash":     result["file_hash"],
        "source_file":   str(Path(stdf_path).resolve()),
        "exported_at":   now_utc(),
        "compression":   compression,
        "parse_stats":   stats,
        "tables":        {},
    }

    import pandas as pd

    for table_name, rows in tables.items():
        schema = TABLE_SCHEMAS.get(table_name)
        if schema is None:
            continue

        # Convert to typed DataFrame
        df = _to_df(rows, schema)

        # Output path
        fname    = f"{file_id}_{table_name}"
        pq_path  = out / f"{fname}.parquet"
        csv_path = out / f"{fname}.csv"

        row_count  = len(df)
        file_size  = 0
        out_format = "parquet"

        if row_count == 0:
            # Still write empty parquet for schema consistency
            pass

        if PARQUET_OK:
            try:
                file_size  = _write_parquet(df.copy(), pq_path, compression)
                out_format = "parquet"
            except Exception as e:
                print(f"    Parquet write error for {table_name}: {e}")
                file_size, csv_path = _write_csv_fallback(df, pq_path)
                out_format = "csv"
        else:
            file_size, csv_path = _write_csv_fallback(df, pq_path)
            out_format = "csv"


        manifest["tables"][table_name] = {
            "file":       f"{fname}.{out_format}",
            "format":     out_format,
            "rows":       row_count,
            "columns":    [c for c, _ in schema],
            "size_bytes": file_size,
        }

        status = "✓" if row_count > 0 else "–"
        size_s = f"{file_size/1024:.1f} KB" if file_size else "0 KB"
        if verbose:
            print(f"  {status}  {table_name:<26}  "
                  f"{row_count:>7,} rows  {size_s:>9}")

    # ── 3. Write manifest ─────────────────────────────────────────────────────
    manifest_path = out / f"{file_id}_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)

    if verbose:
        total_rows = sum(
            manifest["tables"][t]["rows"] for t in manifest["tables"]
        )
        total_size = sum(
            manifest["tables"][t]["size_bytes"] for t in manifest["tables"]
        )
        print(f"\n  {'─'*52}")
        print(f"  Total rows    : {total_rows:,}")
        print(f"  Total size    : {total_size/1024:.1f} KB")
        print(f"  Manifest      : {manifest_path.name}")
        print(f"  Output folder : {out.resolve()}")
        print(f"{'═'*58}\n")

    # ── 4. Summary report ─────────────────────────────────────────────────────
    if summary:
        _print_summary(result, manifest)

    return manifest


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY REPORT
# ══════════════════════════════════════════════════════════════════════════════

def _print_summary(result: dict, manifest: dict):
    """Print human-readable analytics summary after export."""
    print("\n" + "="*58)
    print("  PARSE SUMMARY")
    print("="*58)

    lot = result["lots"][0] if result["lots"] else {}
    print(f"  Lot ID      : {lot.get('lot_id','')}")
    print(f"  Part type   : {lot.get('part_type','')}")
    print(f"  Tester      : {lot.get('node_name','')}")
    print(f"  Operator    : {lot.get('operator','')}")
    print(f"  Start time  : {lot.get('start_time','')}")
    print(f"  Finish time : {lot.get('finish_time','')}")
    print()

    # Wafer yield summary
    print("  WAFER YIELD")
    print(f"  {'Wafer ID':<20} {'Total':>7} {'Pass':>7} {'Fail':>7} "
          f"{'Yield%':>8} {'DPPM':>10}")
    print("  " + "─"*60)
    for w in result["wafers"]:
        y = f"{w['yield_pct']:.2f}" if w['yield_pct'] is not None else "N/A"
        d = f"{w['dppm']:,.0f}"     if w['dppm'] is not None else "N/A"
        print(f"  {w['wafer_id']:<20} {w['total_devices']:>7,} "
              f"{w['pass_count']:>7,} {w['fail_count']:>7,} "
              f"{y:>8} {d:>10}")
    print()

    # Top 10 failing tests
    fails = {}
    for r in result["parametric_results"]:
        if not r.get("passed", True):
            name = r.get("canonical_name") or r.get("test_name_raw") or str(r.get("test_num"))
            fails[name] = fails.get(name, 0) + 1

    if fails:
        print("  TOP FAILING TESTS")
        print(f"  {'Test name':<40} {'Fail count':>10}")
        print("  " + "─"*52)
        for name, cnt in sorted(fails.items(), key=lambda x: -x[1])[:10]:
            print(f"  {name:<40} {cnt:>10,}")
        print()

    # Bin summary
    bins = result.get("bin_dict", [])
    if bins:
        print("  BIN SUMMARY")
        print(f"  {'Type':<6} {'Bin':>5} {'Name':<20} {'Count':>8} {'P/F':>6}")
        print("  " + "─"*48)
        for b in sorted(bins, key=lambda x: (x.get("bin_type",""), x.get("bin_num",0))):
            pf  = b.get("pass_fail") or "?"
            cnt = b.get("bin_count") or 0
            print(f"  {b.get('bin_type',''):6} {b.get('bin_num',0):>5}  "
                  f"{(b.get('bin_name') or ''):20} {cnt:>8,}  {pf:>6}")
        print()

    # DLQ
    dlq = result.get("dead_letter_queue", [])
    if dlq:
        print(f"  DEAD LETTER QUEUE  ({len(dlq)} entries)")
        for e in dlq[:5]:
            print(f"    offset={e['offset']}  type={e['rec_type']}  "
                  f"reason={e['reason']}  err={e['exception'][:60]}")
        print()

    print("="*58 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Export STDF file to 9 normalised Parquet tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Tables written:
  1  lots                  MIR + MRR
  2  wafers                WIR + WRR  (yield from PRR)
  3  parts                 PRR  (device + attempt_index)
  4  hardware_sites        SDR
  5  bin_dict              HBR + SBR
  6  test_limits           PTR metadata  (unique per test_num)
  7  parametric_results    PTR values    (one per device × test)
  8  functional_results    FTR
  9  dead_letter_queue     parse errors
     tsr_summary           bonus

Examples:
  python export_parquet.py  file.stdf
  python export_parquet.py  file.stdf.gz  --output ./output  --summary
  python export_parquet.py  file.stdf     --compression gzip
        """,
    )
    ap.add_argument("stdf_file",    help="Path to .stdf / .stdf.gz / .zip")
    ap.add_argument("--output",     default="./output", help="Output directory")
    ap.add_argument("--compression",default="snappy",
                    choices=["snappy","gzip","brotli","none"])
    ap.add_argument("--summary",    action="store_true",
                    help="Print analytics summary after export")
    ap.add_argument("--quiet",      action="store_true")
    args = ap.parse_args()

    export(
        stdf_path   = args.stdf_file,
        output_dir  = args.output,
        compression = args.compression,
        summary     = args.summary,
        verbose     = not args.quiet,
    )

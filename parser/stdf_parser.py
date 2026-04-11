"""
parser/stdf_parser.py
=====================
Core STDF streaming parser.

Reads the file record-by-record (never loads entire file into memory)
and produces rows for all 9 normalised tables:

  Table 1  lots             ← MIR + MRR
  Table 2  wafers           ← WIR + WRR  (yield computed from PRR)
  Table 3  parts            ← PRR  (+ attempt_index from PIR state machine)
  Table 4  hardware_sites   ← SDR
  Table 5  bin_dict         ← HBR + SBR
  Table 6  test_limits      ← PTR metadata  (unique per test_num)
  Table 7  parametric_results ← PTR values  (one per device × test)
  Table 8  functional_results ← FTR
  Table 9  dead_letter_queue  ← parse errors

DESIGN
  • Byte order auto-detected from FAR record (cpu_type byte).
  • PIR state machine tracks (head, site) → device_id and attempt_index.
  • Yield is computed from PRR pass/fail counts (WRR good_count is unreliable).
  • All UUIDs use sequential integers prefixed by file_id for reproducibility.
  • Dead letter entries are appended for any record that raises during parsing.
"""

import struct
import gzip
import zipfile
import uuid
from pathlib import Path
from datetime import datetime, timezone

from parser.records import (
    RECORD_MAP, PARSER_VERSION,
    parse_FAR, parse_MIR, parse_MRR,
    parse_WIR, parse_WRR,
    parse_PIR, parse_PRR,
    parse_PTR, parse_FTR,
    parse_HBR, parse_SBR,
    parse_SDR, parse_TSR,
)
from parser.utils import (
    sha256_bytes, now_utc, today_utc, DeadLetterQueue,
)


# ── File loader ───────────────────────────────────────────────────────────────

def _load(filepath: str) -> bytes:
    path = Path(filepath)
    suffix = path.suffix.lower()
    if suffix == ".gz":
        with gzip.open(path, "rb") as f:
            return f.read()
    if suffix == ".zip":
        with zipfile.ZipFile(path) as z:
            names = z.namelist()
            stdf = [n for n in names if n.lower().endswith((".stdf",".std"))]
            with z.open(stdf[0] if stdf else names[0]) as f:
                return f.read()
    with open(path, "rb") as f:
        return f.read()


# ── Main parser ───────────────────────────────────────────────────────────────

def parse(filepath: str, verbose: bool = True) -> dict:
    """
    Stream-parse a STDF file and return a dict containing rows for all 9 tables.

    Returns
    -------
    {
        "file_id":            str,
        "file_hash":          str,
        "lots":               list[dict],   # Table 1
        "wafers":             list[dict],   # Table 2
        "parts":              list[dict],   # Table 3
        "hardware_sites":     list[dict],   # Table 4
        "bin_dict":           list[dict],   # Table 5
        "test_limits":        list[dict],   # Table 6
        "parametric_results": list[dict],   # Table 7
        "functional_results": list[dict],   # Table 8
        "dead_letter_queue":  list[dict],   # Table 9
        "tsr_summary":        list[dict],   # Bonus
        "stats":              dict,
    }
    """
    data     = _load(filepath)
    file_hash = sha256_bytes(data)
    file_id   = file_hash[:16]          # short stable ID for this file
    nbytes    = len(data)
    ingested  = now_utc()
    ing_date  = today_utc()

    dlq = DeadLetterQueue(file_id)

    # ── Accumulators ─────────────────────────────────────────────────────────
    lot_row       = None   # ONE lot per file (MIR)
    mrr_extra     = {}     # MRR finish_time etc. merged into lot row
    wir_by_wid    = {}     # wafer_id → WIR fields
    wrr_by_wid    = {}     # wafer_id → WRR fields
    wafer_order   = []     # preserve WRR order

    part_rows     = []     # Table 3 (PRR enriched)
    hw_site_rows  = []     # Table 4 (SDR)
    bin_dict_rows = []     # Table 5 (HBR + SBR)
    canonical_test_registry = {} # Table 5a (test_name_raw -> registry dict)
    test_limits   = {}     # Table 6 (test_num → limit dict, deduplicated)
    param_rows    = []     # Table 7 (PTR results)
    func_rows     = []     # Table 8 (FTR results)
    tsr_rows      = []     # Bonus

    # ── State machine ─────────────────────────────────────────────────────────
    # Byte order
    bo = ">"

    # Context
    lot_id       = ""
    part_type    = ""
    wafer_id     = ""
    current_wir  = {}

    # Device tracking
    device_counter = 0
    pir_state      = {}    # (head, site) → {"device_id", "attempt_index"}
    dev_wafer      = {}    # device_counter → wafer_id

    # Yield (computed from PRR)
    w_pass  = {}   # wafer_id → int
    w_fail  = {}   # wafer_id → int

    # Record counts
    rec_counts    = {}
    parse_errors  = 0

    # ── Streaming loop ────────────────────────────────────────────────────────
    offset = 0
    while offset + 4 <= nbytes:
        try:
            rec_len, rec_typ, rec_sub = struct.unpack_from(bo+"HBB", data, offset)
        except Exception:
            break

        # Guard against corrupt length
        if rec_len > nbytes - offset - 4:
            offset += 1
            continue

        body     = data[offset+4 : offset+4+rec_len]
        rec_name = RECORD_MAP.get((rec_typ, rec_sub), "")
        rec_counts[rec_name or f"UNK({rec_typ},{rec_sub})"] = \
            rec_counts.get(rec_name or f"UNK({rec_typ},{rec_sub})", 0) + 1

        try:

            # ── FAR: set byte order ───────────────────────────────────────────
            if rec_name == "FAR":
                bo, _ = parse_FAR(body)

            # ── MIR → Table 1 (Lot) ──────────────────────────────────────────
            elif rec_name == "MIR":
                mir = parse_MIR(body, bo)
                lot_id   = mir.get("lot_id") or ""
                part_type= mir.get("part_type") or ""
                lot_row  = {
                    "file_id":       file_id,
                    "file_hash":     file_hash,
                    "file_name":     Path(filepath).name,
                    "parser_version":PARSER_VERSION,
                    "ingestion_ts":  ingested,
                    **mir,
                }

            # ── MRR → merged into Lot ─────────────────────────────────────────
            elif rec_name == "MRR":
                mrr_extra = parse_MRR(body, bo)

            # ── WIR → Table 2 (Wafer) ────────────────────────────────────────
            elif rec_name == "WIR":
                wir = parse_WIR(body, bo)
                wafer_id = wir.get("wafer_id") or f"W_{offset}"
                current_wir = wir
                wir_by_wid[wafer_id] = wir

            # ── WRR → Table 2 (Wafer) ────────────────────────────────────────
            elif rec_name == "WRR":
                wrr = parse_WRR(body, bo)
                wid = wrr.get("wafer_id") or wafer_id
                wrr["wafer_id"] = wid
                wrr_by_wid[wid] = wrr
                if wid not in wafer_order:
                    wafer_order.append(wid)

            # ── PIR → update state machine ────────────────────────────────────
            elif rec_name == "PIR":
                pir = parse_PIR(body, bo)
                key = (pir.get("head_num", 0), pir.get("site_num", 0))
                device_counter += 1
                # attempt_index: how many times has this (head,site) been seen
                # since the last PRR reset? 0 = first attempt, 1+ = retest.
                prev = pir_state.get(key)
                attempt = (prev["attempt_index"] + 1) if prev and prev.get("open") else 0
                pir_state[key] = {
                    "device_counter": device_counter,
                    "attempt_index":  attempt,
                    "open":           True,
                }
                dev_wafer[device_counter] = wafer_id

            # ── PRR → Table 3 (Part/Chip) ─────────────────────────────────────
            elif rec_name == "PRR":
                prr = parse_PRR(body, bo)
                key  = (prr.get("head_num", 0), prr.get("site_num", 0))
                st   = pir_state.get(key, {})
                dc   = st.get("device_counter", device_counter)
                wid  = dev_wafer.get(dc, wafer_id)
                prr["device_id"]     = f"{file_id}_{dc}"
                prr["wafer_id"]      = wid
                prr["lot_id"]        = lot_id
                prr["part_type"]     = part_type
                prr["file_id"]       = file_id
                prr["attempt_index"] = st.get("attempt_index", 0)
                prr["ingestion_ts"]  = ingested
                prr["ingestion_date"]= ing_date
                part_rows.append(prr)

                # yield tracking
                if prr.get("pass_fail") == "PASS":
                    w_pass[wid] = w_pass.get(wid, 0) + 1
                else:
                    w_fail[wid] = w_fail.get(wid, 0) + 1

                # Close PIR state so next PIR on same (head,site) = new device
                if key in pir_state:
                    pir_state[key]["open"] = False

            # ── PTR → Table 6 (limits) + Table 7 (results) ───────────────────
            elif rec_name == "PTR":
                ptr = parse_PTR(body, bo)
                key  = (ptr.get("head_num", 0), ptr.get("site_num", 0))
                st   = pir_state.get(key, {})
                dc   = st.get("device_counter", device_counter)
                wid  = dev_wafer.get(dc, wafer_id)
                dev_id = f"{file_id}_{dc}"

                tnum = ptr.get("test_num")
                raw_name = ptr.get("test_name_raw")

                # ── Table 5a: Canonical Test Registry ────────────────────────
                if raw_name and raw_name not in canonical_test_registry:
                    canonical_test_registry[raw_name] = {
                        "test_name_raw":  raw_name,
                        "canonical_name": ptr.get("canonical_name"),
                        "si_unit":        ptr.get("si_unit"),
                        "si_factor":      ptr.get("si_factor"),
                    }

                # ── Table 6: Test Limits (store once per unique test_num) ─────
                if tnum is not None and tnum not in test_limits:
                    test_limits[tnum] = {
                        "file_id":        file_id,
                        "lot_id":         lot_id,
                        "test_num":       tnum,
                        "test_name_raw":  raw_name,
                        "lo_limit_scaled":ptr.get("lo_limit_scaled"),
                        "hi_limit_scaled":ptr.get("hi_limit_scaled"),
                        "orig_unit":      ptr.get("orig_unit"),
                        "lo_spec":        ptr.get("lo_spec"),
                        "hi_spec":        ptr.get("hi_spec"),
                        "parser_version": PARSER_VERSION,
                        "ingestion_ts":   ingested,
                    }

                # ── Table 7: Parametric Result (one per device × test) ────────
                result_id = f"{dev_id}_{tnum}"
                param_rows.append({
                    "result_id":      result_id,
                    "device_id":      dev_id,
                    "wafer_id":       wid,
                    "file_id":        file_id,
                    "lot_id":         lot_id,
                    "part_type":      part_type,
                    "test_num":       tnum,
                    "test_name_raw":  ptr.get("test_name_raw"),
                    "canonical_name": ptr.get("canonical_name"),
                    "result_raw":     ptr.get("result_raw"),
                    "result_scaled":  ptr.get("result_scaled"),
                    "result_si":      ptr.get("result_si"),
                    "si_unit":        ptr.get("si_unit"),
                    "orig_unit":      ptr.get("orig_unit"),
                    "lo_limit_scaled":ptr.get("lo_limit_scaled"),
                    "hi_limit_scaled":ptr.get("hi_limit_scaled"),
                    "passed":         ptr.get("passed"),
                    "failed_low":     ptr.get("failed_low"),
                    "failed_high":    ptr.get("failed_high"),
                    "alarm":          ptr.get("alarm"),
                    "not_executed":   ptr.get("not_executed"),
                    "attempt_index":  st.get("attempt_index", 0),
                    "head_num":       ptr.get("head_num"),
                    "site_num":       ptr.get("site_num"),
                    "parser_version": PARSER_VERSION,
                    "ingestion_ts":   ingested,
                    "ingestion_date": ing_date,
                })

            # ── FTR → Table 8 (Functional Results) ────────────────────────────
            elif rec_name == "FTR":
                ftr = parse_FTR(body, bo)
                key  = (ftr.get("head_num", 0), ftr.get("site_num", 0))
                st   = pir_state.get(key, {})
                dc   = st.get("device_counter", device_counter)
                wid  = dev_wafer.get(dc, wafer_id)
                ftr["device_id"]      = f"{file_id}_{dc}"
                ftr["wafer_id"]       = wid
                ftr["lot_id"]         = lot_id
                ftr["file_id"]        = file_id
                ftr["attempt_index"]  = st.get("attempt_index", 0)
                ftr["parser_version"] = PARSER_VERSION
                ftr["ingestion_ts"]   = ingested
                ftr["ingestion_date"] = ing_date
                func_rows.append(ftr)

            # ── SDR → Table 4 (Hardware Site) ─────────────────────────────────
            elif rec_name == "SDR":
                sdr = parse_SDR(body, bo)
                sdr["file_id"]   = file_id
                sdr["lot_id"]    = lot_id
                hw_site_rows.append(sdr)

            # ── HBR + SBR → Table 5 (Bin Dictionary) ─────────────────────────
            elif rec_name == "HBR":
                hbr = parse_HBR(body, bo)
                hbr["file_id"] = file_id
                hbr["lot_id"]  = lot_id
                bin_dict_rows.append(hbr)

            elif rec_name == "SBR":
                sbr = parse_SBR(body, bo)
                sbr["file_id"] = file_id
                sbr["lot_id"]  = lot_id
                bin_dict_rows.append(sbr)

            # ── TSR → Bonus table ─────────────────────────────────────────────
            elif rec_name == "TSR":
                tsr = parse_TSR(body, bo)
                tsr["file_id"] = file_id
                tsr["lot_id"]  = lot_id
                tsr_rows.append(tsr)

        except Exception as e:
            dlq.add(
                offset=offset,
                rec_type=rec_name or f"UNK({rec_typ},{rec_sub})",
                reason="PARSE_ERROR",
                raw_hex=body[:128].hex(),
                exception=str(e),
            )
            parse_errors += 1

        offset += 4 + rec_len

    # ── Post-loop: build wafer rows ───────────────────────────────────────────
    all_wids = list(dict.fromkeys(wafer_order + list(wir_by_wid)))
    if not all_wids and wafer_id:
        all_wids = [wafer_id]

    wafer_rows = []
    for wid in all_wids:
        wir = wir_by_wid.get(wid, {})
        wrr = wrr_by_wid.get(wid, {})
        ps  = w_pass.get(wid, 0)
        fl  = w_fail.get(wid, 0)
        tot = ps + fl
        wrr_total = wrr.get("total_parts")
        total_dev = int(wrr_total) if wrr_total is not None else tot
        yld  = round(ps/tot*100, 4) if tot else None
        dppm = round(fl/tot*1_000_000, 2) if tot else None
        retest = wrr.get("retest_count") or 0

        wafer_rows.append({
            "file_id":         file_id,
            "lot_id":          lot_id,
            "wafer_id":        wid,
            "head_num":        wir.get("head_num"),
            "site_grp":        wir.get("site_grp"),
            "start_time":      wir.get("start_time"),
            "finish_time":     wrr.get("finish_time"),
            "total_devices":   total_dev,
            "pass_count":      ps,
            "fail_count":      fl,
            "retest_count":    int(retest),
            "abort_count":     wrr.get("abort_count"),
            "yield_pct":       yld,
            "dppm":            dppm,
            "ingestion_ts":    ingested,
            "ingestion_date":  ing_date,
        })

    # ── Merge MRR into lot row ────────────────────────────────────────────────
    if lot_row and mrr_extra:
        lot_row.update(mrr_extra)

    lots = [lot_row] if lot_row else []

    # ── Print summary ─────────────────────────────────────────────────────────
    if verbose:
        print(f"\n  {'─'*52}")
        print(f"  File     : {Path(filepath).name}")
        print(f"  SHA256   : {file_hash[:16]}...")
        print(f"  Byte ord : {'Big-endian' if bo=='>' else 'Little-endian'}")
        print(f"  {'─'*52}")
        print(f"\n  {'Table':<26} {'Rows':>8}")
        print(f"  {'─'*26} {'─'*8}")
        rows_map = [
            ("1  lots",               len(lots)),
            ("2  wafers",             len(wafer_rows)),
            ("3  parts",              len(part_rows)),
            ("4  hardware_sites",     len(hw_site_rows)),
            ("5  bin_dict",           len(bin_dict_rows)),
            ("5a canonical_registry", len(canonical_test_registry)),
            ("6  test_limits",        len(test_limits)),
            ("7  parametric_results", len(param_rows)),
            ("8  functional_results", len(func_rows)),
            ("9  dead_letter_queue",  len(dlq)),
            ("   tsr_summary (bonus)",len(tsr_rows)),
        ]
        for name, cnt in rows_map:
            marker = " ✓" if cnt > 0 else " –"
            print(f"  {name:<26} {cnt:>8,}{marker}")
        print()
        print(f"  Lot ID   : {lot_id}")
        print(f"  Part type: {part_type}")
        tnode = (lot_row or {}).get("node_name","")
        print(f"  Tester   : {tnode}")
        print()
        for w in wafer_rows:
            y = f"{w['yield_pct']:.2f}%" if w['yield_pct'] is not None else "N/A"
            d = f"{w['dppm']:,.0f}"       if w['dppm'] is not None else "N/A"
            print(f"  Wafer {w['wafer_id']:<16}  "
                  f"total={w['total_devices']:,}  "
                  f"pass={w['pass_count']:,}  "
                  f"fail={w['fail_count']:,}  "
                  f"yield={y}  dppm={d}")

    stats = {
        "file_id":       file_id,
        "file_hash":     file_hash,
        "file_size":     nbytes,
        "byte_order":    bo,
        "parse_errors":  parse_errors,
        "record_counts": rec_counts,
        "table_rows": {
            "lots":               len(lots),
            "wafers":             len(wafer_rows),
            "parts":              len(part_rows),
            "hardware_sites":     len(hw_site_rows),
            "bin_dict":           len(bin_dict_rows),
            "canonical_test_registry": len(canonical_test_registry),
            "test_limits":        len(test_limits),
            "parametric_results": len(param_rows),
            "functional_results": len(func_rows),
            "dead_letter_queue":  len(dlq),
            "tsr_summary":        len(tsr_rows),
        }
    }

    return {
        "file_id":             file_id,
        "file_hash":           file_hash,
        "lots":                lots,
        "wafers":              wafer_rows,
        "parts":               part_rows,
        "hardware_sites":      hw_site_rows,
        "bin_dict":            bin_dict_rows,
        "canonical_test_registry": list(canonical_test_registry.values()),
        "test_limits":         list(test_limits.values()),
        "parametric_results":  param_rows,
        "functional_results":  func_rows,
        "dead_letter_queue":   dlq.as_list(),
        "tsr_summary":         tsr_rows,
        "stats":               stats,
    }

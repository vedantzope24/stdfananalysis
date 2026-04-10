"""
parser/records.py
=================
Parsers for every STDF V4 record type needed by the 9-table schema.

Each parser function:
  - Takes raw body bytes
  - Takes the current byte-order character ('>' or '<')
  - Returns a flat dict (all values are Python primitives or None)
  - NEVER raises — errors produce partial dicts with available fields

Tables produced:
  Table 1  Lot          ← MIR + MRR
  Table 2  Wafer        ← WIR + WRR  (yield computed from PRR)
  Table 3  Part/Chip    ← PRR  (+site_number, attempt_index from PIR state)
  Table 4  Hardware Site ← SDR
  Table 5  Bin Dict     ← HBR + SBR
  Table 6  Test Limits  ← PTR metadata (unique test_num rows)
  Table 7  Parametric   ← PTR results (one row per measurement)
  Table 8  Functional   ← FTR
  Table 9  DLQ          ← Python parse errors
"""

import struct
from parser.utils import stdf_ts, normalise_unit, canonical_test_name

# STDF missing-value sentinels
U1_MISS = 0xFF
U2_MISS = 0xFFFF
U4_MISS = 0xFFFFFFFF
I2_MISS = -32768

PARSER_VERSION = "1.0.0"


# ── Low-level readers ─────────────────────────────────────────────────────────

def _u1(b, o, bo):
    return (b[o], o+1) if o < len(b) else (None, o)

def _u2(b, o, bo):
    return (struct.unpack_from(bo+"H",b,o)[0], o+2) if o+2<=len(b) else (None, o)

def _u4(b, o, bo):
    return (struct.unpack_from(bo+"I",b,o)[0], o+4) if o+4<=len(b) else (None, o)

def _i1(b, o, bo):
    return (struct.unpack_from("b",b,o)[0], o+1) if o<len(b) else (0, o)

def _i2(b, o, bo):
    return (struct.unpack_from(bo+"h",b,o)[0], o+2) if o+2<=len(b) else (None, o)

def _r4(b, o, bo):
    if o+4 > len(b): return (None, o)
    v = struct.unpack_from(bo+"f",b,o)[0]
    try:
        if v != v or abs(v) > 1e37: return (None, o+4)
    except Exception: return (None, o+4)
    return (round(v,9), o+4)

def _cn(b, o):
    if o >= len(b): return ("", o)
    l = b[o]; o += 1
    if l == 0: return ("", o)
    text = b[o:o+l].decode("ascii", errors="replace").strip()
    return (text, o+l)

def _char(b, o):
    if o >= len(b): return (None, o+1)
    c = chr(b[o]).strip()
    return (c or None, o+1)

def _skip(b, o, n):
    return min(o+n, len(b))

def _clean(v, sentinel):
    return None if v == sentinel else v


# ── FAR ───────────────────────────────────────────────────────────────────────

def parse_FAR(body: bytes) -> tuple[str, dict]:
    """
    Returns (byte_order_char, fields).
    byte_order_char is '>' or '<' — used for all subsequent records.
    """
    cpu = body[0] if body else 1
    bo  = ">" if cpu == 1 else "<"
    ver = body[1] if len(body) > 1 else 4
    return bo, {"cpu_type": cpu, "stdf_version": ver}


# ── TABLE 1: Lot  (MIR + MRR) ────────────────────────────────────────────────

def parse_MIR(body: bytes, bo: str) -> dict:
    """Master Information Record → Table 1 (Lot)."""
    o = 0
    setup_t, o = _u4(body, o, bo)
    start_t, o = _u4(body, o, bo)
    stat,    o = _u1(body, o, bo)
    mode,    o = _char(body, o)
    rtst,    o = _char(body, o)
    prot,    o = _char(body, o)
    burn,    o = _u2(body, o, bo)
    cmod,    o = _char(body, o)

    r = {
        "setup_time":    stdf_ts(setup_t),
        "start_time":    stdf_ts(start_t),
        "station_num":   _clean(stat, U1_MISS),
        "mode_code":     mode,
        "burn_time_min": _clean(burn, U2_MISS),
    }
    for name in [
        "lot_id","part_type","node_name","tester_type",
        "job_name","job_rev","sublot_id","operator",
        "exec_type","exec_ver","test_code","test_temp",
        "user_text","aux_file","pkg_type","family_id",
        "date_code","facility_id","floor_id","process_id",
        "oper_freq","spec_name","spec_ver","flow_id",
        "setup_id","design_rev","eng_id","rom_code",
        "serial_num","supervisor",
    ]:
        v, o = _cn(body, o)
        r[name] = v or None
    return r


def parse_MRR(body: bytes, bo: str) -> dict:
    """Master Results Record → merged into Table 1 (Lot)."""
    o = 0
    fin_t, o = _u4(body, o, bo)
    disp,  o = _char(body, o)
    udesc, o = _cn(body, o)
    edesc, o = _cn(body, o)
    return {
        "finish_time": stdf_ts(fin_t),
        "disp_code":   disp,
        "user_desc":   udesc or None,
    }


# ── TABLE 2: Wafer  (WIR + WRR) ──────────────────────────────────────────────

def parse_WIR(body: bytes, bo: str) -> dict:
    """Wafer Information Record → Table 2 (Wafer)."""
    o = 0
    head, o  = _u1(body, o, bo)
    sgrp, o  = _u1(body, o, bo)
    strt, o  = _u4(body, o, bo)
    wid,  o  = _cn(body, o)
    return {
        "wafer_id":   wid or None,
        "head_num":   head,
        "site_grp":   _clean(sgrp, U1_MISS),
        "start_time": stdf_ts(strt),
    }


def parse_WRR(body: bytes, bo: str) -> dict:
    """Wafer Results Record → Table 2 (Wafer)."""
    o = 0
    head, o  = _u1(body, o, bo)
    sgrp, o  = _u1(body, o, bo)
    fin,  o  = _u4(body, o, bo)
    tot,  o  = _u4(body, o, bo)
    ret,  o  = _u4(body, o, bo)
    abt,  o  = _u4(body, o, bo)
    good, o  = _u4(body, o, bo)
    func, o  = _u4(body, o, bo)
    wid,  o  = _cn(body, o)
    return {
        "wafer_id":     wid or None,
        "head_num":     head,
        "finish_time":  stdf_ts(fin),
        "total_parts":  _clean(tot,  U4_MISS),
        "retest_count": _clean(ret,  U4_MISS),
        "abort_count":  _clean(abt,  U4_MISS),
        "good_count":   _clean(good, U4_MISS),  # often 0xFFFFFFFF = not reported
    }


# ── TABLE 3: Part/Chip  (PIR + PRR) ──────────────────────────────────────────

def parse_PIR(body: bytes, bo: str) -> dict:
    """Part Information Record — marks start of device test."""
    head, _ = _u1(body, 0, bo)
    site, _ = _u1(body, 1, bo)
    return {"head_num": head, "site_num": site}


def parse_PRR(body: bytes, bo: str) -> dict:
    """
    Part Results Record → Table 3 (Part/Chip).
    attempt_index is injected by the pipeline state machine.
    """
    o = 0
    head, o  = _u1(body, o, bo)
    site, o  = _u1(body, o, bo)
    flag, o  = _u1(body, o, bo)
    ntst, o  = _u2(body, o, bo)
    hbin, o  = _u2(body, o, bo)
    sbin, o  = _u2(body, o, bo)
    x,    o  = _i2(body, o, bo)
    y,    o  = _i2(body, o, bo)
    tt,   o  = _u4(body, o, bo)
    pid,  o  = _cn(body, o)

    if flag is None: flag = 0
    pf = "PASS" if (int(flag) & 0x08) == 0 else "FAIL"
    return {
        "head_num":      head,
        "site_num":      site,
        "pass_fail":     pf,
        "hard_bin":      _clean(hbin, U2_MISS),
        "soft_bin":      _clean(sbin, U2_MISS),
        "x_coord":       _clean(x,    I2_MISS),
        "y_coord":       _clean(y,    I2_MISS),
        "num_tests_run": _clean(ntst, U2_MISS),
        "test_time_ms":  _clean(tt,   U4_MISS),
        "part_id":       pid or None,
        # attempt_index injected by pipeline
    }


# ── TABLE 4: Hardware Site  (SDR) ─────────────────────────────────────────────

def parse_SDR(body: bytes, bo: str) -> dict:
    """
    Site Description Record → Table 4 (Hardware Site).
    Describes physical handler, load board, DIB, cable etc.
    """
    o = 0
    head, o  = _u1(body, o, bo)
    sgrp, o  = _u1(body, o, bo)
    scnt, o  = _u1(body, o, bo)
    scnt_v   = int(scnt) if scnt else 0
    sites    = list(body[o: o + scnt_v]); o += scnt_v

    r = {
        "head_num":  head,
        "site_grp":  sgrp,
        "site_count":scnt_v,
        "site_nums": "|".join(str(s) for s in sites),
    }
    for name in [
        "handler_type","handler_id","card_type","card_id",
        "load_type","load_id","dib_type","dib_id",
        "cable_type","cable_id","contactor_type","contactor_id",
        "laser_type","laser_id","extra_type","extra_id",
    ]:
        v, o = _cn(body, o)
        r[name] = v or None
    return r


# ── TABLE 5: Bin Dictionary  (HBR + SBR) ─────────────────────────────────────

def parse_HBR(body: bytes, bo: str) -> dict:
    """Hardware Bin Record → Table 5 (Bin Dictionary)."""
    o = 0
    head, o  = _u1(body, o, bo)
    site, o  = _u1(body, o, bo)
    bnum, o  = _u2(body, o, bo)
    bcnt, o  = _u4(body, o, bo)
    bpf_b    = body[o] if o < len(body) else 0; o += 1
    bnam, o  = _cn(body, o)
    bpf_c    = chr(bpf_b) if bpf_b else ""
    pf = "PASS" if bpf_c.upper()=="P" else ("FAIL" if bpf_c.upper()=="F" else None)
    return {
        "bin_type":  "HARD",
        "bin_num":   bnum,
        "bin_name":  bnam or None,
        "bin_count": _clean(bcnt, U4_MISS),
        "pass_fail": pf,
        "head_num":  head,
        "site_num":  site,
    }


def parse_SBR(body: bytes, bo: str) -> dict:
    """Software Bin Record → Table 5 (Bin Dictionary)."""
    o = 0
    head, o  = _u1(body, o, bo)
    site, o  = _u1(body, o, bo)
    bnum, o  = _u2(body, o, bo)
    bcnt, o  = _u4(body, o, bo)
    bpf_b    = body[o] if o < len(body) else 0; o += 1
    bnam, o  = _cn(body, o)
    bpf_c    = chr(bpf_b) if bpf_b else ""
    pf = "PASS" if bpf_c.upper()=="P" else ("FAIL" if bpf_c.upper()=="F" else None)
    return {
        "bin_type":  "SOFT",
        "bin_num":   bnum,
        "bin_name":  bnam or None,
        "bin_count": _clean(bcnt, U4_MISS),
        "pass_fail": pf,
        "head_num":  head,
        "site_num":  site,
    }


# ── TABLE 6 + 7: Test Limits + Parametric Results  (PTR) ─────────────────────

def parse_PTR(body: bytes, bo: str) -> dict:
    """
    Parametric Test Record.
    Returns a COMBINED dict — the pipeline splits it into:
      Table 6: Test Limits  (test_num, canonical_name, limits, units)
      Table 7: Parametric Results  (device_id, test_num, result, pass/fail)
    """
    o = 0
    tnum, o  = _u4(body, o, bo)
    head, o  = _u1(body, o, bo)
    site, o  = _u1(body, o, bo)
    tflg, o  = _u1(body, o, bo)
    pflg, o  = _u1(body, o, bo)
    res,  o  = _r4(body, o, bo)
    tnam, o  = _cn(body, o)
    alid, o  = _cn(body, o)

    opt  = body[o] if o < len(body) else 0xFF; o += 1
    rscl, o  = _i1(body, o, bo)
    lscl, o  = _i1(body, o, bo)
    hscl, o  = _i1(body, o, bo)
    lolim, o = _r4(body, o, bo)
    hilim, o = _r4(body, o, bo)
    units, o = _cn(body, o)
    _, o     = _cn(body, o)   # c_resfmt
    _, o     = _cn(body, o)   # c_llmfmt
    _, o     = _cn(body, o)   # c_hlmfmt
    losp, o  = _r4(body, o, bo)
    hisp, o  = _r4(body, o, bo)

    lo_valid = not bool(int(opt) & 0x10)
    hi_valid = not bool(int(opt) & 0x40)
    if tflg is None: tflg = 0
    if pflg is None: pflg = 0

    # Pass/fail decode
    failed       = bool(int(tflg) & 0x80)
    alarm        = bool(int(tflg) & 0x40)
    not_executed = bool(int(tflg) & 0x01)
    failed_low   = bool(int(pflg) & 0x40)
    failed_high  = bool(int(pflg) & 0x80)

    # Scale result and limits
    def sc(v, exp):
        if v is None: return None
        try: return round(float(v) * (10**exp), 9) if exp else round(float(v), 9)
        except: return v

    result_scaled = sc(res,   rscl)
    lo_scaled     = sc(lolim, lscl) if lo_valid else None
    hi_scaled     = sc(hilim, hscl) if hi_valid else None

    # SI normalisation
    si_unit, si_factor = ("", 1.0)
    if units:
        si_unit, si_factor = normalise_unit(units)
    result_si = None
    if result_scaled is not None:
        try:
            result_si = round(result_scaled * si_factor, 12)
        except Exception:
            result_si = None

    # Canonical test name
    canon = canonical_test_name(tnam)

    return {
        # ── Table 6 fields (test definition — stored once per test_num) ──
        "test_num":       tnum,
        "test_name_raw":  tnam  or None,
        "canonical_name": canon or None,
        "lo_limit_scaled":lo_scaled,
        "hi_limit_scaled":hi_scaled,
        "orig_unit":      units  or None,
        "si_unit":        si_unit or None,
        "si_factor":      si_factor,
        "lo_spec":        losp,
        "hi_spec":        hisp,

        # ── Table 7 fields (result — one per device × test) ──────────────
        "head_num":       head,
        "site_num":       site,
        "result_raw":     res,
        "result_scaled":  result_scaled,
        "result_si":      result_si,
        "passed":         not failed,
        "failed_low":     failed_low,
        "failed_high":    failed_high,
        "alarm":          alarm,
        "not_executed":   not_executed,
        "alarm_id":       alid or None,
    }


# ── TABLE 8: Functional Results  (FTR) ───────────────────────────────────────

def parse_FTR(body: bytes, bo: str) -> dict:
    """Functional Test Record → Table 8 (Functional Results)."""
    o = 0
    tnum, o  = _u4(body, o, bo)
    head, o  = _u1(body, o, bo)
    site, o  = _u1(body, o, bo)
    tflg, o  = _u1(body, o, bo)
    opt  = body[o] if o < len(body) else 0xFF; o += 1
    cycl, o  = _u4(body, o, bo)
    rvadr,o  = _u4(body, o, bo)
    rcnt, o  = _u4(body, o, bo)
    nfail,o  = _u4(body, o, bo)
    xfail,o  = _i2(body, o, bo)
    yfail,o  = _i2(body, o, bo)
    voff, o  = _i2(body, o, bo)
    ricnt,o  = _u2(body, o, bo)
    picnt,o  = _u2(body, o, bo)
    ricnt_v  = int(ricnt) if ricnt else 0
    picnt_v  = int(picnt) if picnt else 0
    o = _skip(body, o, ricnt_v*2)
    o = _skip(body, o, (ricnt_v+1)//2)
    o = _skip(body, o, picnt_v*2)
    o = _skip(body, o, (picnt_v+1)//2)
    fpin_n = body[o] if o < len(body) else 0; o += 1
    o = _skip(body, o, int(fpin_n))
    tnam, o  = _cn(body, o)
    vnam, o  = _cn(body, o)
    tset, o  = _cn(body, o)
    opcd, o  = _cn(body, o)
    tnam2,o  = _cn(body, o)
    alid, o  = _cn(body, o)

    if tflg is None: tflg = 0
    passed = not bool(int(tflg) & 0x80)
    alarm  = bool(int(tflg) & 0x40)

    return {
        "test_num":      tnum,
        "test_name":     tnam or tnam2 or None,
        "vector_name":   vnam or None,
        "head_num":      head,
        "site_num":      site,
        "passed":        passed,
        "alarm":         alarm,
        "alarm_id":      alid or None,
        "cycle_count":   _clean(cycl,  U4_MISS),
        "fail_count":    _clean(nfail, U4_MISS),
        "repeat_count":  _clean(rcnt,  U4_MISS),
        "rel_vect_addr": _clean(rvadr, U4_MISS),
        "xfail_addr":    _clean(xfail, I2_MISS),
        "yfail_addr":    _clean(yfail, I2_MISS),
        "vect_offset":   _clean(voff,  I2_MISS),
        "time_set":      tset or None,
        "op_code":       opcd or None,
    }


# ── TSR (bonus: test synopsis aggregate) ─────────────────────────────────────

def parse_TSR(body: bytes, bo: str) -> dict:
    """Test Synopsis Record — aggregate stats (bonus table, not in 9-table core)."""
    o = 0
    head, o  = _u1(body, o, bo)
    site, o  = _u1(body, o, bo)
    ttyp = chr(body[o]) if o < len(body) else " "; o += 1
    tnum, o  = _u4(body, o, bo)
    ecnt, o  = _u4(body, o, bo)
    fcnt, o  = _u4(body, o, bo)
    acnt, o  = _u4(body, o, bo)
    tnam, o  = _cn(body, o)
    snam, o  = _cn(body, o)
    tlbl, o  = _cn(body, o)
    opt  = body[o] if o < len(body) else 0xFF; o += 1
    ttim, o  = _r4(body, o, bo)
    tmin, o  = _r4(body, o, bo)
    tmax, o  = _r4(body, o, bo)
    tsum, o  = _r4(body, o, bo)
    tsq,  o  = _r4(body, o, bo)
    return {
        "head_num":      head,
        "site_num":      site,
        "test_type":     ttyp.strip(),
        "test_num":      tnum,
        "test_name":     tnam or None,
        "exec_count":    _clean(ecnt, U4_MISS),
        "fail_count":    _clean(fcnt, U4_MISS),
        "alarm_count":   _clean(acnt, U4_MISS),
        "avg_exec_time": ttim,
        "min_result":    tmin,
        "max_result":    tmax,
        "sum_results":   tsum,
    }


# ── Record dispatcher ─────────────────────────────────────────────────────────

#  (rec_typ, rec_sub)  →  parser function
RECORD_MAP: dict[tuple, str] = {
    (0,  10): "FAR",  (1, 10): "MIR",  (1, 20): "MRR",
    (2,  10): "WIR",  (2, 20): "WRR",
    (5,  10): "PIR",  (5, 20): "PRR",
    (15, 10): "PTR",  (15,20): "FTR",  (15,15): "MPR",
    (10, 30): "TSR",
    (1,  40): "HBR",  (1, 50): "SBR",
    (1,  30): "PCR",  (1, 80): "SDR",
    (20, 10): "BPS",  (20,20): "EPS",
    (50, 10): "GDR",  (50,30): "DTR",
}

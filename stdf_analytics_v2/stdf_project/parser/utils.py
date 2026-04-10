"""
parser/utils.py
Shared utilities: SHA-256, timestamp conversion, unit normalisation,
canonical test name cleaning, and the Dead Letter Queue (DLQ).
"""
import hashlib
import re
import struct
from datetime import datetime, timezone
from pathlib import Path


# ── SHA-256 ──────────────────────────────────────────────────────────────────

def sha256_file(filepath: str) -> str:
    """Return SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ── Timestamp ─────────────────────────────────────────────────────────────────

def stdf_ts(unix_t) -> str | None:
    """Convert STDF Unix timestamp integer → UTC ISO-8601 string."""
    if not unix_t or unix_t == 0xFFFFFFFF:
        return None
    try:
        return datetime.fromtimestamp(unix_t, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except Exception:
        return None


def now_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def today_utc() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


# ── Unit normalisation ────────────────────────────────────────────────────────
#
# STDF files store units as short ASCII strings (e.g. "mV", "uA", "kohm").
# We normalise to SI base units and record the scale factor so that
# result_si = result_scaled * si_factor.
#
# Table: unit_string → (si_base_unit, scale_factor_to_si)
# e.g. "mV" → ("V", 1e-3)  means  result_si = result_mV * 1e-3

UNIT_TABLE: dict[str, tuple[str, float]] = {
    # Voltage
    "v":    ("V",  1.0),    "V":    ("V",  1.0),
    "mv":   ("V",  1e-3),   "mV":   ("V",  1e-3),
    "uv":   ("V",  1e-6),   "uV":   ("V",  1e-6),
    "kv":   ("V",  1e3),    "kV":   ("V",  1e3),
    # Current
    "a":    ("A",  1.0),    "A":    ("A",  1.0),
    "ma":   ("A",  1e-3),   "mA":   ("A",  1e-3),
    "ua":   ("A",  1e-6),   "uA":   ("A",  1e-6),
    "na":   ("A",  1e-9),   "nA":   ("A",  1e-9),
    # Resistance
    "ohm":  ("Ω",  1.0),    "Ohm":  ("Ω",  1.0),
    "kohm": ("Ω",  1e3),    "kOhm": ("Ω",  1e3),
    "mohm": ("Ω",  1e6),    "MOhm": ("Ω",  1e6),
    "r":    ("Ω",  1.0),
    # Time
    "s":    ("s",  1.0),    "sec":  ("s",  1.0),
    "ms":   ("s",  1e-3),   "us":   ("s",  1e-6),
    "ns":   ("s",  1e-9),   "ps":   ("s",  1e-12),
    # Frequency
    "hz":   ("Hz", 1.0),    "Hz":   ("Hz", 1.0),
    "khz":  ("Hz", 1e3),    "kHz":  ("Hz", 1e3),
    "mhz":  ("Hz", 1e6),    "MHz":  ("Hz", 1e6),
    "ghz":  ("Hz", 1e9),    "GHz":  ("Hz", 1e9),
    # Power
    "w":    ("W",  1.0),    "mw":   ("W",  1e-3),
    # Capacitance
    "f":    ("F",  1.0),    "pf":   ("F",  1e-12), "nf": ("F", 1e-9),
    # Dimensionless
    "%":    ("%",  1.0),    "":     ("",   1.0),
}


def normalise_unit(unit_str: str) -> tuple[str, float]:
    """
    Return (si_unit, scale_factor).
    scale_factor is multiplied by result_scaled to get result_si.
    Unknown units are returned as-is with factor 1.0.
    """
    if not unit_str:
        return ("", 1.0)
    u = unit_str.strip()
    entry = UNIT_TABLE.get(u) or UNIT_TABLE.get(u.lower())
    if entry:
        return entry
    return (u, 1.0)   # unknown unit — pass through unchanged


# ── Test name canonicalisation ────────────────────────────────────────────────

def canonical_test_name(raw: str) -> str:
    """
    Clean a raw test name into a stable, comparable canonical form.

    Rules:
    - Strip leading/trailing whitespace
    - Collapse multiple spaces
    - Remove pin suffix patterns like '<> glxy_pin2' (everything after '<>')
    - Strip non-alphanumeric chars except _ and - and .
    - Lowercase
    - Max 128 chars
    """
    if not raw:
        return ""
    # Remove pin suffix (e.g.  "glxy_SS_IH     <> glxy_pin2"  →  "glxy_SS_IH")
    name = re.split(r"\s*<>", raw)[0]
    # Collapse spaces
    name = re.sub(r"\s+", "_", name.strip())
    # Strip special chars
    name = re.sub(r"[^\w\-\.]", "", name)
    # Lowercase
    name = name.lower().strip("_")
    return name[:128]


# ── Dead Letter Queue ─────────────────────────────────────────────────────────

class DeadLetterQueue:
    """
    Table 9: Quarantine zone for parsing errors.

    Collects records that could not be parsed due to:
    - Truncated / corrupt record body
    - Missing mandatory fields (MIR/WIR absent)
    - Unexpected byte values
    - Python exceptions during parsing

    Each entry is a flat dict ready for DB insertion or JSON export.
    """

    def __init__(self, file_id: str):
        self.file_id = file_id
        self.entries: list[dict] = []

    def add(
        self,
        offset: int,
        rec_type: str,
        reason: str,
        raw_hex: str = "",
        exception: str = "",
    ):
        self.entries.append({
            "file_id":    self.file_id,
            "offset":     offset,
            "rec_type":   rec_type,
            "reason":     reason,       # SHORT description: TRUNCATED, PARSE_ERROR, etc.
            "raw_hex":    raw_hex[:256],# First 256 hex chars of body
            "exception":  exception[:512],
            "logged_at":  now_utc(),
        })

    def __len__(self):
        return len(self.entries)

    def as_list(self) -> list[dict]:
        return list(self.entries)

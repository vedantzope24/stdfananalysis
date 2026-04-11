import pytest
from datetime import datetime, timezone
import hashlib
from parser.utils import (
    sha256_bytes, 
    stdf_ts, 
    normalise_unit, 
    canonical_test_name, 
    DeadLetterQueue
)

def test_sha256_bytes():
    data = b"hello STDF"
    expected_hash = hashlib.sha256(data).hexdigest()
    assert sha256_bytes(data) == expected_hash

def test_stdf_ts():
    # Test valid timestamp
    # Unix time 1704067200 -> 2024-01-01T00:00:00Z
    assert stdf_ts(1704067200) == "2024-01-01T00:00:00Z"
    
    # Test blank or None
    assert stdf_ts(None) is None
    assert stdf_ts(0) is None
    
    # Test invalid timestamp (e.g. 0xFFFFFFFF STDF marker)
    assert stdf_ts(4294967295) is None

def test_normalise_unit():
    # Capitalization tests
    assert normalise_unit("mV") == ("V", 1e-3)
    assert normalise_unit("mv") == ("V", 1e-3)
    
    # Known units
    assert normalise_unit("ms") == ("s", 1e-3)
    assert normalise_unit("kohm") == ("Ω", 1e3)
    assert normalise_unit("ghz") == ("Hz", 1e9)
    assert normalise_unit("%") == ("%", 1.0)
    
    # Unknown units
    assert normalise_unit("xyz") == ("xyz", 1.0)
    assert normalise_unit("") == ("", 1.0)
    assert normalise_unit(None) == ("", 1.0)

def test_canonical_test_name():
    # Null cases
    assert canonical_test_name(None) == ""
    assert canonical_test_name("") == ""
    
    # Strip suffixes
    assert canonical_test_name("glxy_SS_IH     <> glxy_pin2") == "glxy_ss_ih"
    
    # Space collapsing and punctuation cleaning
    # Keeps dashes and dots, removes special chars, collapses spaces
    assert canonical_test_name("Test#$Name   1") == "testname_1"
    assert canonical_test_name("  some_test-name.123  ") == "some_test-name.123"

def test_dead_letter_queue():
    dlq = DeadLetterQueue("test_file_id")
    assert len(dlq) == 0
    
    dlq.add(offset=100, rec_type="UNK", reason="PARSE_ERROR", raw_hex="ABCD", exception="ValueError")
    assert len(dlq) == 1
    
    entry = dlq.as_list()[0]
    assert entry["file_id"] == "test_file_id"
    assert entry["offset"] == 100
    assert entry["rec_type"] == "UNK"
    assert entry["reason"] == "PARSE_ERROR"
    assert entry["raw_hex"] == "ABCD"
    assert entry["exception"] == "ValueError"
    assert "logged_at" in entry

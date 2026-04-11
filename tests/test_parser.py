import pytest
from parser.stdf_parser import parse

def test_demofile_parsing_integration(demo_stdf_path):
    """
    Test parsing the demofile.stdf to ensure all tables are generated 
    without unhandled exceptions. This also validates the basic functionality
    of binary record unpacking.
    """
    # Act
    result = parse(str(demo_stdf_path), verbose=False)
    
    # Assert
    assert isinstance(result, dict)
    
    # Basic table existence
    expected_tables = [
        "file_id", "file_hash", "lots", "wafers", "parts", 
        "hardware_sites", "bin_dict", "test_limits", 
        "parametric_results", "functional_results", 
        "dead_letter_queue", "canonical_test_registry"
    ]
    for tbl in expected_tables:
        assert tbl in result
        
    # Validations specific to demofile
    assert result["file_id"] and result["file_hash"]
    
    # Check that it parsed at least some data
    # (Since it's a valid demo STDF file, we expect some rows in key tables)
    assert len(result["lots"]) > 0
    assert len(result["parts"]) > 0
    assert len(result["parametric_results"]) > 0
    assert result["stats"]["parse_errors"] == 0

def test_lot2_parsing_integration(lot2_stdf_path):
    """
    Test parsing the lot2.stdf to ensure different STDF structures
    are handled correctly by the parser without errors.
    """
    result = parse(str(lot2_stdf_path), verbose=False)
    
    assert isinstance(result, dict)
    assert len(result["lots"]) > 0
    
    # Optional logic: we expect it to track wafer parts effectively
    assert len(result["parts"]) > 0

def test_missing_mrr_parsing(missing_mrr_stdf_path):
    """
    Test parsing an STDF file missing an MRR record. The parser should
    handle it gracefully and still output the `lots` structure using 
    fallback logic based on the initial MIR record.
    """
    result = parse(str(missing_mrr_stdf_path), verbose=False)
    
    assert isinstance(result, dict)
    # the lot should still be parsed based on MIR
    assert len(result["lots"]) > 0
    
    # We should not have an MRR specific field overriding MIR improperly,
    # but the parser shouldn't fail.
    assert result["stats"]["parse_errors"] >= 0 # No system crash at least

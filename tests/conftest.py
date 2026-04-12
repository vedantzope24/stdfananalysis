import pytest
from pathlib import Path

# Provide a root directory path for test files
@pytest.fixture
def test_data_dir() -> Path:
    current_dir = Path(__file__).parent
    data_dir = current_dir.parent / "test_data"
    return data_dir

@pytest.fixture
def demo_stdf_path(test_data_dir) -> Path:
    return test_data_dir / "golden_sample.stdf"

@pytest.fixture
def lot2_stdf_path(test_data_dir) -> Path:
    return test_data_dir / "test_big_endian.stdf"

@pytest.fixture
def missing_mrr_stdf_path(test_data_dir) -> Path:
    return test_data_dir / "error_invalid_strings.stdf"

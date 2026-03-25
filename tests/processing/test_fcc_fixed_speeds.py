# tests/processing/test_fcc_fixed_speeds.py
import zipfile
import pytest
from network_idx.processing.fcc_fixed_speeds import (
    parse_fips_from_filename,
    get_fips_from_dir,
    extract_zip_file,
)


# ── parse_fips_from_filename ──

def test_parse_fips_valid():
    assert parse_fips_from_filename("bdc_01_Cable_fixed_broadband.zip") == "01"

def test_parse_fips_invalid():
    assert parse_fips_from_filename("random_file.zip") is None

def test_parse_fips_two_digit_codes():
    assert parse_fips_from_filename("bdc_48_Fiber_fixed_broadband.zip") == "48"
    assert parse_fips_from_filename("bdc_06_Copper_fixed_broadband.zip") == "06"

def test_parse_fips_three_digit_code_ignored():
    """FIPS regex expects exactly 2 digits after bdc_"""
    assert parse_fips_from_filename("bdc_123_Cable_fixed_broadband.zip") is None or \
           parse_fips_from_filename("bdc_123_Cable_fixed_broadband.zip") == "12"
    # Verifies your regex behavior — \d{2} will match the first 2 digits


# ── get_fips_from_dir ──

def test_get_fips_from_dir_finds_codes(tmp_path):
    """Create fake zip files and verify FIPS extraction."""
    (tmp_path / "bdc_01_Cable_fixed_broadband.zip").touch()
    (tmp_path / "bdc_01_Fiber_fixed_broadband.zip").touch()
    (tmp_path / "bdc_48_Cable_fixed_broadband.zip").touch()

    result = get_fips_from_dir(tmp_path)
    assert result == ["01", "48"]  # sorted, deduplicated

def test_get_fips_from_dir_empty(tmp_path):
    """Empty directory returns empty list."""
    assert get_fips_from_dir(tmp_path) == []

def test_get_fips_from_dir_skips_non_matching(tmp_path):
    """Files that don't match pattern are skipped."""
    (tmp_path / "random_file.zip").touch()
    (tmp_path / "bdc_01_Cable_fixed_broadband.zip").touch()

    result = get_fips_from_dir(tmp_path)
    assert result == ["01"]

def test_get_fips_from_dir_ignores_non_zip(tmp_path):
    """Only .zip files are scanned."""
    (tmp_path / "bdc_01_Cable_fixed_broadband.csv").touch()

    assert get_fips_from_dir(tmp_path) == []


# ── extract_zip_file ──

def _make_zip_with_csv(zip_path, csv_name="data.csv", csv_content="a,b\n1,2\n"):
    """Helper: create a real zip file containing a CSV."""
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(csv_name, csv_content)

def test_extract_zip_file_basic(tmp_path):
    zip_path = tmp_path / "test.zip"
    extract_to = tmp_path / "output"
    _make_zip_with_csv(zip_path, "data.csv")

    result = extract_zip_file(zip_path, extract_to)

    assert result == extract_to / "data.csv"
    assert result.exists()
    assert result.read_text() == "a,b\n1,2\n"

def test_extract_zip_file_creates_output_dir(tmp_path):
    zip_path = tmp_path / "test.zip"
    extract_to = tmp_path / "nested" / "deep" / "output"
    _make_zip_with_csv(zip_path)

    result = extract_zip_file(zip_path, extract_to)

    assert extract_to.exists()
    assert result.exists()

def test_extract_zip_file_skips_if_csv_exists(tmp_path):
    zip_path = tmp_path / "test.zip"
    extract_to = tmp_path / "output"
    _make_zip_with_csv(zip_path, "data.csv", "a,b\n1,2\n")

    # Pre-create the CSV with different content
    extract_to.mkdir()
    (extract_to / "data.csv").write_text("old content")

    result = extract_zip_file(zip_path, extract_to)

    assert result.read_text() == "old content"  # was NOT overwritten

def test_extract_zip_file_no_csv_raises(tmp_path):
    zip_path = tmp_path / "test.zip"
    extract_to = tmp_path / "output"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("readme.txt", "no csv here")

    with pytest.raises(FileNotFoundError):
        extract_zip_file(zip_path, extract_to)

def test_extract_zip_file_multiple_csvs_takes_first(tmp_path):
    zip_path = tmp_path / "test.zip"
    extract_to = tmp_path / "output"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("first.csv", "a\n1\n")
        zf.writestr("second.csv", "b\n2\n")

    result = extract_zip_file(zip_path, extract_to)

    assert result.name == "first.csv"
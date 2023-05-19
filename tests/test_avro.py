import os
import tempfile
from pathlib import Path
from data_tools.utils.avro import AvroUtils

TEST_DATA_DIR = Path(__file__).resolve().parent


def test_create_default_sample():
    # Create a temporary directory to store files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Generate sample data file path
        file_path = Path(tmpdir) / "sample.avro"

        # Generate sample data
        schema_path = TEST_DATA_DIR / "sample_schema.avsc"
        sample_size = 100
        AvroUtils.create_sample(file_path, schema_path, sample_size)

        assert file_path.exists()
        assert os.path.getsize(file_path) > 0


def test_create_sample():
    # Create a temporary directory to store files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Generate sample data file path
        file_path = Path(tmpdir) / "sample.avro"

        # Generate sample data
        schema_path = TEST_DATA_DIR / "sample_schema.avsc"
        sample_size = 100
        codec = "deflate"
        metadata = {"Description": "Sample data file"}
        sync_interval = 1024 * 1024
        AvroUtils.create_sample(file_path, schema_path, sample_size, codec, metadata, sync_interval)

        assert file_path.exists()
        assert os.path.getsize(file_path) > 0


def test_snappy_meta():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test-snappy.avro"
    result = AvroUtils.meta(file_path)
    excpected_schema = {"fields": [{"name": "stringField", "type": "string"}, {"name": "longField", "type": "long"}],
                        "name": "Test", "type": "record"}

    # Add assertions to verify the expected output
    assert isinstance(result, tuple)
    assert len(result) == 4
    assert result[0] == excpected_schema
    assert result[2] == "snappy"
    assert result[3] == 13677


def test_deflate_meta():
    file_path = TEST_DATA_DIR / "data" / "avro" / "test-deflate.avro"
    result = AvroUtils.meta(file_path)
    expected_schema = {"fields": [{"name": "stringField", "type": "string"}, {"name": "longField", "type": "long"}],
                       "name": "Test", "type": "record"}

    # Add assertions to verify the expected output
    assert isinstance(result, tuple)
    assert len(result) == 4
    assert result[0] == expected_schema
    assert result[2] == "deflate"
    assert result[3] == 12556


def test_meta():
    file_path = TEST_DATA_DIR / "data" / "avro" / "weather.avro"
    result = AvroUtils.meta(file_path)
    expected_schema = {"doc": "A weather reading.",
                       "fields": [{"name": "station", "type": "string"}, {"name": "time", "type": "long"},
                                  {"name": "temp", "type": "int"}], "name": "test.Weather", "type": "record"}

    # Add assertions to verify the expected output
    assert isinstance(result, tuple)
    assert len(result) == 4
    assert result[0] == expected_schema
    assert result[2] == "null"
    assert result[3] == 358

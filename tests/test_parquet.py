import os
import tempfile
from pathlib import Path
from data_tools.utils.parquet import ParquetUtils

TEST_DATA_DIR = Path(__file__).resolve().parent


def test_create_default_sample():
    # Create a temporary directory to store files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Generate sample data file path
        file_path = Path(tmpdir) / "sample.parquet"

        # Generate sample data
        schema_path = TEST_DATA_DIR / "sample_schema.avsc"
        sample_size = 100
        ParquetUtils.create_sample(file_path, schema_path, sample_size)

        assert file_path.exists()
        assert os.path.getsize(file_path) > 0


def test_create_sample():
    # Create a temporary directory to store files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Generate sample data file path
        file_path = Path(tmpdir) / "sample.parquet"

        # Generate sample data
        schema_path = TEST_DATA_DIR / "sample_schema.avsc"
        sample_size = 100
        codec = None
        metadata = {"Description": "Sample data file"}
        sync_interval = 1024 * 1024
        ParquetUtils.create_sample(file_path, schema_path, sample_size, codec, metadata, sync_interval)

        assert file_path.exists()
        assert os.path.getsize(file_path) > 0


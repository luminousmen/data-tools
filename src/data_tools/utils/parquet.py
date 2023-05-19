import json
from pathlib import Path
import typing as T

import fastavro
import pyarrow as pa
import pyarrow.parquet as pq
from data_tools.utils.base import BaseUtils


class ParquetUtils(BaseUtils):
    @classmethod
    def create_sample(cls, file_path: Path, schema_path: Path, sample_size: int, codec: str = None,
            metadata=None, sync_interval: int = 1024 * 1024) -> Path:
        """
        Create a random sample data file given an Avro schema file.
        """
        if metadata is None:
            metadata = {"Name": "Dummy data"}

        with open(schema_path, "r") as f:
            schema = json.load(f)
            schema = fastavro.parse_schema(schema=schema)

        sample_data = cls.generate_data(schema, sample_size)

        table = pa.Table.from_pylist(sample_data)
        table = table.replace_schema_metadata(metadata)

        pq.write_table(table, file_path, compression=codec)
        return file_path

    @classmethod
    def meta(cls, file_path: Path) -> T.Tuple:
        """
        Inspect metadata of an Avro or Parquet file.
        """
        parquet_file = pq.ParquetFile(file_path)
        codec = parquet_file.metadata.row_group(0).column(0).compression
        return parquet_file.schema, parquet_file.metadata, codec, parquet_file.metadata

    @classmethod
    def tail(cls, file_path: Path, n: int = 20):
        """
        Prints the last N records of a Parquet file.
        """
        table = pq.read_table(str(file_path), use_threads=True)
        total_rows = table.num_rows
        start_row = max(total_rows - n, 0)
        for row in table.slice(start_row, total_rows).to_pydict().values():
            print(row)

    @classmethod
    def head(cls, file_path: Path, n: int = 20):
        table = pa.parquet.read_table(file_path).slice(0, n)
        cls._print_table(table)

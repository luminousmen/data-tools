import os
import json
import typing as T
from pathlib import Path

import fastavro

from data_tools.utils.base import BaseUtils


class AvroUtils(BaseUtils):
    @classmethod
    def create_sample(
            cls, file_path: Path, schema_path: Path, sample_size: int, codec: str = "null",
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

        with open(file_path, "wb") as f:
            fastavro.writer(f, schema, sample_data, codec=codec, metadata=metadata,
                            sync_interval=sync_interval, codec_compression_level=None)
        return file_path

    @classmethod
    def meta(cls, file_path: Path) -> T.Tuple:
        """
        Inspect metadata of an Avro file.
        """
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            schema = avro_reader.writer_schema
            serialized_size = os.path.getsize(file_path)

            cls.print_metadata(schema, avro_reader.metadata, avro_reader.codec, serialized_size)
            return schema, avro_reader.metadata, avro_reader.codec, serialized_size

    @classmethod
    def schema(cls, file_path: Path) -> None:
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            schema = avro_reader.writer_schema
            serialized_size = os.path.getsize(file_path)

            print(f"Avro schema: {schema}")
            print(f"Avro metadata: {avro_reader.metadata}, {serialized_size}")

    @classmethod
    def stats(cls, file_path: Path) -> T.Tuple[int, T.Dict]:
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            num_rows = 0
            column_stats = {}
            for row in avro_reader:
                num_rows += 1
                for k, v in row.items():
                    column_stat = column_stats.get(k, {
                        "count": 0,
                        "null_count": 0,
                        "min": None,
                        "max": None
                    })
                    column_stat["count"] += 1
                    if v is None:
                        column_stat["null_count"] += 1
                    elif column_stat["min"] is None or v < column_stat["min"]:
                        column_stat["min"] = v
                    elif column_stat["max"] is None or v > column_stat["max"]:
                        column_stat["max"] = v
                    column_stats[k] = column_stat
            print(column_stat)
            return num_rows, column_stats

    @classmethod
    def tail(cls, file_path: Path, n: int = 20) -> T.List:
        """
        Returns the last N records of an Avro file as a list of dictionaries.
        """
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            records = list(avro_reader)
            num_records = len(records)

            # Calculate the number of records to read
            num_to_read = min(num_records, n)
            # Read the last N records and store them in a list
            records = [record for record in records[num_records - num_to_read:num_records]]
            print(records)
            return records

    @classmethod
    def head(cls, file_path: Path, n: int = 20):
        with open(file_path, "rb") as f:
            avro_reader = fastavro.reader(f)
            for i, record in enumerate(avro_reader):
                if i == n:
                    break
                print(record)

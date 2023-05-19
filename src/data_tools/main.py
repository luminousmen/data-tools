from pathlib import Path

import argparse as argparse

from data_tools.utils.avro import AvroUtils
from data_tools.utils.parquet import ParquetUtils


def get_file_format(file_path: Path) -> str:
    """
    Identify file format based on file extension.
    """
    ext = file_path.suffix.lower()
    if ext == ".avro":
        return "avro"
    elif ext == ".parquet":
        return "parquet"
    elif ext == ".csv":
        return "csv"
    elif ext == ".json":
        return "json"
    else:
        raise ValueError("Unsupported file format.")


def main(command: str, file_path: Path):
    file_format = get_file_format(file_path)
    if file_format == "avro":
        utilsCls = AvroUtils
    elif file_format == "parquet":
        utilsCls = ParquetUtils
    else:
        raise ValueError("Unsupported file format.")

    function = getattr(utilsCls, command)
    function(file_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", type=str, help="Command")
    parser.add_argument("file_path", type=Path, help="Path to the file")
    args = parser.parse_args()
    main(args.command, args.file_path)

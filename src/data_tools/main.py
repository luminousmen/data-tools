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


def init_args():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(help="commands", dest="command")

    # data-tools head
    head_parser = subparsers.add_parser("head")
    head_parser.add_argument("file_path", action="store")

    # data-tools tail
    tail_parser = subparsers.add_parser("tail")
    tail_parser.add_argument("file_path", type=Path, action="store")

    # data-tools meta
    meta_parser = subparsers.add_parser("meta")
    meta_parser.add_argument("file_path", type=Path, action="store")

    # data-tools create_sample
    create_sample_parser = subparsers.add_parser("create_sample")
    create_sample_parser.add_argument("schema_path", type=Path, action="store")
    create_sample_parser.add_argument("sample_size", type=int, action="store")
    create_sample_parser.add_argument("file_path", type=Path, action="store")

    # data-tools schema
    schema_parser = subparsers.add_parser("schema")
    schema_parser.add_argument("file_path", type=Path, action="store")

    # data-tools stats
    stats_parser = subparsers.add_parser("stats")
    stats_parser.add_argument("file_path", type=Path, action="store")

    # data-tools query
    query_parser = subparsers.add_parser("query")
    query_parser.add_argument("file_path", type=Path, action="store")
    query_parser.add_argument("query_expression", type=str, action="store")

    args = parser.parse_args()
    return args


def main():
    args = init_args()
    file_path = Path(args.file_path)
    file_format = get_file_format(file_path)
    if file_format == "avro":
        utilsCls = AvroUtils
    elif file_format == "parquet":
        utilsCls = ParquetUtils
    else:
        raise ValueError("Unsupported file format.")

    if hasattr(utilsCls, args.command):
        function = getattr(utilsCls, args.command)
        function_args = []
        for arg_name in vars(args):
            if arg_name != "command":
                function_args.append(getattr(args, arg_name))
        function(*function_args)
    else:
        raise ValueError("Invalid command.")


if __name__ == "__main__":
    main()

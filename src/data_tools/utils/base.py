import random
import string
import timeit
import typing as T
from pathlib import Path

import duckdb
import pyarrow as pa


class BaseUtils:
    @classmethod
    def _generate_random_record(cls, schema: T.Dict) -> T.Dict:
        """
        Generate a random record based on the given Avro or Parquet schema.
        """
        record = {}
        # @TODO: support union types
        # @TODO: support logical types
        for field in schema["fields"]:
            if field["type"] == "null":
                record[field["name"]] = None
            elif field["type"] == "boolean":
                record[field["name"]] = random.choice([True, False])
            elif field["type"] == "int":
                record[field["name"]] = random.randint(-2147483648, 2147483647)
            elif field["type"] == "long":
                record[field["name"]] = random.randint(-9223372036854775808, 9223372036854775807)
            elif field["type"] == "float":
                record[field["name"]] = random.uniform(-3.4e38, 3.4e38)
            elif field["type"] == "double":
                record[field["name"]] = random.uniform(-1.7e308, 1.7e308)
            elif field["type"] == "string":
                record[field["name"]] = cls._generate_random_string()
            elif field["type"] == "bytes":
                record[field["name"]] = cls._generate_random_bytes()
            elif field["type"] == "array":
                record[field["name"]] = cls._generate_random_array(field["items"])
            elif field["type"] == "map":
                record[field["name"]] = cls._generate_random_map(field["values"])
            elif field["type"] == "record":
                record[field["name"]] = cls._generate_random_record(field)
            elif isinstance(field["type"], dict) and field["type"]["type"] == "record":
                record[field["name"]] = cls._generate_random_record(field["type"])
            elif isinstance(field["type"], dict) and field["type"]["type"] == "array":
                record[field["name"]] = cls._generate_random_array(field["type"]["items"])
            elif isinstance(field["type"], dict) and field["type"]["type"] == "map":
                record[field["name"]] = cls._generate_random_map(field["type"]["values"])
            elif field["type"] == "fixed":
                size = field["size"]
                record[field["name"]] = cls._generate_random_fixed(size)
            elif isinstance(field["type"], dict) and field["type"]["type"] == "fixed":
                size = field["type"]["size"]
                record[field["name"]] = cls._generate_random_fixed(size)
            elif field["type"] == "enum":
                symbols = field["symbols"]
                record[field["name"]] = cls._generate_random_enum(symbols)
            elif isinstance(field["type"], dict) and field["type"]["type"] == "enum":
                symbols = field["type"]["symbols"]
                record[field["name"]] = cls._generate_random_enum(symbols)
            else:
                raise ValueError("Unsupported type: {}".format(field["type"]))
        return record

    @classmethod
    def _generate_random_fixed(cls, size: int) -> bytes:
        """
        Generate a random fixed bytes value of the given size.
        """
        return bytes([random.randint(0, 255) for _ in range(size)])

    @classmethod
    def _generate_random_enum(cls, symbols: T.List[str]) -> str:
        """
        Generate a random enum value from the given symbols.
        """
        return random.choice(symbols)

    @classmethod
    def _generate_random_string(cls, length: int = 10) -> str:
        """
        Generate a random string of given length.
        """
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @classmethod
    def _generate_random_bytes(cls, length: int = 10) -> bytes:
        """
        Generate a random byte string of given length.
        """
        return bytes(random.getrandbits(8) for _ in range(length))

    @classmethod
    def _generate_random_array(cls, items_schema: T.Union[str, T.Dict]) -> T.List:
        """
        Generate a random array of items based on the given Avro or Parquet schema.
        """
        items = []
        for _ in range(random.randint(1, 5)):
            if isinstance(items_schema, str):
                items.append(cls._generate_random_string())
            elif isinstance(items_schema, dict) and items_schema["type"] == "record":
                items.append(cls._generate_random_record(items_schema))
            elif isinstance(items_schema, dict) and items_schema["type"] == "array":
                items.append(cls._generate_random_array(items_schema["items"]))
            elif isinstance(items_schema, dict) and items_schema["type"] == "map":
                items.append(cls._generate_random_map(items_schema["values"]))
            else:
                raise ValueError("Unsupported type: {}".format(items_schema["type"]))
        return items

    @classmethod
    def _generate_random_map(cls, values_schema: T.Union[str, T.Dict]) -> T.Dict:
        """
        Generate a random map of values based on the given Avro or Parquet schema.
        """
        values = {}
        for _ in range(random.randint(1, 5)):
            key = cls._generate_random_string()
            if isinstance(values_schema, str):
                values[key] = cls._generate_random_string()
            elif isinstance(values_schema, dict) and values_schema["type"] == "record":
                values[key] = cls._generate_random_record(values_schema)
            elif isinstance(values_schema, dict) and values_schema["type"] == "array":
                values[key] = cls._generate_random_array(values_schema["items"])
            elif isinstance(values_schema, dict) and values_schema["type"] == "map":
                values[key] = cls._generate_random_map(values_schema["values"])
            else:
                raise ValueError("Unsupported type: {}".format(values_schema["type"]))
        return values
    
    @classmethod
    def _generate_random_value(cls, schema: T.Dict) -> any:
        """
        Generate a random value based on the given Avro or Parquet schema.
        """
        if schema["type"] == "null":
            return None
        elif schema["type"] == "boolean":
            return random.choice([True, False])
        elif schema["type"] == "int":
            return random.randint(-2147483648, 2147483647)
        elif schema["type"] == "long":
            return random.randint(-9223372036854775808, 9223372036854775807)
        elif schema["type"] == "float":
            return random.uniform(-3.4e38, 3.4e38)
        elif schema["type"] == "double":
            return random.uniform(-1.7e308, 1.7e308)
        elif schema["type"] == "string":
            return cls._generate_random_string()
        elif schema["type"] == "bytes":
            return cls._generate_random_bytes()
        elif schema["type"] == "array":
            return cls._generate_random_array(schema["items"])
        elif schema["type"] == "map":
            return cls._generate_random_map(schema["values"])
        elif schema["type"] == "record":
            return cls._generate_random_record(schema)
        else:
            raise ValueError("Unsupported type: {}".format(schema["type"]))

    @classmethod
    def generate_data(cls, schema, sample_size: int) -> T.List:
        # Generate random sample data using the Avro schema
        sample_data = [cls._generate_random_record(schema) for _ in range(sample_size)]
        return sample_data

    @staticmethod
    def time_method(method):
        def timed(*args, **kwargs):
            ts = timeit.default_timer()
            result = method(*args, **kwargs)
            te = timeit.default_timer()
            print(f"{method.__name__} took {te - ts:.6f} seconds")
            return result

        return timed

    def create_sample(self, file_path: Path, schema_path: Path, sample_size: int):
        raise NotImplementedError

    @staticmethod
    def _print_table(table):
        # Convert table to dictionary of lists
        data_dict = table.to_pydict()

        # Get list of column names
        column_names = table.column_names

        # Iterate over rows and print as dictionaries
        for i in range(len(data_dict[column_names[0]])):
            row_dict = {}
            for col_name in column_names:
                row_dict[col_name] = data_dict[col_name][i]
            print(row_dict)

    def meta(self, file_path: Path):
        raise NotImplementedError

    @staticmethod
    def print_metadata(schema, metadata, codec, serialized_size):
        print(f"Schema: {schema}")
        print(f"Metadata: {metadata}")
        print(f"Codec: {codec}")
        print(f"Serialized size: {serialized_size}")

    @classmethod
    def query(cls, file_path: Path, query_expression: str):
        """
        Query and filter data in an Avro or Parquet file.
        """
        my_arrow_table = cls.to_arrow_table(file_path)

        con = duckdb.connect()
        con.register(file_path.name, my_arrow_table)

        # Run query that selects part of the data
        query = con.execute(query_expression)

        # Create Record Batch Reader from Query Result.
        # "fetch_record_batch()" also accepts an extra parameter related to the desired produced chunk size.
        record_batch_reader = query.fetch_record_batch()

        # Retrieve all batch chunks
        all_chunks = []
        while True:
            try:
                # Process a single chunk here
                # pyarrow.lib.RecordBatch
                chunk = record_batch_reader.read_next_batch()
                all_chunks.append(chunk)
            except StopIteration:
                break
        data = pa.Table.from_batches(all_chunks)
        cls._print_table(data)

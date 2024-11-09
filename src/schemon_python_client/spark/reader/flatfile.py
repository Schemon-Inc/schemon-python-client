from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import StructType, DataType, StringType
from pyspark.sql import functions as F

# Define a type for the append_columns structure
from typing import Dict, Any

from schemon_python_client.spark.helper.custom_function import parse_built_in_function
from schemon_python_client.spark.helper.databricks import get_widget_value


def read(
    spark: SparkSession,
    file_path: str,
    format: str = "csv",
    struct_type: StructType = None,
    append_columns: Dict[str, Dict[str, DataType]] = None,
    reader_options: Dict[str, Any] = None,
) -> SparkDataFrame:
    """
    Reads data from the specified file path, applies the given struct_type and format,
    and appends additional columns with specified values and types.

    :param spark: The Spark session.
    :param file_path: Path to the input file.
    :param format: The file format (csv, tsv, json, parquet, avro).
    :param struct_type: The struct_type to apply to the DataFrame.
    :param append_columns: Dictionary specifying columns to append, where each key
                           is a column name, and each value is a dictionary with
                           'value' and 'type' keys.
                           Example:
                           {
                               "new_column_1": {"value": "default_value", "type": StringType()},
                               "new_column_2": {"value": 100, "type": IntegerType()},
                           }
    :param reader_options: Additional options for the reader.
    :return: A DataFrame with the data from the file and appended columns.
    """
    # Initialize default values for optional dictionaries
    append_columns = append_columns or {}
    reader_options = reader_options or {}

    # Read the file with the specified format, schema/struct_type, and options
    if format == "csv" or format == "tsv":
        df = spark.read.csv(file_path, schema=struct_type, **reader_options)
    elif format == "json":
        df = spark.read.json(file_path, schema=struct_type, **reader_options)
    elif format == "parquet":
        df = spark.read.parquet(file_path, schema=struct_type, **reader_options)
    elif format == "avro":
        # Apply struct_type after loading if struct_type is provided for Avro
        if struct_type:
            df = (
                spark.read.format("avro")
                .load(file_path, **reader_options)
                .selectExpr(
                    *[
                        f"CAST({field.name} AS {field.dataType.simpleString()}) AS {field.name}"
                        for field in struct_type
                    ]
                )
            )
        else:
            df = spark.read.format("avro").load(file_path, **reader_options)
    else:
        raise ValueError(f"Unsupported format ({format}) for reading.")

    # Check if DataFrame is empty
    if df.rdd.isEmpty():
        return None
    else:
        # Add columns based on append_columns with specified values and types
        for col_name, col_spec in append_columns.items():
            col_value = col_spec.get("value")
            if col_value == "metadata.full_path":
                col_value = file_path
            elif col_value.startswith("get_widget_value("):
                _, field_func_args = parse_built_in_function(
                    col_value,
                )
                name = field_func_args.get("name")
                default = field_func_args.get("default")
                if default == "None":
                    default = None
                col_value = get_widget_value(name, default)
            col_type: DataType = col_spec.get(
                "type", StringType()
            )  # Default to StringType if type is not provided
            df = df.withColumn(col_name, F.lit(col_value).cast(col_type))

        return df

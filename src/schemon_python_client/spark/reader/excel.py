import pandas as pd
from pyspark.sql import SparkSession
from typing import Dict
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.types import StructType
from schemon_python_client.spark.helper.custom_function import parse_built_in_function, run_custom_function
from schemon_python_client.spark.helper.databricks import get_widget_value
from schemon_python_client.spark.helper.excel import (
    get_excel_cell_value,
    get_excel_last_saved,
    get_sheet_names,
    handle_usecols,
)
from schemon_python_client.spark.helper.pandas import (
    align_columns_to_expected,
    pandas_to_spark,
)


def read(
    spark: SparkSession,
    file_path: str,
    sheet_names: str,
    skip_rows: int,
    total_rows: int,
    use_columns: str,
    struct_type: StructType = None,
    column_names: list = None,
    data_types=str,
    sheet_names_to_exclude: str = None,
    append_columns: Dict[str, Dict[str, str]] = None,
) -> SparkDataFrame:
    """
    Reads data from the specified Excel file path, applies the given schema and format,
    and appends additional columns with specified values and types.
    It uses Pandas read_excel() to read the Excel file and convert it to a Spark DataFrame.

    :param spark: The Spark session.
    :param file_path: Path to the input Excel file.
    :param sheet_names: Comma-separated list of sheet names to read. If sheet_names_to_exclude provided, it will be ignored. 
    :param skip_rows: Number of rows to skip from the top.
    :param total_rows: Number of rows to read.
    :param use_columns: Comma-separated list of column indices to read. 
                        It supports range like 'A:E' or 'A:~' if the column length is unknown.
    :param struct_type: The schema to apply to the final Spark DataFrame.
    :param column_names: List of column names to apply to the Pandas DataFrame during pandas.read_excel().
    :param data_types: The data types to apply to the Pandas DataFrame columns during pandas.read_excel(). Default to str or string.
    :param sheet_names_to_exclude: Comma-separated list of sheet names to exclude. If provided, sheet_names will be ignored. 
    :param append_columns:  Dictionary specifying columns to append, where each key is a column name, and each value is a dictionary with 'value' and 'type' keys.
                            It supports built-in metadata values like 'metadata.index', 'metadata.modified', 'metadata.full_path', 'metadata.sheet_name'.
                            Some built-in functions are provided like 'get_excel_cell_value', 'get_widget_value'.
                            It also supports custom functions defined in the 'def' format.
                            Example:
                            {
                                "new_column_1": {"value": "default_value", "type": "string"},
                                "new_column_2": {"value": 100, "type": "integer"},
                            }
    :return: A Spark DataFrame with the data from the Excel file and appended columns.
    """
    df_list = []
    append_columns = append_columns or {}
    if sheet_names_to_exclude is None:
        sheet_name_list = [name for name in sheet_names.split(",")]
    else:
        sheet_name_list = get_sheet_names(file_path, sheet_names_to_exclude)

    for sheet_name in sheet_name_list:
        usecols = handle_usecols(file_path, sheet_name, use_columns)
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            skiprows=skip_rows,
            nrows=total_rows,
            usecols=usecols,
            names=column_names,
            dtype=data_types,
            header=None,
        )

        if df.empty:
            continue

        if column_names and len(column_names) > 0:
            df = align_columns_to_expected(df, column_names)

        for col_name, col_spec in append_columns.items():
            col_value = col_spec.get("value")
            col_type = col_spec.get("type", "string")
            if col_value == "metadata.index":
                col_value = df.index + 1
            elif col_value == "metadata.modified":
                col_value = get_excel_last_saved(file_path, col_type)
            elif col_value == "metadata.full_path":
                col_value = file_path
            elif col_value == "metadata.sheet_name":
                col_value = sheet_name
            elif col_value.startswith("get_excel_cell_value("):
                _, field_func_args = parse_built_in_function(
                    col_value,
                )
                path = field_func_args.get("path")
                sheet_name_arg = field_func_args.get("sheet_name")
                cell = field_func_args.get("cell")
                if path == "metadata.full_path":
                    path = file_path
                if sheet_name_arg == "metadata.sheet_name":
                    sheet_name = sheet_name
                else:
                    sheet_name = sheet_name_arg
                col_value = get_excel_cell_value(path, sheet_name, cell)
            elif col_value.startswith("get_widget_value("):
                _, field_func_args = parse_built_in_function(
                    col_value,
                )
                name = field_func_args.get("name")
                default = field_func_args.get("default")
                if default == "None":
                    default = None
                col_value = get_widget_value(name, default)
            elif col_value.startswith("def "):
                metadata = {"full_path": file_path}
                col_value = run_custom_function(
                    col_value, metadata
                )
            else:
                col_value = col_value

            df[col_name] = col_value
            df[col_name] = df[col_name].astype(col_type)

        spark_df: SparkDataFrame = pandas_to_spark(spark, df, struct_type)

        df_list.append(spark_df)

    if df_list:
        combined_df: SparkDataFrame = df_list[0]
        for df in df_list[1:]:
            combined_df = combined_df.unionByName(df, allowMissingColumns=True)

        return combined_df
    else:
        return None

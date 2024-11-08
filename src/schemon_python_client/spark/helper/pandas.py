from typing import List
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField


def validate_pandas_data_against_schema(
    pandas_df: pd.DataFrame, schema: StructType
) -> pd.DataFrame:
    """
    Validates that the pandas DataFrame columns match the expected schema in terms of
    column count, order, data type compatibility, and nullability.

    :param pandas_df: The pandas DataFrame to validate.
    :param schema: The StructType schema to validate against.
    :return: Validated pandas DataFrame with NaN/NA values replaced appropriately.
    :raises ValueError: If there's a mismatch in data types, column count, or nullability.
    """
    # Sort the schema fields to match the DataFrame columns order
    sorted_schema = StructType(
        sorted(
            schema.fields, key=lambda field: list(pandas_df.columns).index(field.name)
        )
    )

    # Check if columns match in count and order
    if len(pandas_df.columns) != len(sorted_schema.fields):
        raise ValueError("Column count mismatch between DataFrame and schema.")

    for col, field in zip(pandas_df.columns, sorted_schema.fields):
        # Check if column names and data types match the schema
        if col != field.name:
            raise ValueError(
                f"Column name mismatch: expected '{field.name}' but found '{col}'"
            )

        # Check if nullable fields contain no nulls if nullable=False
        if not field.nullable and pandas_df[col].isnull().any():
            raise ValueError(
                f"Column '{col}' contains nulls, but schema specifies non-nullable."
            )

    # Replace NaN/NA with None for PySpark compatibility
    pandas_df = pandas_df.replace({pd.NA: None, np.nan: None})

    return pandas_df


def pandas_to_spark(
    spark: SparkSession,
    pandas_df: pd.DataFrame,
    schema: StructType,
):
    """
    Converts a validated Pandas DataFrame to a PySpark DataFrame with an explicit schema.
    """
    # Ensure column names are strings and validate against schema
    pandas_df.columns = pandas_df.columns.astype(str)
    pandas_df = validate_pandas_data_against_schema(pandas_df, schema)

    # Convert the Pandas DataFrame to a list of tuples
    data = [tuple(x) for x in pandas_df.to_numpy()]

    # Create a PySpark DataFrame with the explicit schema
    spark_df = spark.createDataFrame(data, schema=schema)
    return spark_df


def align_columns_to_expected(
    df: pd.DataFrame,
    column_names: List[str],
) -> pd.DataFrame:
    """
    Ensures the DataFrame has columns that exactly match the `column_names`:
    - Adds any missing columns from `column_names`, filling with NaN values.
    - Removes any extra columns not specified in `column_names`.

    Parameters:
    - df (pd.DataFrame): The DataFrame to adjust.
    - column_names (List[str]): The list of fields the DataFrame should contain.

    Returns:
    - pd.DataFrame: The adjusted DataFrame with columns aligned to `column_names`.

    # Sometimes, Excel can have dynamic columns, so we use "A:~" tilda to get all available columns.
    # This can lead to column mismatch between expected fields and Excel columns.
    # Adding the missing columns manually can guarantee the expected fields are respected.
    # More Column Names than Actual Columns:

    # If column_names has more entries than the columns in the Excel file,
    # Pandas will create additional columns filled with NaN values for the extra names provided.
    # For example, if the Excel file has three columns but column_names contains five names,
    # the resulting DataFrame will have five columns, with the last two columns populated entirely with NaN values.
    # Fewer Column Names than Actual Columns:

    # If column_names has fewer entries than the columns in the Excel file,
    # Pandas will use only the specified names for the first few columns.
    # The extra columns in the file without corresponding names in column_names will automatically be assigned default names by Pandas (Unnamed: n),
    # where n is the column position starting from the last named column.
    """
    current_fields = df.columns.tolist()
    missing_fields = [field for field in column_names if field not in current_fields]
    extra_fields = [field for field in current_fields if field not in column_names]

    # Add missing fields with NaN values
    for field in missing_fields:
        df[field] = pd.Series([None] * len(df), dtype="string")

    # Drop extra fields to match exactly with column_names
    df = df.drop(columns=extra_fields)

    # Reorder columns to match column_names order
    df = df[column_names]

    return df

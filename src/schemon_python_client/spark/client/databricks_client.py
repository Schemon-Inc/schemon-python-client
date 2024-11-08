from datetime import datetime
from typing import Dict, Optional
from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType
from pyspark.sql.streaming import StreamingQuery
from schemon_python_client.spark.base.client import Client
from schemon_python_client.spark.credential_manager.unity_catalog_credential_manager import (
    UnityCatalogCredentialManager,
)
from schemon_python_logger.print import print_sql
from schemon_python_client.spark.listener.streaming_trigger_listener import (
    StreamingTriggerListener,
)
from typing import Any
from functools import partial


class DatabricksClient(Client):
    def __init__(
        self,
        spark: SparkSession,
        platform: str,
        format: str,
        credential_manager: UnityCatalogCredentialManager,
        show_sql: bool = False,
    ):
        super().__init__(
            spark=spark,
            provider="dataricks",
            name="Databricks",
            platform=platform,
            format=format,
            credential_manager=credential_manager,
        )
        self.show_sql = show_sql

    def list_tables(self, database: str, schema: str) -> SparkDataFrame:
        try:
            query = f"SHOW TABLES IN {database}.{schema}"
            df = self.execute_query(query)
            return df
        except AnalysisException as e:
            print(f"An error occurred while listing tables: {str(e)}")
            raise e

    def set_database(self, database: str):
        try:
            self.spark.catalog.setCurrentDatabase(database)
        except AnalysisException as e:
            raise e

    def check_table_exists(self, database: str, schema: str, table: str) -> bool:
        try:
            exist = self.spark.catalog.tableExists(f"{database}.{schema}.{table}")
            return exist
        except AnalysisException:
            return False

    def truncate(self, database: str, schema: str, table: str):
        query = f"TRUNCATE TABLE {database}.{schema}.{table}"
        if self.show_sql:
            print_sql(query)
        self.spark.sql(query)

    def execute_query(self, query: str) -> SparkDataFrame:
        try:
            if self.show_sql:
                print_sql(query)
            df = self.spark.sql(query)
            return df
        except AnalysisException as e:
            print(f"An error occurred while executing the query: {str(e)}")
            raise e

    def read(
        self,
        database: str,
        schema: str,
        table: str,
        columns: Optional[list[str]] = None,
        use_sql: bool = False,
    ) -> SparkDataFrame:
        try:
            table_path = f"{database}.{schema}.{table}"
            df_reader = self.spark.read.format(self.format)
            df = df_reader.table(table_path)

            if columns:
                df = df.select(*columns)

            if use_sql:
                columns_str = ", ".join(columns) if columns else "*"
                print_sql(f"SELECT {columns_str} FROM {table_path}")

            return df

        except AnalysisException as e:
            print(f"An error occurred while reading the table: {str(e)}")
            raise e

    def write(
        self,
        df: SparkDataFrame,
        database: str,
        schema: str,
        table: str,
        mode: str = "append",
    ):
        try:
            df.write.format(self.format).mode(mode).saveAsTable(
                f"{database}.{schema}.{table}"
            )
        except AnalysisException as e:
            print(f"An error occurred while writing data: {str(e)}")
            raise e

    def join(
        self,
        df: SparkDataFrame,
        lookup_table: str,
        join_type: str,
        join_conditions: list,
        lookup_columns: list,
    ) -> SparkDataFrame:
        try:
            lookup_df = self.spark.table(lookup_table)

            df_alias = df.alias("source_df")
            lookup_df_alias = lookup_df.alias("lookup_df")

            join_expr = None
            for condition in join_conditions:
                df_col, lookup_col = condition.split("=")
                df_col = df_col.strip()
                lookup_col = lookup_col.strip()

                if join_expr is None:
                    join_expr = df_alias[df_col] == lookup_df_alias[lookup_col]
                else:
                    join_expr &= df_alias[df_col] == lookup_df_alias[lookup_col]

            joined_df = df_alias.join(lookup_df_alias, join_expr, join_type)
            final_columns = [df_alias[col] for col in df.columns] + [
                lookup_df_alias[col] for col in lookup_columns
            ]
            final_df = joined_df.select(*final_columns)

            return final_df

        except AnalysisException as e:
            print(f"An error occurred while joining tables: {str(e)}")
            raise e

    def merge(
        self,
        database: str,
        schema: str,
        table: str,
        merge_condition: str,
        update_condition: str,
        update_set: dict,
        insert_set: dict,
        source_table: str = None,
        source_df: SparkDataFrame = None,
        use_sql: bool = False,
    ):
        try:
            if not source_table and not source_df:
                raise ValueError("Source table or DataFrame must be provided.")

            target_table = f"{database}.{schema}.{table}"

            if use_sql:
                if not source_table:
                    source_table = f"temp_table_schemon_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
                    source_df.createOrReplaceTempView(source_table)
                merge_query = f"""
                MERGE INTO {target_table} AS target
                USING {source_table} AS source
                ON {merge_condition}
                WHEN MATCHED AND ({update_condition}) THEN
                UPDATE SET {", ".join([f"{target_col} = {source_col}" for target_col, source_col in update_set.items()])}
                WHEN NOT MATCHED BY TARGET THEN
                INSERT ({", ".join(insert_set.keys())}) 
                VALUES ({", ".join(insert_set.values())})
                """

                if self.show_sql:
                    print_sql(merge_query)

                result = self.spark.sql(merge_query)

            else:
                target_table = DeltaTable.forName(self.spark, target_table)
                if update_set:
                    result = (
                        target_table.alias("target")
                        .merge(source_df.alias("source"), merge_condition)
                        .whenMatchedUpdate(condition=update_condition, set=update_set)
                        .whenNotMatchedInsert(values=insert_set)
                        .execute()
                    )
                else:
                    result = (
                        target_table.alias("target")
                        .merge(source_df.alias("source"), merge_condition)
                        .whenNotMatchedInsert(values=insert_set)
                        .execute()
                    )
            return result

        except Exception as e:
            print(f"An error occurred during the merge: {str(e)}")
            raise e

    def unpivot(
        self,
        df: SparkDataFrame,
        id_columns: list,
        key_column_name: str,
        value_column_name: str,
        value_column_type: str,
        first_row_contains_header: bool = False,
        row_number_column: str = None,
        use_sql: bool = False,
    ) -> SparkDataFrame:
        """
        Unpivot the DataFrame.

        :param df: Input DataFrame to unpivot
        :param id_columns: List of columns to keep as is.
            id_columns, also called id_vars generally in unpivot operation,
            refers to the columns in a DataFrame that you want to keep as is
            while transforming the remaining columns from wide format to long format.
        :param key_column_name: Name of the key column.
            A key column refers to the original column name
            that becomes part of the new column in the long-format DataFrame.
            The key column holds the names of the unpivoted columns
            that were converted from wide format to long format.
        :param value_column_name: Name of the value column.
            A value column is the column that holds the actual values
            from the original wide-format columns
            that have been transformed into a single long-format column.
        :param value_column_type: Type of the value column
        :param first_row_contains_header: Whether the first row contains header values
        :param row_number_column: Name of the row number column.
            If the first row contains header values,
            the row number column is used to identify the header row.
        :param use_sql: Whether to use SQL UNPIVOT clause

        :return: Unpivoted DataFrame
        """
        try:
            if first_row_contains_header:
                # Identify the header row based on the smallest row_number_col value
                header_row = df.filter(
                    F.col(row_number_column)
                    == F.lit(df.agg(F.min(row_number_column)).first()[0])
                ).limit(1)

                for col_name in header_row.columns:
                    header_row = header_row.withColumn(
                        col_name,
                        F.regexp_replace(F.col(col_name), "['\"]", ""),
                    )

                # Extract the header values directly using a SQL transformation (without collect)
                header_expr = [F.col(col).alias(col) for col in df.columns]
                headers = header_row.select(*header_expr).first()

                # Drop the header row from the original DataFrame
                df_without_header = df.filter(
                    F.col(row_number_column)
                    != F.lit(df.agg(F.min(row_number_column)).first()[0])
                )

                # Automatically generate the list of value columns as all columns except id_columns
                value_columns = [
                    col
                    for col in df_without_header.columns
                    if col not in id_columns
                    and headers[col] != None
                    and col != row_number_column
                ]
            else:
                df_without_header = df
                value_columns = [col for col in df.columns if col not in id_columns]

            if use_sql:
                # Use SQL UNPIVOT clause
                temp_table_name = (
                    f"temp_table_schemon_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
                )
                df_without_header.createOrReplaceTempView(temp_table_name)

                # Build the UNPIVOT SQL query
                unpivot_query = f"""
                    SELECT {", ".join(id_columns)}, {key_column_name}, CAST({value_column_name} AS {value_column_type}) AS {value_column_name}
                    FROM {temp_table_name}
                    UNPIVOT (
                        {value_column_name} FOR {key_column_name} IN ({", ".join(value_columns)})
                    )
                """
                if self.show_sql:
                    print_sql(unpivot_query)

                unpivoted_df = self.spark.sql(unpivot_query)

            else:
                unpivot_expr = F.expr(
                    f"stack({len(value_columns)}, "
                    + ", ".join(
                        [
                            f"'{col}', CAST(`{col}` AS {value_column_type})"
                            for col in value_columns
                        ]
                    )
                    + ")"
                )
                unpivoted_df = df_without_header.select(
                    *id_columns, unpivot_expr.alias(key_column_name, value_column_name)
                )

            row_count = unpivoted_df.count()
            return unpivoted_df

        except AnalysisException as e:
            print(f"An error occurred while unpivoting: {str(e)}")
            raise e

    def read_stream(
        self,
        path: str,
        schema: StructType = None,
        use_autoloader: bool = False,
        format: str = "delta",
        options: dict = None,
        watermark_column: str = None,
        watermark_delay: str = "10 minutes",
        **kwargs: Dict[str, Any],
    ) -> SparkDataFrame:
        try:
            if options is None:
                options = {}

            if use_autoloader:
                supported_formats_with_metadata = {
                    "csv",
                    "json",
                    "parquet",
                    "text",
                    "binaryFile",
                }

                # Load data using Auto Loader
                stream_df = (
                    self.spark.readStream.format("cloudFiles")
                    .option("cloudFiles.format", format)
                    .options(**options)
                    .schema(schema)
                    .load(path)
                )

            else:
                # Regular readStream
                stream_df = (
                    self.spark.readStream.format(format).options(**options).load(path)
                )

            # Handle reserved metadata keys and custom columns from kwargs
            for col_name, value in kwargs.items():
                if value == "metadata.full_path":
                    stream_df = stream_df.withColumn(col_name, F.input_file_name())
                elif (
                    value == "metadata.modified"
                    and format in supported_formats_with_metadata
                ):
                    stream_df = stream_df.withColumn(
                        col_name,
                        F.col("_metadata.file_modification_time").cast(TimestampType()),
                    )
                elif callable(value):
                    # If a function is provided, register it as a UDF if arguments are needed
                    if isinstance(value, partial):
                        # If value is a partial function with args, register and apply it
                        udf_col = F.udf(
                            value.func,
                            returnType=value.keywords.get("returnType", None),
                        )
                        stream_df = stream_df.withColumn(
                            col_name, udf_col(*[F.lit(arg) for arg in value.args])
                        )
                    else:
                        # Directly register as a UDF without arguments
                        udf_col = F.udf(value)
                        stream_df = stream_df.withColumn(col_name, udf_col())
                else:
                    # Directly add as a literal value or column transformation if specified
                    stream_df = stream_df.withColumn(
                        col_name,
                        F.lit(value) if not isinstance(value, F.Column) else value,
                    )

            # Apply watermarking if both column and delay are specified
            if watermark_column and watermark_delay:
                stream_df = stream_df.withWatermark(watermark_column, watermark_delay)

            return stream_df

        except Exception as e:
            print(f"An error occurred while reading the stream: {str(e)}")
            raise e

    def write_stream(
        self,
        df: SparkDataFrame,
        database: str,
        schema: str,
        table: str,
        checkpoint_path: str,
        output_mode: str = "append",
        trigger_interval: str = "10 seconds",
        foreach_batch_function: callable = None,
        **kwargs,
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to a Delta table.

        :param df: The streaming DataFrame to write.
        :param database: The database name.
        :param schema: The schema name.
        :param table: The table name.
        :param checkpoint_path: Path to store checkpoint information.
        :param output_mode: The output mode, typically "append", "complete", or "update".
        :param trigger_interval: Trigger interval for streaming, e.g., "10 seconds".
        :param foreach_batch_function: Optional custom function to process each batch (used in foreachBatch).
        :return: StreamingQuery object.
        """
        target_table = f"{database}.{schema}.{table}"

        try:
            if (
                not hasattr(self.spark, "trigger_listener_added")
                or not self.spark.trigger_listener_added
            ):
                self.spark.streams.addListener(StreamingTriggerListener())
                self.spark.trigger_listener_added = True
                print("StreamingTriggerListener added.")
            else:
                print("StreamingTriggerListener already exists. Skipping.")

            write_stream = (
                df.writeStream.format("delta")
                .outputMode(output_mode)
                .option("checkpointLocation", checkpoint_path)
                .trigger(processingTime=trigger_interval)
            )

            # Use foreachBatch if a custom function is provided
            if foreach_batch_function:
                write_stream = write_stream.foreachBatch(
                    lambda batch_df, epoch_id: foreach_batch_function(
                        batch_df, epoch_id, target_table, **kwargs
                    )
                )
            else:
                write_stream = write_stream.toTable(target_table)

            query = write_stream.start()
            print(f"Started streaming to {target_table} with query ID: {query.id}")
            return query

        except Exception as e:
            print(f"An error occurred while writing the stream: {str(e)}")
            raise e

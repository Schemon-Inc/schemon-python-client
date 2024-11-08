from abc import abstractmethod
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from schemon_python_client.spark.base.base import Base
from schemon_python_client.spark.base.credential_manager import CredentialManager
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery
from typing import Any, Optional, Dict


class Client(Base):
    def __init__(
        self,
        spark: SparkSession,
        provider: str,
        name: str,
        platform: str = None,
        format: str = None,
        credential_manager: CredentialManager = None,
    ):
        """
        Initializes the Client with necessary Spark and configuration details.

        :param spark: The SparkSession instance.
        :param provider: The vendor/provider name (e.g., "AWS").
        :param name: The name of the client (e.g., "Databricks").
        :param platform: The platform used (e.g., Databricks).
        :param format: The data format (e.g., "delta").
        :param credential_manager: Credential manager for authentication.
        """
        self.spark = spark
        self.provider = provider
        self.platform = platform
        self.format = format
        self.name = name
        self.credential_manager = credential_manager

    @abstractmethod
    def check_database_exists(self, database: str) -> bool:
        """
        Checks if a specified database exists.

        :param database: The name of the database.
        :return: True if the database exists, False otherwise.
        """
        pass

    @abstractmethod
    def check_table_exists(self, database: str, schema: str, table: str) -> bool:
        """
        Checks if a specified table exists within a given database and schema.

        :param database: The database name.
        :param schema: The schema name.
        :param table: The table name.
        :return: True if the table exists, False otherwise.
        """
        pass

    @abstractmethod
    def list_tables(self, database: str, schema: str) -> SparkDataFrame:
        """
        Lists all tables available in the database.

        :param database: The database name.
        :param schema: The schema name.
        :return: A DataFrame containing the list of tables.
        """
        pass

    @abstractmethod
    def truncate(self, database: str, schema: str, table: str):
        """
        Truncates a specified table, removing all its records.

        :param database: The database name.
        :param schema: The schema name.
        :param table: The table name to truncate.
        """
        pass

    @abstractmethod
    def read(
        self,
        database: str,
        schema: str,
        table: str,
        columns: Optional[list[str]] = None,
        use_sql: bool = False,
    ) -> SparkDataFrame:
        """
        Reads data from the specified table in the given database and schema.

        :param database: The database name.
        :param schema: The schema name.
        :param table: The table name.
        :param columns: Optional list of columns to select from the table.
        :param use_sql: Whether to use SQL SELECT clause.
        :return: A SparkDataFrame containing the data from the specified table.
        """
        pass

    @abstractmethod
    def write(
        self,
        df: SparkDataFrame,
        database: str,
        schema: str,
        table: str,
        mode: str = "append",
    ):
        """
        Writes a DataFrame to a specified table.

        :param df: The DataFrame to write.
        :param database: The target database name.
        :param schema: The target schema name.
        :param table: The target table name.
        :param mode: The write mode, typically "append" or "overwrite".
        """
        pass

    @abstractmethod
    def execute_query(self, query: str) -> SparkDataFrame:
        """
        Executes a SQL query and returns the result as a DataFrame.

        :param query: The SQL query to execute.
        :return: A DataFrame with the query result.
        """
        pass

    @abstractmethod
    def join(
        self,
        query: str,
        df: SparkDataFrame,
        lookup_table: str,
        join_type: str,
        join_conditions: list,
        lookup_columns: list,
    ) -> SparkDataFrame:
        """
        Joins a DataFrame with a lookup table using specified join conditions.

        :param query: The main query string.
        :param df: The DataFrame to join.
        :param lookup_table: The lookup table to join with.
        :param join_type: Type of join (e.g., "inner", "left").
        :param join_conditions: List of join conditions.
        :param lookup_columns: List of columns to select from the lookup table.
        :return: The resulting DataFrame after the join.
        """
        pass

    @abstractmethod
    def merge(
        self,
        database: str,
        schema: str,
        table: str,
        merge_condition: str,
        update_condition: str,
        update_set: dict,
        insert_set: dict,
        source_table: str,
        source_df: SparkDataFrame,
        use_sql: bool = False,
    ) -> SparkDataFrame:
        """
        Merges source data into a target table with specified conditions.

        :param database: Target database name.
        :param schema: Target schema name.
        :param table: Target table name.
        :param merge_condition: Condition for merging.
        :param update_condition: Condition for updating.
        :param update_set: Columns to update in the target table.
        :param insert_set: Columns to insert when not matched.
        :param source_table: Name of the source table.
        :param source_df: DataFrame of the source data.
        :param use_sql: Whether to use SQL-based merging.
        :return: The resulting DataFrame after the merge.
        """
        pass

    @abstractmethod
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
        Transforms a DataFrame from wide format to long format.

        :param df: The DataFrame to unpivot.
        :param id_columns: List of columns to keep as identifiers.
        :param key_column_name: Name for the unpivoted column holding keys.
        :param value_column_name: Name for the unpivoted column holding values.
        :param value_column_type: Type of the value column.
        :param first_row_contains_header: Whether the first row contains headers.
        :param row_number_column: Row number column, if needed.
        :param use_sql: Whether to use SQL for unpivoting.
        :return: The unpivoted DataFrame.
        """
        pass

    @abstractmethod
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
        """
        Reads a streaming DataFrame with options to add custom metadata columns.

        :param path: The path to read the stream from.
        :param schema: The schema to apply to the streaming data.
        :param use_autoloader: Flag to indicate if Auto Loader should be used.
        :param format: The format of the files (e.g., "parquet", "json", etc.).
        :param options: Dictionary of options to pass to the reader.
        :param watermark_column: The column to apply the watermark on.
        :param watermark_delay: The delay threshold for watermarking (e.g., "10 minutes").
        :param kwargs: Additional column definitions. Reserved keys include "metadata.full_path" and "metadata.modified".
                       Values can be literal, column transformations, or callable UDFs (including with arguments).
        :return: A streaming DataFrame.
        """

    @abstractmethod
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
        Writes a streaming DataFrame to a Delta table.

        :param df: The DataFrame to write.
        :param database: Target database name.
        :param schema: Target schema name.
        :param table: Target table name.
        :param checkpoint_path: Path to store checkpoint information.
        :param output_mode: Output mode, e.g., "append", "complete".
        :param trigger_interval: Trigger interval for streaming.
        :param foreach_batch_function: Optional function for foreachBatch processing.
        :param kwargs: Additional arguments for the batch function.
        :return: A StreamingQuery object.
        """
        pass

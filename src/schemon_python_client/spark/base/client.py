from abc import abstractmethod
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from schemon_python_client.spark.base.base import Base
from schemon_python_client.spark.base.credential_manager import CredentialManager


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
        self.spark = spark
        self.provider = provider
        self.platform = platform
        self.format = format
        self.name = name
        self.credential_manager = credential_manager

    @abstractmethod
    def check_database_exists(self, database: str) -> bool:
        pass

    @abstractmethod
    def check_table_exists(self, database: str, schema: str, table: str) -> bool:
        pass

    @abstractmethod
    def list_tables(self) -> SparkDataFrame:
        pass

    @abstractmethod
    def truncate(self, database: str, schema: str, table: str):
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
        pass

    @abstractmethod
    def execute_query(self, query: str) -> SparkDataFrame:
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
        pass

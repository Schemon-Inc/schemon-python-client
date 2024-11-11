from typing import Optional
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.utils import AnalysisException
from schemon_python_client.spark.base.client import Client
from schemon_python_client.spark.credential_manager.mssql_credential_manager import (
    MSSQLCredentialManager,
)
from schemon_python_client.spark.helper.mssql import get_mssql_jdbc_connection
from schemon_python_logger.print import print_sql


class MSSQLClient(Client):
    def __init__(
        self,
        spark: SparkSession,
        server: str,
        database: str,
        credential_manager: MSSQLCredentialManager,
        driver_type: str,
        show_sql: bool = False,
        connection_options: dict = None,
        provider: str = "azure",
    ):
        super().__init__(
            spark=spark,
            provider=provider,
            name="MSSQL",
            credential_manager=credential_manager,
        )
        self.server = server
        self.database = database
        self.driver_type = driver_type
        self.connection_url = self._initialize_jdbc_connection_url(connection_options)
        self.show_sql = show_sql

    def _initialize_jdbc_connection_url(self, connection_options: dict) -> str:
        if self.driver_type == "jdbc":
            try:
                credentials = self.credential_manager.get_credentials()
                if not credentials:
                    raise ValueError("No MSSQL credentials provided")

                # Start building the JDBC connection URL with credentials
                base_url = (
                    f"jdbc:sqlserver://{self.server};"
                    f"databaseName={self.database};"
                    f"user={credentials['username']};"
                    f"password={credentials['password']}"
                )

                # Append connection options to the URL
                options = ";".join([f"{k}={v}" for k, v in connection_options.items()])
                return f"{base_url};{options}" if options else base_url

            except ValueError as e:
                print(f"Error: {str(e)}")
                return None

        elif self.driver_type == "spark" or self.driver_type == "databricks":
            # Basic URL without credentials, with optional connection options
            base_url = f"jdbc:sqlserver://{self.server};databaseName={self.database}"
            options = ";".join([f"{k}={v}" for k, v in connection_options.items()])
            return f"{base_url};{options}" if options else base_url

        else:
            raise ValueError(
                "Invalid driver type. Supported driver types are: 'jdbc', 'spark', 'databricks'."
            )

    def check_database_exists(self, database: str) -> bool:
        """Check if the specified database exists in the SQL Server."""
        try:
            query = f"SELECT DB_ID('{database}') as DB_ID"
            df = self.execute_query(query)
            # If DB_ID returns a non-null value, the database exists
            return df.collect()[0][0] is not None
        except Exception as e:
            print(f"An error occurred while checking if the database exists: {str(e)}")
            return False

    def check_table_exists(self, database: str, schema: str, table: str) -> bool:
        """Check if the specified table exists in the given database and schema."""
        try:
            query = f"""
            SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            """
            df = self.execute_query(query)
            # If the query returns any rows, the table exists
            return df.count() > 0
        except Exception as e:
            print(f"An error occurred while checking if the table exists: {str(e)}")
            return False

    def truncate(self, database: str, schema: str, table: str):
        """Truncate the specified table in the given schema."""
        try:
            # Construct the TRUNCATE TABLE SQL statement
            truncate_query = f"TRUNCATE TABLE {schema}.{table}"

            # Execute the query
            credentials = self.credential_manager.get_credentials()
            conn = get_mssql_jdbc_connection(
                self.spark,
                self.connection_url,
                credentials["username"],
                credentials["password"],
            )
            stmt = conn.createStatement()
            stmt.execute(truncate_query)
            print(f"Successfully truncated table {schema}.{table}")

        except Exception as e:
            print(f"An error occurred while truncating the table: {str(e)}")
            raise e
        finally:
            if conn:
                conn.close()

    def list_tables(self):
        try:
            query = "SELECT * FROM INFORMATION_SCHEMA.TABLES"
            df = self.execute_query(query)
            return df
        except AnalysisException as e:
            print(f"An error occurred while listing tables: {str(e)}")
            raise e

    def execute_query(self, query: str) -> SparkDataFrame:
        try:
            if self.driver_type == "jdbc":
                df = (
                    self.spark.read.format("jdbc")
                    .option("url", self.connection_url)
                    .option("query", query)
                    .load()
                )
            elif self.driver_type == "spark":
                # TODO: driver needs to be installed on spark session creation
                credentials = self.credential_manager.get_credentials()
                connection_properties = {
                    "user": credentials["username"],
                    "password": credentials["password"],
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                }
                df = (
                    self.spark.read.format("jdbc")
                    .option("url", self.connection_url)
                    .option("query", query)
                    .options(**connection_properties)
                    .load()
                )
            elif self.driver_type == "databricks":
                credentials = self.credential_manager.get_credentials()
                connection_properties = {
                    "user": credentials["username"],
                    "password": credentials["password"],
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                }

                df = (
                    self.spark.read.format("jdbc")
                    .option("url", self.connection_url)
                    .option("query", query)
                    .options(**connection_properties)
                    .load()
                )

            return df
        except AnalysisException as e:
            print(f"An error occurred while executing query: {str(e)}")
            raise e

    def read(
        self,
        database: str,
        schema: str,
        table: str,
        columns: Optional[list[str]] = None,
        use_sql: bool = False,
    ) -> SparkDataFrame:
        """
        Reads data from the specified SQL Server table.

        :param database: The database name.
        :param schema: The schema name.
        :param table: The table name.
        :param columns: Optional list of columns to select from the table.
        :param use_sql: It has no impacts as MSSQL always uses SQL SELECT clause.
        :return: A SparkDataFrame containing the data from the specified table.
        """
        try:
            # Construct full table path
            table_path = f"{database}.{schema}.{table}"
            select_columns = ", ".join(columns) if columns else "*"
            query = f"SELECT {select_columns} FROM {table_path}"

            if self.show_sql:
                print_sql(query)

            # Read data into DataFrame using execute_query method
            df = self.execute_query(query)
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
            if self.driver_type == "jdbc":
                (
                    df.write.format("jdbc")
                    .option("url", self.connection_url)
                    .option("dbtable", f"{schema}.{table}")
                    .mode(mode)
                    .save()
                )
            elif self.driver_type == "spark":
                # TODO: driver needs to be installed on spark session creation
                credentials = self.credential_manager.get_credentials()
                connection_properties = {
                    "user": credentials["username"],
                    "password": credentials["password"],
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                }
                df.write.jdbc(
                    url=self.connection_url,
                    table=f"{schema}.{table}",
                    mode=mode,
                    properties=connection_properties,
                )
            elif self.driver_type == "databricks":
                credentials = self.credential_manager.get_credentials()
                connection_properties = {
                    "user": credentials["username"],
                    "password": credentials["password"],
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                }
                df.write.jdbc(
                    url=self.connection_url,
                    table=f"{schema}.{table}",
                    mode=mode,
                    properties=connection_properties,
                )
        except AnalysisException as e:
            print(f"An error occurred while writing data: {str(e)}")
            raise e

    def update(self, table: str, set_clause: str, condition: str):
        update_query = f"UPDATE {table} SET {set_clause} WHERE {condition}"
        credentials = self.credential_manager.get_credentials()
        try:
            if self.driver_type == "spark" or self.driver_type == "databricks":
                conn = get_mssql_jdbc_connection(
                    self.spark,
                    self.connection_url,
                    credentials["username"],
                    credentials["password"],
                )
                stmt = conn.createStatement()
                stmt.execute(update_query)

                print(f"Successfully updated rows in {table} where {condition}")
            else:
                raise NotImplementedError(
                    "Update is not supported for this driver type."
                )

        except Exception as e:
            print(f"An error occurred while updating data: {str(e)}")
            raise e
        finally:
            if conn:
                conn.close()

    def delete(self, table: str, condition: str):
        delete_query = f"DELETE FROM {table} WHERE {condition}"
        credentials = self.credential_manager.get_credentials()
        try:
            if self.driver_type == "spark" or self.driver_type == "databricks":
                conn = get_mssql_jdbc_connection(
                    self.spark,
                    self.connection_url,
                    credentials["username"],
                    credentials["password"],
                )
                stmt = conn.createStatement()
                stmt.execute(delete_query)
            else:
                raise NotImplementedError(
                    "Write is not supported for this driver type."
                )

        except Exception as e:
            print(f"An error occurred while deleting data: {str(e)}")
            raise e
        finally:
            if conn:
                conn.close()

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
        conn = None
        try:
            target_table = f"{database}.{schema}.{table}"
            if self.driver_type == "spark" or self.driver_type == "databricks":
                credentials = self.credential_manager.get_credentials()
                if source_df:
                    stg_table = f"stg_{table}"
                    source_table = f"{database}.{schema}.{stg_table}"
                    self.write(
                        df=source_df,
                        database=database,
                        schema=schema,
                        table=stg_table,
                        mode="overwrite",
                    )
                elif source_table:
                    pass
                else:
                    raise ValueError("Source table or DataFrame must be provided.")

                # Build the MERGE query dynamically based on the provided conditions
                merge_query = f"""
                MERGE INTO {target_table} AS target
                USING {source_table} AS source
                ON {merge_condition}
                WHEN MATCHED AND ({update_condition}) THEN
                UPDATE SET {", ".join([f"{target_col} = {source_col}" for target_col, source_col in update_set.items()])}
                WHEN NOT MATCHED BY TARGET THEN
                INSERT ({", ".join(insert_set.keys())}) 
                VALUES ({", ".join(insert_set.values())});
                """
                if self.show_sql:
                    print_sql(merge_query)

                # Create the JDBC connection and execute the merge query
                conn = get_mssql_jdbc_connection(
                    self.spark,
                    self.connection_url,
                    credentials["username"],
                    credentials["password"],
                )

                # Execute the merge query using Spark JDBC
                stmt = conn.createStatement()
                stmt.execute(merge_query)

                print(
                    f"Successfully merged data into {target_table} from the Spark DataFrame."
                )

            else:
                raise NotImplementedError(
                    "Merge is not supported for this driver type."
                )

        except Exception as e:
            print(f"An error occurred while merging data: {str(e)}")
            raise e

        finally:
            if conn:
                conn.close()

    def join(
        self,
        df: SparkDataFrame,
        lookup_table: str,
        join_type: str,
        join_conditions: list,
        lookup_columns: list,
    ) -> SparkDataFrame:
        try:
            query = f"SELECT * FROM {lookup_table}"
            lookup_df = self.execute_query(query)

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
            print(f"An error occurred while joining the tables: {str(e)}")
            raise e

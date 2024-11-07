from databricks.sdk.runtime import *  # type: ignore
from pyspark.sql import DataFrame as SparkDataFrame
from schemon_python_logger.logger import Logger


def get_secret_value(scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)  # type: ignore


def list_files(directory: str):
    return dbutils.fs.ls(directory)  # type: ignore


def get_all_widgets():
    return dbutils.widgets.getAll()  # type: ignore


def get_widget_value(name: str, default: str = None):
    widgets: dict = dbutils.widgets.getAll()  # type: ignore
    if name in widgets:
        return widgets.get(name, default)
    else:
        raise ValueError(f"Widget '{name}' not found in the notebook")


def foreach_batch_function(
    batch_df: SparkDataFrame,
    epoch_id: str,
    target_table: str,
):
    # Get distinct original file names from the batch
    file_names = batch_df.select("Source").distinct().rdd.flatMap(lambda x: x).collect()

    # Perform the write operation
    batch_df.write.format("delta").mode("append").saveAsTable(target_table)

    # Print a custom message after the batch is written, including the original file names
    num_records = batch_df.count()
    log_message = f"Batch {epoch_id} processed and written to {target_table} with {num_records} records from files: {', '.join(file_names)}"

    print(log_message)

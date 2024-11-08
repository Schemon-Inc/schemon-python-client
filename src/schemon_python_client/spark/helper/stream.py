from pyspark.sql import DataFrame as SparkDataFrame, functions as F
from schemon_python_expectation.spark.expectation.column_expectation import (
    ColumnExpectation,
)


def foreach_batch_function(
    batch_df: SparkDataFrame,
    epoch_id: str,
    target_table: str,
    bad_row_path: str = None,
    expectations: dict = None,
):
    valid_rows = None
    violations = None
    if expectations:
        # Generate validation checks and violation messages in parallel
        validation_checks = [
            F.when(
                ~getattr(ColumnExpectation, rule)(column),
                F.lit(f"Column {column} failed '{rule}'"),
            ).otherwise(F.lit(None))
            for column, rule in expectations.items()
        ]

        # Create a single DataFrame with a violation message column
        checked_df = batch_df.withColumn(
            "violation_message",
            F.to_json(
                F.struct(*[check for check in validation_checks if check is not None])
            ),
        )

        # Separate valid and invalid rows using a single transformation
        valid_rows = checked_df.filter(
            "violation_message IS NULL OR violation_message == ''"
        )
        violations = checked_df.filter(
            "violation_message IS NOT NULL AND violation_message <> ''"
        )
    else:
        valid_rows = batch_df

    # Log the batch processing results
    num_records = batch_df.count()
    num_valid = valid_rows.count() if valid_rows else 0
    num_violations = violations.count() if violations else 0

    # Write valid and violation rows as before
    if num_valid > 0:
        valid_rows = valid_rows.drop("violation_message")
        valid_rows.write.format("delta").mode("append").saveAsTable(target_table)
    if num_violations > 0:
        if bad_row_path:
            bad_row_path = f"{bad_row_path}/{target_table}"
            violations.write.format("parquet").mode("append").save(bad_row_path)
        else:
            print(
                f"Batch {epoch_id} | {num_violations} violating records found, but no bad_row_path specified."
            )

    log_message = f"Batch {epoch_id} | {num_records} records processed."

    if expectations:
        log_message += (
            f"\nBatch {epoch_id} | {num_valid} valid records written to {target_table}."
        )
        log_message += f"\nBatch {epoch_id} | {num_violations} violating records redirected to {bad_row_path}."

    print(log_message)

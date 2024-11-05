from pyspark.sql import SparkSession
from delta import *


def run_hive_query(spark: SparkSession = None, query: str = None, format="parquet"):
    try:
        if not query:
            raise ValueError("Query is required")
        if not spark:
            print("Creating a new Spark session")
            if format == "parquet":
                spark = (
                    SparkSession.builder.appName("HiveQueryRunner")
                    .enableHiveSupport()
                    .getOrCreate()
                )
            elif format == "delta":
                builder = (
                    SparkSession.builder.appName("HiveQueryRunnerForDelta")
                    .enableHiveSupport()
                    .config(
                        "spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension",
                    )
                    .config(
                        "spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    )
                )
                spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Run the query
        result_df = spark.sql(query)

        # print(spark.catalog.listColumns("stage.geomet_sieving_test"))
        # Return the DataFrame (optional)
        return result_df

    except Exception as e:
        print(f"Error running the query: {e}")
        return None


if __name__ == "__main__":
    # Example SQL query
    # query = "SHOW DATABASES"
    # query = "DESCRIBE extended stage.geomet_sieving_battery_of_test"
    # query = "SHOW CREATE TABLE stage.geomet_sieving_battery_of_test"
    query = "select *  from kaggle_bronze.kaggle_transfermarkt_game_lineups"
    # query = "select * from bronze.geomet_sieving_test"
    # query = """
    #         SELECT *
    #         FROM
    #         stage.geomet_sieving_test
    #         WHERE Type is not Null
    #         """

    # Run the query and get the result
    # run_hive_query(query=query, format="delta").show(truncate=False, vertical=True)
    run_hive_query(query=query).show(truncate= False, vertical=True)

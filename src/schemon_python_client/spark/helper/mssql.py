def create_mssql_jdbc_connection(spark, server, database, username, password):
    jdbc_url = f"jdbc:sqlserver://{server};databaseName={database}"

    try:
        conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            jdbc_url, username, password
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to SQL Server using spark mssql jdbc: {e}")
        return None


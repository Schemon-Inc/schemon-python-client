def get_mssql_jdbc_connection(spark, url, username, password):
    """
    Create a connection to SQL Server using spark mssql jdbc.
    IT is executing non-SELECT SQL commands directly on the database, such as INSERT, UPDATE, DELETE, or CREATE TABLE.
    Since this function does not return results, it is often used for commands that modify the database state.
    """
    try:
        conn = spark._sc._gateway.jvm.java.sql.DriverManager.getConnection(
            url, username, password
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to SQL Server using spark mssql jdbc: {e}")
        return None

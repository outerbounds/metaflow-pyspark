
def run(spark, db=None, table=None):

    spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{db}")

    spark.sql(f"""DROP TABLE glue_catalog.{db}.{table}""")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.{db}.{table} (
            id INT,
            name STRING,
            age INT
        )
        USING iceberg
        LOCATION 's3://iceberg-demo-warehouse/pyspark/{db}/{table}/'
    """)

    return spark.sql(f"SELECT * FROM glue_catalog.{db}.{table}")

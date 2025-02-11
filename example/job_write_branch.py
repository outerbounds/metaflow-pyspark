
def run(spark, db=None, table=None):

    spark.sql(f"""ALTER TABLE glue_catalog.{db}.{table} CREATE BRANCH staging""")

    spark.sql(f"""
        INSERT INTO glue_catalog.{db}.{table}.branch_staging
        VALUES (1, 'Alice', 30), (2, 'Bob', 35), (3, 'Charlie', 40)
    """)

    return spark.sql(f"SELECT * FROM glue_catalog.{db}.{table}.branch_staging")


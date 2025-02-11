
def run(spark, db=None, table=None):

    return spark.sql(f"SELECT * FROM glue_catalog.{db}.{table}.branch_staging")


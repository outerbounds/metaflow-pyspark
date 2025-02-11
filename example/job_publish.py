
def run(spark, db=None, table=None):
    spark.sql(f"CALL glue_catalog.system.fast_forward(table => 'glue_catalog.{db}.{table}', branch => 'main', to => 'staging')")
    return spark.sql(f"SELECT * FROM glue_catalog.{db}.{table}")


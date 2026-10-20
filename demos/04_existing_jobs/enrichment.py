"""The `customer-enrichment` job, as importable Python.

This is what the ported Databricks job looks like once it is a function: it takes a session
and its parameters, returns a DataFrame, and knows nothing about Metaflow. It runs
unchanged under `@spark`, under `pytest` with a local session, and in a notebook.

Column names below match `samples.bakehouse.sales_transactions`, the default table for
`ported_step.py`. Point `orders_table_name` at your own orders table instead and adjust the
column names accordingly.
"""

from pyspark.sql import functions as F


def enrich_customers(spark, run_date, orders_table_name):
    orders = spark.read.table(orders_table_name).where(
        F.to_date("dateTime") <= run_date
    )

    return (
        orders.groupBy("customerID")
        .agg(
            F.count("transactionID").alias("orders"),
            F.sum("totalPrice").alias("lifetime_value"),
            F.max(F.to_date("dateTime")).alias("last_order_date"),
        )
        .withColumn(
            "segment",
            F.when(F.col("lifetime_value") > 5000, "high")
            .when(F.col("lifetime_value") > 500, "mid")
            .otherwise("low"),
        )
        .withColumn("as_of", F.lit(run_date))
    )

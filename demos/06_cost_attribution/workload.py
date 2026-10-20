"""A workload with enough shuffle in it that the compute shape shows up in the timing."""

from pyspark.sql import functions as F


def summarize(spark, row_count=20_000_000):
    df = spark.range(row_count).select(
        (F.col("id") % 5000).alias("key"),
        (F.rand(42) * 1000).alias("value"),
    )
    return (
        df.groupBy("key")
        .agg(
            F.count("*").alias("rows"),
            F.avg("value").alias("avg_value"),
            F.stddev("value").alias("stddev_value"),
        )
        .orderBy(F.col("rows").desc())
        .limit(50)
    )

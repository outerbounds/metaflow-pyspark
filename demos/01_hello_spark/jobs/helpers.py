from pyspark.sql import functions as F


def bucketize(df, cutoff):
    return (
        df.withColumn("bucket", (F.col("value") / cutoff).cast("int"))
        .groupBy("bucket")
        .agg(F.count("*").alias("rows"), F.avg("value").alias("avg_value"))
        .orderBy("bucket")
    )

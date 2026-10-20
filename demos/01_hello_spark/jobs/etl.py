"""The Spark job submitted by photon_job.py.

Runs on the cluster, so it must not import metaflow or anything else that only exists in
the flow's environment. It may import its siblings, which is why the job lives in a
package: `build_package` ships the whole `jobs/` directory, not just this file.
"""

from .helpers import bucketize


def summarize(spark, cutoff_value=100.0):
    df = spark.range(5_000_000).selectExpr("id", "rand(42) * 1000 AS value")
    return bucketize(df, cutoff_value)

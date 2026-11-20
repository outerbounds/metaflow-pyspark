"""The Spark job `daily_job.py` submits.

This runs on the cluster, so it must not import metaflow. `_orders` is not part of this
package, which is why the step names it in `packages=`.
"""

from _orders import DAILY_SQL, source


def summarize(spark, table_name, version=None):
    return spark.sql(DAILY_SQL.format(source=source(table_name, version)))

# Chapter 6: the same summary as a Databricks job

```bash
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
python daily_job.py run
METAFLOW_DATABRICKS_RUNTIME_VERSION=15.4.x-scala2.12 python daily_job.py run
```

Chapters 1 to 5 keep the Spark driver in the task. This one sends the work to Databricks: `@spark(mode="job")` packages `jobs/daily.py` and `_orders.py`, stages them on the Volume, and submits `daily.summarize` through the Jobs API. The job computes `DAILY_SQL` over the orders version that `start` pinned and writes the result to `main.retail.orders_daily_job`. The step gets back a `UnityCatalogTable` pinned to that write, and `end` reads it through credential vending.

The first command runs on serverless job compute. The second creates a job cluster on a pinned runtime, which takes a few minutes to start and carries the step's pathspec as cluster tags in `system.billing.usage`.

Neither the task nor its environment needs `pyspark` or `databricks-connect`. The job module only imports `_orders`, which has no dependencies, and the cluster supplies Spark.

The Volume needs `READ VOLUME` and `WRITE VOLUME`. The driver script is staged in your workspace folder, `/Users/<you>/metaflow/`, because serverless compute can't run a Python file from a Volume. Both are cleaned up after the run.

A job that fails raises `SparkJobFailed` in `summarize`, with the run link and the Python traceback from the cluster.

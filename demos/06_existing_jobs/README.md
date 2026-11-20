# Chapter 6: a job that already exists in Databricks

```bash
python create_job.py
python trigger.py run
python trigger.py run --run-date 2023-01-01
```

Most teams already run pipelines as Databricks jobs. `@databricks_job` lets a flow trigger one without porting it: the job keeps its tasks, compute, and permissions, and the step passes it parameters and gets back what each task reported.

`create_job.py` stands in for a job someone else owns. It uploads the two notebooks in `notebooks/` to your workspace folder and creates `metaflow-pyspark-demo-daily-orders`, a serverless job with two tasks and job-level `run_date` and `target` parameters. `check` fails for dates before the orders table starts, and `summarize` writes the daily summary up to `run_date` to `main.retail.orders_daily_existing_job` and exits with the table name and the number of days.

`trigger.py` passes `run_date` from a flow parameter, prints each task's state from `self.databricks_job_result`, and reads the table back in `end` through a pinned `UnityCatalogTable`, which needs `deltalake` and the `EXTERNAL USE SCHEMA` grant as in chapter 2.

The second command shows a failure. `check` raises, `summarize` is skipped, and the step fails with `SparkJobFailed` carrying the `ValueError` from the notebook and a link to the run. Interrupting the flow while the job runs cancels it.

The identity running the flow needs `CAN MANAGE RUN` on the job, which the job's creator has.

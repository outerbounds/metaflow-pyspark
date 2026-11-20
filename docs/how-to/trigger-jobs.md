# Trigger Databricks jobs

`@databricks_job` starts a job that already exists in the Databricks workspace, waits for it, and gives the step what its tasks returned. The job keeps its own tasks, compute, libraries, schedule, and permissions, so a flow can orchestrate pipelines that stay in Databricks without porting any code.

```python
from metaflow import FlowSpec, Parameter, databricks_job, step


class NightlyFlow(FlowSpec):
    run_date = Parameter("run-date", default="2024-05-17")

    @step
    def start(self):
        self.next(self.enrich)

    @databricks_job(job_name="nightly-enrichment", parameters_from=["run_date"])
    @step
    def enrich(self):
        print(self.databricks_job_result)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    NightlyFlow()
```

The step needs the `[databricks]` extra and Databricks credentials, as described in [Use @spark](spark-decorator.md#credentials). The identity it runs as needs `CAN MANAGE RUN` on the job, and the run executes as the job's configured run-as identity.

## Name the job

`job_name=` looks the job up by its exact name, and fails if no job or more than one job has that name. `job_id=` skips the lookup, and is the safer choice when names aren't unique in the workspace.

## Pass parameters

The job receives job-level parameters, which work for every task type. `parameters={...}` passes literal values, and `parameters_from=[...]` names flow attributes to read when the step starts; a value from `parameters_from` wins over a literal of the same name. Strings are passed as they are and anything else is JSON-encoded, so `10` arrives as `"10"` and `["eu", "us"]` as `'["eu", "us"]'`.

The job has to declare each parameter under its job-level parameters. Databricks silently ignores a parameter the job doesn't declare, and would run the job with its default, so the step checks the names first and fails with the list the job does declare.

## Use the result

Before the step body runs, `self.databricks_job_result` holds one entry per task, keyed by `task_key`:

```python
{
    "check": {"state": "SUCCESS", "run_id": "93817953842692", "result": "ok"},
    "summarize": {
        "state": "SUCCESS",
        "run_id": "452411814187773",
        "result": '{"table": "main.retail.orders_daily", "days": 17}',
    },
}
```

- `state` is `SUCCESS`, `FAILED`, `CANCELLED`, or `SKIPPED` for a task that never ran because an upstream task failed. A task Databricks retried is reported by its last attempt.
- `result` is what a notebook task passed to `dbutils.notebook.exit()`, as a string. Databricks truncates long exit values, and the step logs a warning when it does.
- `error` is the error of a failed task.

`output_artifact=` renames the artifact. For anything bigger than a few values, have the job write a table and pass its name back as the exit value. The next step can then read it pinned, as [Read data](read-data.md) describes:

```python
@step
def end(self):
    written = json.loads(self.databricks_job_result["summarize"]["result"])
    daily = UnityCatalogTable(written["table"])
    print(daily.to_pandas().tail())
```

## Failures, timeouts, and cancellation

A failed run raises `SparkJobFailed` before the step body runs. The message has Databricks' reason for the failure, a link to the run, and the error and traceback of each task that failed. Tasks skipped because of it are left out:

```
Spark job failed with state 'FAILED'.
Error class: RUN_EXECUTION_ERROR
Message: Task check failed with message: Workload failed, see run output for details. This caused all downstream tasks to get skipped.
Run details: https://adb-....azuredatabricks.net/?o=...#job/.../run/...

Last 12 lines of output:
    [check]
    ValueError: run_date 2023-01-01 is before the orders table starts
    ...
```

With `crash_on_failure=False` the step body runs anyway, and `self.databricks_job_result` shows which tasks failed.

The step waits as long as the run takes, and the job's own timeout applies. Pass `timeout=` (minutes) to cancel the run sooner. Interrupting the step, or Metaflow killing the task, cancels the run. If the Jobs API stops answering while the run is in flight, the step raises `ControlPlaneError` and leaves the run alone, since it may still be healthy.

Runs are queued when the job is at its `max_concurrent_runs` limit; with `queue=False` Databricks skips the run instead, which fails the step. `idempotency_token=` makes Databricks return the existing run instead of starting a second one for the same token.

The printed output of each task (for Python script and wheel tasks; notebooks have none) is shown in the task log after the run; `show_output=False` turns that off. The job id, run id, and run link are recorded as task metadata:

```python
from metaflow import Task

Task("NightlyFlow/42/enrich/7").metadata_dict["databricks-run-url"]
```

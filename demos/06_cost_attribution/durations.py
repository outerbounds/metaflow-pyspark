"""Read a step's Databricks run timings back out of Metaflow metadata.

`@spark` records `spark-job-id`, `spark-job-url`, and `spark-ui-url` as task metadata, so a
finished run stays inspectable without keeping anything in flight. That is what makes the
cold-start comparison a measurement: the numbers come from the Jobs API, per run, after
the fact.
"""

from metaflow import Step


def databricks_run_id(flow_name, run_id, step_name):
    task = Step("%s/%s/%s" % (flow_name, run_id, step_name)).task
    return task.metadata_dict.get("spark-job-id")


def run_durations(flow_name, run_id, step_name):
    """Return (setup, execution, cleanup) seconds for a step's Databricks run."""
    from databricks.sdk import WorkspaceClient

    job_run_id = databricks_run_id(flow_name, run_id, step_name)
    if job_run_id is None:
        return (0.0, 0.0, 0.0)

    run = WorkspaceClient().jobs.get_run(int(job_run_id))
    # Single-task runs report durations on the task, and Databricks has moved these
    # between the run and the task across API versions, so take whichever is populated.
    sources = list(run.tasks or []) + [run]
    totals = []
    for field in ("setup_duration", "execution_duration", "cleanup_duration"):
        millis = next(
            (
                getattr(source, field)
                for source in sources
                if getattr(source, field, None)
            ),
            0,
        )
        totals.append(millis / 1000.0)
    return tuple(totals)

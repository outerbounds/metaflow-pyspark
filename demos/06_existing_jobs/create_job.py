"""Create the workspace job that `trigger.py` runs.

    python create_job.py

Uploads the two notebooks in `notebooks/` to your workspace folder and creates (or
replaces) a two-task job on serverless compute, with job-level `run_date` and `target`
parameters. In a real workspace this job would already exist, owned by whoever runs the
pipeline; this script only stands in for that.
"""

import base64
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

JOB_NAME = "metaflow-pyspark-demo-daily-orders"
TARGET = "main.retail.orders_daily_existing_job"
HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    w = WorkspaceClient()
    folder = "/Users/%s/metaflow-pyspark-demo" % w.current_user.me().user_name
    w.workspace.mkdirs(folder)
    for name in ("check", "summarize"):
        with open(os.path.join(HERE, "notebooks", name + ".py"), "rb") as handle:
            content = base64.b64encode(handle.read()).decode()
        w.workspace.import_(
            "%s/%s" % (folder, name),
            content=content,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )

    settings = {
        "name": JOB_NAME,
        "parameters": [
            {"name": "run_date", "default": "2024-05-17"},
            {"name": "target", "default": TARGET},
        ],
        "tasks": [
            {
                "task_key": "check",
                "notebook_task": {"notebook_path": folder + "/check"},
            },
            {
                "task_key": "summarize",
                "depends_on": [{"task_key": "check"}],
                "notebook_task": {"notebook_path": folder + "/summarize"},
            },
        ],
        "max_concurrent_runs": 5,
    }
    existing = w.api_client.do("GET", "/api/2.2/jobs/list", query={"name": JOB_NAME})
    for job in existing.get("jobs") or []:
        w.api_client.do("POST", "/api/2.2/jobs/delete", body={"job_id": job["job_id"]})
    job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]
    print("created job %s (%s)" % (JOB_NAME, job_id))


if __name__ == "__main__":
    main()

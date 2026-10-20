"""EMR Serverless, ported onto the shared backend abstraction.

Behaviourally compatible with the original ``@pyspark`` decorator, with the polling,
cancellation, and error-classification bugs fixed by virtue of that logic now living in
the base class. The two original TODOs are also closed: jobs are no longer limited to a
single self-contained module.
"""

import json
import posixpath

from . import JobBackend
from .. import remote_driver
from ..config import require
from ..context import JobHandle, JobState, JobStatus
from ..exceptions import SparkBackendUnavailable, SparkException
from ..packaging import build_package, pickle_inputs, python_version

APPLICATION_HINT = (
    "Set it with @spark(application_id='...'), the 'application-id' key of the config "
    "artifact, or METAFLOW_SPARK_EMR_APPLICATION_ID."
)
ROLE_HINT = (
    "Set it with @spark(execution_role='arn:aws:iam::...'), the 'execution-role' key of "
    "the config artifact, or METAFLOW_SPARK_EMR_EXECUTION_ROLE."
)

_STATE_MAP = {
    "SUBMITTED": JobState.PENDING,
    "PENDING": JobState.PENDING,
    "SCHEDULED": JobState.PENDING,
    "QUEUED": JobState.PENDING,
    "RUNNING": JobState.RUNNING,
    "SUCCESS": JobState.SUCCESS,
    "FAILED": JobState.FAILED,
    "CANCELLING": JobState.RUNNING,
    "CANCELLED": JobState.CANCELLED,
}


class EMRServerlessBackend(JobBackend):
    name = "emr-serverless"

    def __init__(self, config, ctx=None):
        super().__init__(config, ctx)
        self._client = None

    @property
    def client(self):
        if self._client is None:
            try:
                import boto3
            except ImportError:
                raise SparkBackendUnavailable("emr-serverless", "boto3", extra="emr")
            self._client = boto3.client("emr-serverless")
        return self._client

    # ------------------------------------------------------------------
    def submit(self, ctx):
        from metaflow import S3

        config = self.config
        if ctx.job_func is None:
            raise SparkException(
                "The emr-serverless backend needs a job function: "
                "@spark(backend='emr-serverless', job=mymodule.run)."
            )
        app_id = require(config, "application-id", self.name, APPLICATION_HINT)
        role = require(config, "execution-role", self.name, ROLE_HINT)

        package = build_package(
            ctx.job_func,
            include=config.get("include"),
            extra_modules=config.get("packages"),
        )
        for warning in package.warnings:
            ctx.log(warning, stream="stderr")

        s3args = {}
        if config.get("s3-prefix"):
            s3args["s3root"] = config["s3-prefix"]
        else:
            s3args["run"] = ctx.flow

        prefix = "metaflow_spark/%s" % ctx.pathspec
        with S3(**s3args) as s3:
            ((_, driver_url),) = s3.put_files(
                [("%s/remote_driver.py" % prefix, remote_driver.__file__)]
            )
            package_url = s3.put(
                "%s/code.tar.gz" % prefix, package.blob, content_type="application/gzip"
            )
            inputs_url = s3.put("%s/inputs.pickle" % prefix, pickle_inputs(ctx.inputs))

        s3root = posixpath.dirname(driver_url)
        logs_url = posixpath.join(s3root, "logs")
        out_url = config.get("output_path") or posixpath.join(s3root, "out")
        result_url = posixpath.join(s3root, "result.json")

        params = config.get("spark-parameters") or {}
        params_str = " ".join("--conf %s=%s" % (k, v) for k, v in params.items())

        driver_conf = {
            "package_path": package_url,
            "inputs_path": inputs_url,
            "module_name": package.module_name,
            "func_name": ctx.job_func.__name__,
            "output_path": out_url,
            "output_table": None,
            "write_mode": config.get("write_mode", "overwrite"),
            "partition_by": config.get("partition_by"),
            "result_path": result_url,
            "expected_python": python_version(),
            "spark_parameters": {},
            "app_name": ctx.job_name,
        }

        request = {
            "applicationId": app_id,
            "executionRoleArn": role,
            "jobDriver": {
                "sparkSubmit": {
                    "entryPoint": driver_url,
                    "entryPointArguments": [json.dumps(driver_conf)],
                    "sparkSubmitParameters": params_str,
                }
            },
            "configurationOverrides": {
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": logs_url}
                }
            },
            "name": ctx.job_name[:255],
        }
        if ctx.timeout_minutes:
            request["executionTimeoutMinutes"] = int(ctx.timeout_minutes)
        if ctx.tags:
            # EMR tag keys and values are less restrictive than Databricks', but the
            # same normalized tags are used so cost queries look the same either way.
            request["tags"] = dict(ctx.tags)

        response = self.client.start_job_run(**request)
        job_id = response["jobRunId"]
        return JobHandle(
            backend=self.name,
            job_id=job_id,
            ui_url=None,
            output_url=out_url,
            log_url=logs_url,
            extra={
                "application_id": app_id,
                "result_path": result_url,
                "prefix": prefix,
                "s3root": s3root,
                "s3args": s3args,
            },
        )

    def poll(self, handle):
        response = self.client.get_job_run(
            applicationId=handle.extra["application_id"], jobRunId=handle.job_id
        )
        run = response["jobRun"]
        raw_state = run.get("state")
        return JobStatus(
            state=_STATE_MAP.get(raw_state, JobState.PENDING),
            message=run.get("stateDetails"),
            error_class=raw_state,
            ui_url=None,
        )

    def cancel(self, handle):
        self.client.cancel_job_run(
            applicationId=handle.extra["application_id"], jobRunId=handle.job_id
        )

    def fetch_logs(self, handle, stream="stdout"):
        import gzip

        from metaflow import S3

        parts = []
        path = (
            posixpath.join(
                handle.log_url,
                "applications",
                handle.extra["application_id"],
                "jobs",
                handle.job_id,
                "SPARK_DRIVER",
                stream,
            )
            + ".gz"
        )
        with S3() as s3:
            obj = s3.get(path, return_missing=True)
            if obj.exists:
                with gzip.open(obj.path, "rt", errors="replace") as handle_:
                    parts.append(handle_.read())

            marker = s3.get(handle.extra["result_path"], return_missing=True)
            if marker.exists:
                try:
                    with open(marker.path) as handle_:
                        result = json.load(handle_)
                except (OSError, ValueError):
                    result = {}
                if result.get("error"):
                    parts.insert(0, result["error"])

        return "\n".join(p for p in parts if p) or None

    def read_output(self, handle, output_format):
        from ..output import from_storage

        return from_storage(handle.output_url, output_format)

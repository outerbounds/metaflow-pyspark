import traceback
import functools
import inspect
import pickle
import gzip
import json
import time
import sys
import os

from metaflow import S3

DEFAULT_TIMEOUT = 60
NUM_RETRIES = 7
STATUS_MESSAGE_INTERVAL = 60


def log(msg, job_id=None, stream="stdout"):
    prefix = "@pyspark"
    if job_id:
        prefix += f"[job {job_id}]"
    print(f"{prefix}: {msg}", file=getattr(sys, stream))


def wait_job(app_id, job_id):
    import boto3

    client = boto3.client("emr-serverless")

    last_status = 0
    while True:

        response = None
        for i in range(NUM_RETRIES):
            try:
                response = client.get_job_run(applicationId=app_id, jobRunId=job_id)
                break
            except:
                if i == NUM_RETRIES - 1:
                    msg = "querying job status failed:\n" + traceback.format_exc()
                    log(msg, job_id=job_id, stream="stderr")
                    return False
                else:
                    time.sleep(2**i)
                    try:
                        client = boto3.client("emr-serverless")
                    except:
                        pass

        state = response["jobRun"]["state"]
        if state == "SUCCESS":
            log("Job finished successfully", job_id=job_id)
            return True
        elif state == "FAILED":
            log("Job failed", job_id=job_id)
            return False
        elif state == "CANCELLED":
            log("Job cancelled", job_id=job_id)
            return False

        now = time.time()
        if now - last_status > STATUS_MESSAGE_INTERVAL:
            log(f"Job status: {state}", job_id=job_id)
            last_status = now
        time.sleep(5)


def execute_job(
    driver_url=None,
    driver_conf=None,
    params_str=None,
    logs_url=None,
    config=None,
    timeout=None,
):

    from metaflow import current
    import boto3

    client = boto3.client("emr-serverless")
    app_id = config["application-id"]
    resp = client.start_job_run(
        applicationId=app_id,
        executionRoleArn=config["execution-role"],
        jobDriver={
            "sparkSubmit": {
                "entryPoint": driver_url,
                "entryPointArguments": [driver_conf],
                "sparkSubmitParameters": params_str,
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": logs_url}
            }
        },
        executionTimeoutMinutes=timeout,
        name="metaflow_pyspark_" + current.pathspec.replace("/", "_"),
    )
    job_id = resp["jobRunId"]
    log("Spark job submitted", job_id=job_id)
    return job_id, app_id, wait_job(app_id, job_id)


def run_spark(
    pyspark_func,
    s3client,
    timeout=None,
    config=None,
    inputs=None,
    show_stdout=None,
    show_stderr=None,
):

    from metaflow import current
    from . import pyspark_driver

    pathspec = current.pathspec
    prefix = f"metaflow_pyspark/{pathspec}"

    # 1) resolve entrypoint to the user's pyspark code
    # TODO: use Metaflow job package to support multi-file pyspark jobs
    # TODO: support importing of Python packages
    module = inspect.getmodule(pyspark_func)
    module_name = module.__name__
    func_name = pyspark_func.__name__

    # 2) upload job files
    files = [
        (f"{prefix}/driver.py", pyspark_driver.__file__),
        (f"{prefix}/mfjob.py", module.__file__),
    ]
    [(_, driver_url), (_, job_url)] = s3client.put_files(files)

    # 3) upload input artifacts
    pickled = pickle.dumps(inputs, protocol=4)
    inputs_url = s3client.put(f"{prefix}/inputs", pickled)

    # 4) construct results paths
    s3root = os.path.dirname(driver_url)
    logs_url = os.path.join(s3root, "logs")
    out_url = os.path.join(s3root, "out")

    # 5) construct sparkSubmitParameters
    params = config.get("spark-parameters", {})
    params_str = " ".join("--conf %s=%s" % kv for kv in params.items())

    # 6) launch a Spark job
    driver_conf = json.dumps(
        {
            "job_url": job_url,
            "input_url": inputs_url,
            "func_name": func_name,
            "output_url": out_url,
        }
    )
    job_id, app_id, success = execute_job(
        driver_url=driver_url,
        driver_conf=driver_conf,
        params_str=params_str,
        logs_url=logs_url,
        config=config,
        timeout=timeout,
    )
    if show_stdout:
        show_logs(s3client, prefix, app_id, job_id, "stdout")
    if show_stderr:
        show_logs(s3client, prefix, app_id, job_id, "stderr")

    return success, out_url


def show_logs(s3client, prefix, app_id, job_id, stream):
    log_path = (
        os.path.join(
            prefix,
            "logs",
            "applications",
            app_id,
            "jobs",
            job_id,
            "SPARK_DRIVER",
            stream,
        )
        + ".gz"
    )
    log_obj = s3client.get(log_path, return_missing=True)
    if log_obj.exists:
        with gzip.open(log_obj.path, "rt") as f:
            txt = f.read()
        log(f"job {stream}\n-----\n{txt}\n-----\n", job_id=job_id)
    else:
        log(f"job produced no output on {stream}", job_id=job_id)


def load_table(url):
    from pyarrow.parquet import ParquetDataset

    with S3() as s3:
        files = s3.get_recursive([url])
        parqs = [f for f in files if f.url.endswith(".parquet")]
        total = sum(p.size for p in parqs)
        log("Downloaded %.1fMB of compressed outputs" % (total / 1024**2))
        return ParquetDataset([p.path for p in parqs]).read()


def pyspark_wrapper(
    flow,
    step_func,
    job=None,
    job_parameters=None,
    output_artifact="pyspark_df",
    output_pyarrow=False,
    output_pandas=True,
    save_output=True,
    show_stdout=True,
    show_stderr=False,
    crash_on_failure=True,
    spark_config="spark_config",
    user_timeout=None,
):
    @functools.wraps(step_func)
    def pyspark_step():

        # fail fast if pyarrow is not available but it is needed
        if output_pandas or output_pyarrow:
            try:
                import pyarrow
            except:
                raise Exception(
                    "@pyspark: Install 'pyarrow' or use @conda "
                    "to enable output_pandas and output_pyarrow."
                )

        config = getattr(flow, spark_config, None)
        if config is None:
            raise Exception(
                f"@pyspark: Specify the Spark configuration "
                "in the '{spark_config}' artifact."
            )
        else:
            if type(config) != dict:
                try:
                    config = json.loads(config)
                except:
                    raise Exception(f"@pyspark: Invalid JSON in " "'{spark_config}'.")

        inputs = {}
        for inp in job_parameters if job_parameters else []:
            if hasattr(flow, inp):
                inputs[inp] = getattr(flow, inp)
            else:
                raise Exception(f"@pyspark: Input artifact '{input}' " "missing.")

        if user_timeout is None:
            timeout = config.get("timeout", DEFAULT_TIMEOUT)
        else:
            timeout = user_timeout

        s3args = {}
        if "s3-prefix" in config:
            s3args["s3root"] = config["s3-prefix"]
        else:
            s3args["run"] = flow

        with S3(**s3args) as s3:

            success, url = run_spark(
                job,
                s3,
                timeout=timeout,
                config=config,
                inputs=inputs,
                show_stdout=show_stdout,
                show_stderr=show_stderr,
            )

            if crash_on_failure and not success:
                raise Exception("@pyspark job failed")

            if output_artifact:
                if not success:
                    setattr(flow, output_artifact, None)
                elif output_pyarrow or output_pandas:
                    table = load_table(url)
                    art = table.to_pandas() if output_pandas else table
                    setattr(flow, output_artifact, art)
                else:
                    setattr(flow, output_artifact, url)

        step_func()

        if output_artifact and not save_output:
            delattr(flow, output_artifact)

    return pyspark_step

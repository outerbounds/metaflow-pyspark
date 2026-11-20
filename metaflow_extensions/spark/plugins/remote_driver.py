"""Entry point executed inside a remote Spark driver.

Standard library only, and deliberately so: it runs before the job's own environment is
known to be importable, and its most important job is to report a comprehensible error
when that environment turns out to be wrong.

Staged paths are Unity Catalog Volume paths, which are POSIX paths on a Databricks
cluster.

Invoked as::

    remote_driver.py '<json config>'
"""

import json
import os
import sys
import tarfile
import tempfile
import traceback


def _log(msg):
    print("@spark[driver]: %s" % msg, flush=True)


def _require_readable(path):
    if not os.path.exists(path):
        raise RuntimeError(
            "Cannot read %s. Code must be staged somewhere the cluster can also read, "
            "such as a Unity Catalog Volume." % path
        )
    return path


def _extract_package(package_path, dest):
    if not package_path:
        return None
    local = _require_readable(package_path)
    os.makedirs(dest, exist_ok=True)
    with tarfile.open(local, "r:gz") as tar:
        # filter="data" rejects absolute paths and traversal. Required on Python 3.14,
        # unsupported before 3.12, hence the fallback.
        try:
            tar.extractall(dest, filter="data")
        except TypeError:
            tar.extractall(dest)
    sys.path.insert(0, dest)
    _log("code package extracted to %s" % dest)
    return dest


def _load_inputs(inputs_path, expected_python):
    if not inputs_path:
        return {}
    import pickle

    local = _require_readable(inputs_path)
    with open(local, "rb") as handle:
        payload = handle.read()
    try:
        return pickle.loads(payload)
    except Exception as exc:
        running = "%d.%d" % sys.version_info[:2]
        if expected_python and not expected_python.startswith(running):
            hint = (
                "\nThe flow ran on Python %s and this cluster runs Python %s. "
                "job_parameters are pickled, so both sides need compatible Python and "
                "library versions. Align the versions, pass plain built-in types, or "
                "use mode='connect', where nothing is pickled."
                % (expected_python, running)
            )
        else:
            hint = (
                "\njob_parameters are pickled, so every class they reference must be "
                "importable on the cluster at a compatible version. Pass plain built-in "
                "types, or use mode='connect', where nothing is pickled."
            )
        raise RuntimeError("Could not deserialize job_parameters: %s%s" % (exc, hint))


def _import_job(module_name, package_dir):
    try:
        return __import__(module_name, fromlist=["*"])
    except ImportError as exc:
        contents = []
        if package_dir and os.path.isdir(package_dir):
            contents = sorted(os.listdir(package_dir))[:40]
        raise RuntimeError(
            "Could not import the job module '%s': %s\n"
            "The code package contains: %s\n"
            "If the job imports local modules, add their root with "
            "@spark(include=[...])." % (module_name, exc, ", ".join(contents))
        )


def _write_output(df, output_path, output_table, write_mode, partition_by):
    if df is None:
        _log("job returned no DataFrame; nothing written")
        return {"wrote": None}

    writer = df.write.mode(write_mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if output_table:
        writer.saveAsTable(output_table)
        _log("wrote result to table %s" % output_table)
        return {"wrote": "table", "table": output_table}

    writer.parquet(output_path)
    _log("wrote result to %s" % output_path)
    return {"wrote": "parquet", "path": output_path}


def start_job(
    package_path=None,
    inputs_path=None,
    module_name=None,
    func_name=None,
    output_path=None,
    output_table=None,
    write_mode="overwrite",
    partition_by=None,
    result_path=None,
    expected_python=None,
    spark_parameters=None,
    app_name="metaflow",
):
    workdir = tempfile.mkdtemp(prefix="metaflow-spark-")
    package_dir = _extract_package(package_path, os.path.join(workdir, "code"))
    inputs = _load_inputs(inputs_path, expected_python)

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    for key, value in (spark_parameters or {}).items():
        builder = builder.config(key, str(value))
    spark = builder.getOrCreate()
    _log("Spark %s session ready" % spark.version)

    module = _import_job(module_name, package_dir)
    func = getattr(module, func_name or "", None)
    if func is None:
        available = [n for n in dir(module) if not n.startswith("_")][:40]
        raise RuntimeError(
            "Module '%s' has no attribute '%s'. It defines: %s"
            % (module_name, func_name, ", ".join(available))
        )

    df = func(spark, **inputs)
    return _write_output(df, output_path, output_table, write_mode, partition_by)


def _publish_result(result_path, result):
    """Write the result marker so the submitting side can read the real traceback."""
    if not result_path:
        return
    payload = json.dumps(result).encode("utf-8")
    try:
        parent = os.path.dirname(result_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(result_path, "wb") as handle:
            handle.write(payload)
    except Exception as exc:
        _log("could not write the result marker: %s" % exc)


def main():
    conf = json.loads(sys.argv[1])
    result_path = conf.pop("result_path", None)
    try:
        result = start_job(result_path=result_path, **conf)
        result["ok"] = True
    except Exception:
        error = traceback.format_exc()
        _log("job failed:\n%s" % error)
        # The marker is what the step shows, where the Python traceback and Spark's
        # message matter and a hundred JVM frames would bury them.
        result = {"ok": False, "error": error.split("\n\nJVM stacktrace:", 1)[0]}

    _publish_result(result_path, result)
    if not result["ok"]:
        sys.exit(1)


if __name__ == "__main__":
    main()

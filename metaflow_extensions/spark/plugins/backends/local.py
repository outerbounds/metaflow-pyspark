"""In-process Spark, so that a flow can be developed and tested without a cloud account.

This exists as much for CI as for laptops: it lets the demos and the test suite exercise
the whole @spark code path with no credentials, which is what makes the rest of the
backends safe to refactor.
"""

import os
import shutil
import sys
import tempfile
from contextlib import contextmanager

from . import SessionBackend
from ..exceptions import SparkBackendUnavailable


def _ensure_java_home(prefix=None):
    """Point JAVA_HOME at a JDK installed into the running Python's environment.

    conda's openjdk sets JAVA_HOME from an activation script, which a baked image that
    runs the environment's python directly never sources. The environment's JDK wins
    over a `java` on PATH, which on macOS is often only a stub that asks to install one.
    """
    if os.environ.get("JAVA_HOME"):
        return
    prefix = prefix or sys.prefix
    for home in (os.path.join(prefix, "lib", "jvm"), prefix):
        if os.path.exists(os.path.join(home, "bin", "java")):
            os.environ["JAVA_HOME"] = home
            return


class LocalSparkBackend(SessionBackend):
    name = "local"

    @contextmanager
    def session(self, ctx):
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            raise SparkBackendUnavailable("local", "pyspark", extra="local")

        _ensure_java_home()
        config = self.config
        master = config.get("master", "local[*]")
        params = dict(config.get("spark-parameters") or {})

        tmpdir = None
        if "spark.sql.warehouse.dir" not in params:
            tmpdir = tempfile.mkdtemp(prefix="metaflow-spark-")
            params["spark.sql.warehouse.dir"] = tmpdir

        app_name = "metaflow-%s" % ctx.pathspec.replace("/", "-")
        builder = SparkSession.builder.master(master).appName(app_name)
        for key, value in params.items():
            builder = builder.config(key, str(value))

        session = builder.getOrCreate()
        ctx.log(
            "local Spark session ready (master=%s, version=%s)"
            % (master, session.version)
        )
        try:
            yield session
        finally:
            # Only stop a session we are responsible for. getOrCreate may have handed
            # back one the user created themselves, and stopping that would surprise.
            if config.get("stop_session", True):
                try:
                    session.stop()
                except Exception:
                    pass
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)

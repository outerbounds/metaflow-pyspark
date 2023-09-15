import json
import os
from metaflow import FlowSpec, step, IncludeFile, Parameter
from metaflow import pyspark, pypi_base

import myjob

@pypi_base(
    packages={"pydash": "5.1.1", "pyarrow": "8.0.0"},
)
class PySparkTestFlow(FlowSpec):

    param = Parameter("param", default=123)
    spark_config = IncludeFile("spark_config", default="spark_config.json")

    def update_spark_config(self, config):
        cfg = json.loads(config)
        metaflow_python = "/home/hadoop/metaflow-python.sh"
        spark_params = cfg["spark-parameters"]
        spark_params["spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON"] = metaflow_python
        spark_params["spark.emr-serverless.driverEnv.PYSPARK_PYTHON"] = metaflow_python
        spark_params["spark.executorEnv.PYSPARK_PYTHON"] = metaflow_python

        for field in {"spark.executorEnv", "spark.emr-serverless.driverEnv"}:
            for env_var in {"_METAFLOW_CONDA_ENV", "METAFLOW_CODE_URL", "METAFLOW_DATASTORE_SYSROOT_S3"}:
                key = f"{field}.{env_var}"
                value = os.environ[env_var]
                if env_var == "_METAFLOW_CONDA_ENV":
                    spark_params[key] = f"'{value}'"
                else:
                    spark_params[key] = value

        return cfg

    @step
    def start(self):
        self.my_spark_config = self.update_spark_config(self.spark_config)
        self.next(self.run_pyspark)

    @pyspark(
        job=myjob.run,
        job_parameters=["param"],
        output_artifact="spark_df",
        spark_config="my_spark_config",
        output_pandas=True,
    )
    @step
    def run_pyspark(self):
        print('PySpark job produced %d rows' % len(self.spark_df))
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    PySparkTestFlow()



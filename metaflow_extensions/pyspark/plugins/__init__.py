from functools import partial
from metaflow.decorators import StepDecorator


class PySparkDecorator(StepDecorator):
    name = "pyspark"
    defaults = {
        "job": None,
        "job_parameters": None,
        "output_artifact": "pyspark_df",
        "output_pyarrow": False,
        "output_pandas": True,
        "save_output": True,
        "show_stdout": True,
        "show_stderr": False,
        "crash_on_failure": True,
        "spark_config": "spark_config",
        "user_timeout": None,
    }

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        from .metaflow_pyspark import pyspark_wrapper

        return pyspark_wrapper(flow, step_func, **self.attributes)


STEP_DECORATORS = [PySparkDecorator]

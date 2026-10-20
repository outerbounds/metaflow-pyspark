"""Metaflow extension for Spark and governed Databricks data.

`@spark` gives a step a live Spark session, locally or on Databricks through Spark
Connect, or submits a packaged job to Databricks or EMR Serverless. `@pyspark` is the
original EMR Serverless decorator, now an alias of `@spark`. `@databricks_job` and
`@databricks_notebook` run work that already exists in a workspace. `UnityCatalogTable`
and `query()` are plain objects and functions usable from any step. Nothing here imports pyspark,
boto3, databricks-sdk, deltalake, or pyarrow until a step actually uses them.
"""

# Importing a submodule directly (`from metaflow_extensions.spark.plugins.warehouse
# import query`) before metaflow would otherwise start metaflow's initialization halfway
# through config.py, which then loads this extension's toplevel module and finds
# config.py half-defined. When metaflow itself is the importer, this is a no-op.
import metaflow  # noqa: F401

STEP_DECORATORS_DESC = [
    ("spark", ".decorator.SparkDecorator"),
    ("pyspark", ".decorator.PySparkDecorator"),
    ("databricks_job", ".databricks_decorators.DatabricksJobDecorator"),
    ("databricks_notebook", ".databricks_decorators.DatabricksNotebookDecorator"),
]

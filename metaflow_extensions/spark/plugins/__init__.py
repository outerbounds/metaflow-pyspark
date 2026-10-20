"""Metaflow extension for Spark and governed Databricks data.

`@spark` gives a step a live Spark session, locally or on Databricks through Spark
Connect. `@pyspark` submits a job to EMR Serverless. `UnityCatalogTable` and `query()`
are plain objects and functions usable from any step. Nothing here imports pyspark,
boto3, databricks-sdk, deltalake, or pyarrow until a step actually uses them.
"""

# Importing a submodule directly (`from metaflow_extensions.spark.plugins.warehouse
# import query`) before metaflow would otherwise start metaflow's initialization halfway
# through config.py, which then loads this extension's toplevel module and finds
# config.py half-defined. When metaflow itself is the importer, this is a no-op.
import metaflow  # noqa: F401

STEP_DECORATORS_DESC = [
    ("spark", ".decorator.SparkDecorator"),
    ("pyspark", ".pyspark_decorator.PySparkDecorator"),
]

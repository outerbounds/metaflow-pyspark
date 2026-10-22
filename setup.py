from setuptools import setup, find_namespace_packages

version = "0.0.3"

setup(
    name="metaflow_pyspark",
    version=version,
    description="An EXPERIMENTAL PySpark decorator for Metaflow",
    author="Philipp Rudiger",
    author_email="prudiger@anaconda.com",
    packages=find_namespace_packages(include=["metaflow_extensions.*"]),
    py_modules=[
        "metaflow_extensions",
    ],
    extras_require={
        # query_tags on the Statement Execution API first appear in 0.86.
        "databricks": ["databricks-sdk>=0.86", "pyarrow"],
        "catalog": ["databricks-sdk>=0.86", "deltalake>=0.18", "pyarrow"],
        "local": ["pyspark>=3.5"],
        # Bundles its own pyspark, so it cannot share an environment with "local".
        "connect": ["databricks-sdk>=0.86", "databricks-connect>=15.4"],
    },
)

from setuptools import setup, find_namespace_packages

version = "0.0.1"

setup(
    name="metaflow-pyspark",
    version=version,
    description="An EXPERIMENTAL PySpark decorator for Metaflow",
    author="Ville Tuulos",
    author_email="ville@outerbounds.co",
    packages=find_namespace_packages(include=["metaflow_extensions.*"]),
    py_modules=[
        "metaflow_extensions",
    ],
    install_requires=[
         "metaflow"
    ]
)

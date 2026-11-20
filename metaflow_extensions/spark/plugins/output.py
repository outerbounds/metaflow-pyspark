"""Turning a Spark or Arrow result into the format a step asked for.

A submitted job can also hand the next step a reference instead of the data:
``output_format="table"`` returns a ``UnityCatalogTable`` for ``output_table``, and
``"url"`` returns where the job wrote its output.
"""

from .exceptions import SparkConfigError, SparkException

#: Formats that materialize data into the task process.
MATERIALIZING = ("pandas", "arrow", "polars")

#: Formats that return a reference to a submitted job's output.
REFERENCES = ("url", "table")

FORMATS = MATERIALIZING + REFERENCES + ("none",)


def validate_format(output_format, session=False):
    valid = MATERIALIZING + ("none",) if session else FORMATS
    if output_format not in valid:
        if session and output_format in REFERENCES:
            raise SparkConfigError(
                "@spark(output_format=%r) needs a submitted job, which writes its output "
                "to storage. A session step can write a table itself and pass a "
                "UnityCatalogTable." % output_format
            )
        raise SparkConfigError(
            "Unknown @spark(output_format=%r). Choose one of: %s."
            % (output_format, ", ".join(valid))
        )
    return output_format


def _require(module, output_format):
    try:
        return __import__(module)
    except ImportError:
        raise SparkException(
            "output_format=%r requires '%s'. Install it, or add it to the step with "
            "@pypi(packages={'%s': ''})." % (output_format, module, module)
        )


def _references_pyspark(value):
    if isinstance(value, (list, tuple)):
        return any(_references_pyspark(item) for item in value)
    return type(value).__module__.startswith("pyspark")


def strip_spark_attrs(value):
    """Drop pandas `attrs` entries that hold pyspark objects, in place.

    Spark Connect's toPandas() stores query metrics in `DataFrame.attrs` as pyspark
    objects. Left in, unpickling the artifact needs pyspark in every step that reads it,
    including steps whose environment has no Spark at all.
    """
    if not type(value).__module__.startswith("pandas"):
        return value
    attrs = getattr(value, "attrs", None)
    if attrs:
        for key in [k for k, v in attrs.items() if _references_pyspark(v)]:
            del attrs[key]
    return value


def from_spark_dataframe(df, output_format):
    """Materialize a live Spark DataFrame according to `output_format`."""
    validate_format(output_format, session=True)
    if df is None or output_format == "none":
        return None
    if output_format == "pandas":
        _require("pandas", output_format)
        return strip_spark_attrs(df.toPandas())
    arrow = _spark_to_arrow(df)
    return from_arrow_table(arrow, output_format)


def _spark_to_arrow(df):
    """Collect a Spark DataFrame as an Arrow table without going through pandas."""
    _require("pyarrow", "arrow")
    # Spark Connect and classic Spark both expose the private Arrow collector, and it
    # avoids a pandas round trip that would otherwise mangle types.
    for attr in ("_collect_as_arrow", "toArrow"):
        collect = getattr(df, attr, None)
        if collect is None:
            continue
        result = collect()
        if hasattr(result, "num_rows"):
            return result
        import pyarrow as pa

        return pa.Table.from_batches(result)
    import pyarrow as pa

    return pa.Table.from_pandas(df.toPandas(), preserve_index=False)


def from_arrow_table(table, output_format):
    """Convert an already-collected Arrow table to `pandas`, `arrow`, or `polars`."""
    if output_format == "arrow":
        return table
    if output_format == "pandas":
        _require("pandas", output_format)
        return table.to_pandas()
    if output_format == "polars":
        polars = _require("polars", output_format)
        return polars.from_arrow(table)
    return table


def from_storage(url, output_format, filesystem=None):
    """Materialize a remote job's Parquet output according to `output_format`."""
    validate_format(output_format)
    if output_format == "none":
        return None
    if output_format in REFERENCES:
        return url
    if url is None:
        return None
    return from_arrow_table(read_parquet(url, filesystem=filesystem), output_format)


def read_parquet(url, filesystem=None, storage_options=None):
    """Read a Parquet dataset from any supported storage URL as an Arrow table.

    Resolution order is pyarrow's own filesystem handling first, then fsspec, then
    Metaflow's S3 client, which picks up Metaflow's role assumption and retry behaviour
    on S3.
    """
    import pyarrow.dataset as ds

    if filesystem is not None:
        return ds.dataset(url, filesystem=filesystem, format="parquet").to_table()

    try:
        import pyarrow.fs as pafs

        fs, path = pafs.FileSystem.from_uri(url)
        return ds.dataset(path, filesystem=fs, format="parquet").to_table()
    except Exception as pa_exc:
        try:
            import fsspec

            fs = fsspec.filesystem(
                url.split("://")[0] if "://" in url else "file",
                **(storage_options or {}),
            )
            return ds.dataset(url, filesystem=fs, format="parquet").to_table()
        except Exception:
            pass

        if url.startswith("s3://"):
            return _read_parquet_via_metaflow_s3(url)
        raise SparkException(
            "Could not read the Spark output at %s: %s" % (url, pa_exc)
        ) from pa_exc


def _read_parquet_via_metaflow_s3(url):
    from metaflow import S3
    from pyarrow.parquet import ParquetDataset

    from .context import log

    with S3() as s3:
        files = s3.get_recursive([url])
        parqs = [f for f in files if f.url.endswith(".parquet")]
        if not parqs:
            raise SparkException("No Parquet files found under %s." % url)
        total = sum(p.size for p in parqs)
        log("downloaded %.1fMB of compressed output" % (total / 1024**2))
        return ParquetDataset([p.path for p in parqs]).read()

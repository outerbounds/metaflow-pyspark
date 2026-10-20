"""Turning a Spark or Arrow result into the format a step asked for."""

from .exceptions import SparkConfigError, SparkException

FORMATS = ("pandas", "arrow", "polars", "none")


def validate_format(output_format):
    if output_format not in FORMATS:
        raise SparkConfigError(
            "Unknown @spark(output_format=%r). Choose one of: %s."
            % (output_format, ", ".join(FORMATS))
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
    validate_format(output_format)
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

from pydash import py_


DATA_URL = 'https://raw.githubusercontent.com/outerbounds/metaflow-pyspark/main/example/noaa_example.csv'

def findLargest(df, col_name):
    from pyspark.sql import functions as F
    """
    Find the largest value in `col_name` column.
    Values of 99.99, 999.9 and 9999.9 are excluded because they indicate "no reading" for that attribute.
    While 99.99 _could_ be a valid value for temperature, for example, we know there are higher readings.
    """
    return (
        df.select(
            "STATION", "DATE", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME", col_name
        )
        .filter(~F.col(col_name).isin([99.99, 999.9, 9999.9]))
        .orderBy(F.desc(col_name))
        .limit(1)
        .first()
    )

def run(spark, param=None):

    print('example parameter from @pyspark(job_parameters): ', param)

    # read data over HTTPS - this requires that Serverless has a NAT connection set up
    # from pyspark import SparkFiles
    # spark.sparkContext.addFile(DATA_URL)
    # spark.read.csv("file://"+SparkFiles.get("noaa_example.csv"), header=True, inferSchema=True)

    # read data from an S3 location
    df = spark.read.csv(f"s3://ville-sandbox/tmp/noaa/", header=True, inferSchema=True)

    print(f"The amount of weather readings is: {df.count()}\n")

    print(f"Here are some extreme weather stats:")
    stats_to_gather = [
        {"description": "Highest temperature", "column_name": "MAX", "units": "°F"},
        {
            "description": "Highest all-day average temperature",
            "column_name": "TEMP",
            "units": "°F",
        },
        {
            "description": "Highest average wind speed",
            "column_name": "WDSP",
            "units": "mph",
        },
        {
            "description": "Highest precipitation",
            "column_name": "PRCP",
            "units": "inches",
        },
    ]

    for stat in stats_to_gather:
        max_row = findLargest(df, py_.get(stat, "column_name"))
        print(
            f"  {stat['description']}: {max_row[stat['column_name']]}{stat['units']} on {max_row.DATE} at {max_row.NAME} ({max_row.LATITUDE}, {max_row.LONGITUDE})"
        )
    return df

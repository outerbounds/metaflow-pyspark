import pickle
import json
import sys

JOB_MODULE = "mfjob"
INPUTS = "inputs.pickle"


def download(url, dst):
    from urllib.parse import urlparse
    import boto3

    parsed = urlparse(url)
    s3 = boto3.client("s3")
    s3.download_file(parsed.netloc, parsed.path[1:], dst)


def start_job(job_url=None, input_url=None, func_name=None, output_url=None):

    download(job_url, JOB_MODULE + ".py")
    download(input_url, INPUTS)
    with open(INPUTS, "rb") as f:
        inputs = pickle.load(f)

    sys.path.append(".")
    mod = __import__(JOB_MODULE)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("metaflow-pyspark").getOrCreate()

    df = getattr(mod, func_name)(spark, **inputs)
    if df is not None:
        df.write.parquet(output_url)


if __name__ == "__main__":
    conf = json.loads(sys.argv[1])
    start_job(**conf)

from pyspark.sql import SparkSession
import os


packages = ",".join([
    "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4",
    "net.snowflake:snowflake-jdbc:3.14.0",
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.1,org.apache.hadoop:hadoop-common:3.3.1",
    "org.apache.hadoop:hadoop-client:3.3.1"
])

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages " + packages + "  pyspark-shell"

spark = None


def init_spark():
    '''
    #This function initializes a PySpark SparkSession with specific configurations,
    including application name, driver memory, external packages, exclusion of certain
    JARs, and S3 credentials.
    '''
    spark = SparkSession.builder \
        .appName("Project App") \
        .config("spark.driver.memory", "12g") \
        .config("spark.jars.packages", packages) \
        .config("spark.jars.excludes", "com.google.guava:guava") \
        .config('spark.hadoop.fs.s3a.access.key', "AKIAXYKEFI5RY2UZOUHX") \
        .config('spark.hadoop.fs.s3a.secret.key', "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") \
        .getOrCreate()
    return spark


baseInputPath = "/Users/anjali/Documents/yelp-data-segmentation/input"
baseOutputPath = "/Users/anjali/Documents/yelp-data-segmentation/output/"
env = "local"

if env == "aws":
    baseInputPath = "s3a://yelp-data-segmentation/input/"
    baseOutputPath = "s3a://yelp-data-segmentation/output/"


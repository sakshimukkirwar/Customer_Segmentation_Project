import sys

from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from pyspark.sql.types import TimestampType

from sampling import *
from sentiment import *

global sample


baseInputPath = baseInputPath






def process_user_data(spark, sample):
    '''
    This function processes user data in PySpark, attempting to read a Parquet file;
    if unsuccessful, it retrieves sampled user data, performs transformations, writes to Parquet,
    and finally returns the user DataFrame.
    '''
    try:
        userDf = spark.read.parquet(f"{sample_output_path(sample)}/user") 
    except Exception as e:

        sampled_users, is_sampled = get_sampled_users_data(spark, sample)
        userDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_user.json')
        if is_sampled:
            userDf = userDf.join(sampled_users, on = ["user_id"])

        userDf = userDf \
            .drop("friends") \
            .withColumn("elite", split(col("elite"), ", ")) \
            .withColumn("yelping_since", col("yelping_since").cast("timestamp"))

        userDf.repartition(1).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/user")
        userDf = spark.read.parquet(f"{sample_output_path(sample)}/user")
        print(f"sample users ares = {userDf.count()}")
    return userDf


def process_business_data(spark, sample):
    try:
        businessDf = spark.read.parquet(f"{sample_output_path(sample)}/business")
    except Exception as e:

        schema = StructType([
            StructField("address", StringType(), True),
            StructField("attributes", MapType(StringType(), StringType()), True),
            StructField("business_id", StringType(), True),
            StructField("categories", StringType(), True),
            StructField("city", StringType(), True),
            StructField("hours", MapType(StringType(), StringType()), True),
            StructField("is_open", LongType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("name", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("review_count", LongType(), True),
            StructField("stars", DoubleType(), True),
            StructField("state", StringType(), True),
        ])

        sampled_business, is_sampled = get_sampled_business_data(spark, sample)
        businessDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_business.json', schema)
        if is_sampled:
            businessDf = businessDf.join(sampled_business, on = ["business_id"])

        businessDf = businessDf \
            .withColumn("categories", split(col("categories"), ", "))
        businessDf.repartition(2).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/business")
        businessDf = spark.read.parquet(f"{sample_output_path(sample)}/business")
        print(f"sample business ares = {businessDf.count()}")

    return businessDf

#This function processes friends data in Spark, attempting to read a parquet file, and in case of an exception, retrieves sampled user data, joins it if sampled, selects relevant columns, prints the schema, writes the data to parquet, reads it again, and finally returns the resulting DataFrame.
def process_friends_data(spark, sample):
    try:
        friendsDf = spark.read.parquet(f"{sample_output_path(sample)}/friends")
    except Exception as e:

        sampled_users, is_sampled = get_sampled_users_data(spark, sample)
        friendsDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_user.json')
        if is_sampled:
            friendsDf = friendsDf.join(sampled_users, on = ["user_id"])

        friendsDf = friendsDf.select("user_id", split(col("friends"), ", ").alias("friends"))

        friendsDf.printSchema()
        friendsDf.repartition(2).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/friends")
        friendsDf = spark.read.parquet(f"{sample_output_path(sample)}/friends")
        print("sample friends ares = ", friendsDf.count())
    return friendsDf


def process_checkin_data(spark, sample):
    try:
        checkinDf = spark.read.parquet(f"{sample_output_path(sample)}/checkin")
    except Exception as e:

        sampled_business, is_sampled = get_sampled_business_data(spark, sample)
        checkinDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_checkin.json')
        if is_sampled:
            checkinDf = checkinDf.join(sampled_business, on = ["business_id"])

        checkinDf = checkinDf \
            .withColumn("date", expr("transform(split(date, ', '), d -> to_timestamp(d))").cast(ArrayType(TimestampType())))

        checkinDf.printSchema()

        checkinDf.repartition(1).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/checkin")
        checkinDf = spark.read.parquet(f"{sample_output_path(sample)}/checkin")
        print("sample checkin ares = ", checkinDf.count())
    return checkinDf


def process_tip_data(spark, sample):
    try:
        tipDf = spark.read.parquet(f"{sample_output_path(sample)}/tip")
    except Exception as e:

        sampled_users, is_sampled = get_sampled_users_data(spark, sample)
        tipDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_tip.json')
        if is_sampled:
            tipDf = tipDf.join(sampled_users, on = ["user_id"])

        tipDf = tipDf.withColumn("date", col("date").cast("timestamp"))

        tipDf.repartition(1).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/tip")
        tipDf = spark.read.parquet(f"{sample_output_path(sample)}/tip")
        print("sample tip ares = ", tipDf.count())
    return tipDf


def process_review_data(spark, sample):
    try:
        reviewDf = spark.read.parquet(f"{sample_output_path(sample)}/review")
    except Exception as e:
        sampled_users, is_sampled = get_sampled_users_data(spark, sample)
        reviewDf = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_review.json')
        if is_sampled:
            reviewDf = reviewDf.join(sampled_users, on = ["user_id"])

        reviewDf = reviewDf \
            .withColumn("date", col("date").cast("timestamp")) \
            .withColumn("sentiment",  get_sentiment(col("text"))) \
            .withColumn("frequent_words", tokenize_and_get_top_words(col("text")))

        reviewDf.printSchema()
        reviewDf.repartition(4).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/review")
        reviewDf = spark.read.parquet(f"{sample_output_path(sample)}/review")
        print("sample review ares = ", reviewDf.count())
    return reviewDf


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: data_processing.py <baseInputPath> <baseOutputPath> <sample>")
        exit(-1)

    baseInputPath = sys.argv[1]
    baseOutputPath = sys.argv[2]
    sample = float(sys.argv[3])

    sparkSession = init_spark()
    user_df = process_user_data(sparkSession, sample)
    business_df = process_business_data(sparkSession, sample)
    friends_df = process_friends_data(sparkSession, sample)
    checkin_df = process_checkin_data(sparkSession, sample)
    tip_df = process_tip_data(sparkSession, sample)
    review_df = process_review_data(sparkSession, sample)

    sparkSession.stop()

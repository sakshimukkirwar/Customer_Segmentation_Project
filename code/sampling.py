import sys

from pyspark.sql.functions import col

from commons import *

baseInputPath = baseInputPath


def sample_output_path(sample):
    return f"{baseOutputPath}/sample={sample}/"
    #This function generates a formatted output path based on the provided sample identifier within the context of a given base output path.


def get_sampled_users_data(spark, sample):
    if sample != 1:
        try:
            sampled_users = spark.read.parquet(f"{sample_output_path(sample)}/sampled_user_id")
        except Exception as e:
            sampled_users = spark.read.json(f'{baseInputPath}/yelp_academic_dataset_review.json') \
                .groupBy("user_id").count().orderBy(col("count").desc()).select("user_id").sample(sample)

            sampled_users.repartition(1).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/sampled_user_id")
            sampled_users = spark.read.parquet(f"{sample_output_path(sample)}/sampled_user_id")
            print(f"sample users ares = {sampled_users.count()}")
        return sampled_users, True
    else:
        return None, False


def get_sampled_business_data(spark, sample):
    if sample != 1:
        try:
            sampled_business = spark.read.parquet(f"{sample_output_path(sample)}/sampled_business_id")
        except Exception as e:

            sampled_user, _ = get_sampled_users_data(spark, sample)
            sampled_business =  spark.read.json(f'{baseInputPath}/yelp_academic_dataset_review.json') \
                .join(sampled_user, on = ["user_id"]) \
                .select("business_id").distinct()

            sampled_business.repartition(1).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/sampled_business_id")
            sampled_business = spark.read.parquet(f"{sample_output_path(sample)}/sampled_business_id")
            print(f"sample business ares = {sampled_business.count()}")
        return sampled_business, True
    else:
        return None, False


if __name__ == "__main__":
    sample = float(sys.argv[1])
    spark = init_spark()
    get_sampled_users_data(spark, sample)
    get_sampled_business_data(spark, sample)
    spark.stop()

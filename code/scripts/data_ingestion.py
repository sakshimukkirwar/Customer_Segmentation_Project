import sys

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from commons import *
from sampling import *
from sentiment import *

global sample

class Consumer:

    def __init__(self, server, output_path):
        self.server = server
        self.output_path = output_path

    def read_from_topic(self, spark, topic):
        print(f"reading data from the topic = ", topic, "server = ", self.server)
        df = (
            spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.server)
            .option("startingOffsets", "earliest")
            .option("subscribe", topic)
            .load()
        )
       
	
	print(f"is spark is reading the streams from kafka = {df.isStreaming}")
        df.printSchema()
        return df.withColumn("json_string", col("value").cast(StringType()))

    def write_stream(self, df_result, topic_name):
        writer = df_result \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", f"{self.output_path}/{topic_name}/data") \
            .option("checkpointLocation", f"{self.output_path}/{topic_name}/checkpoint") \
            .trigger(processingTime="10 seconds") \
            .start()
        writer.awaitTermination()

    def read_checkins(self, spark):
        topicName = "checkins"
        stream_df = self.read_from_topic(spark, topicName)
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("date", StringType(), True),
        ])
        df_result = stream_df.select(from_json(col("json_string"), schema).alias("data"))
        self.write_stream(df_result, topicName)

    def read_tips(self, spark):
        topicName = "tips"
        stream_df = self.read_from_topic(spark, topicName)
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("compliment_count", LongType(), True),
            StructField("date", StringType(), True),
            StructField("text", StringType(), True),
            StructField("user_id", StringType(), True),
        ])
        df_result = stream_df.select(from_json(col("json_string"), schema).alias("data"))
        self.write_stream(df_result, topicName)

    def process_review_data_df(self, review_df, x):
        sampled_users, is_sampled = get_sampled_users_data(spark, sample)
        if is_sampled:
            print("got sampled users ... processing that.")
            sampled_users.printSchema()
            review_df.printSchema()
            review_df = review_df.join(sampled_users, on=["user_id"])

        review_df = review_df \
            .withColumn("date", col("date").cast("timestamp")) \
            .withColumn("sentiment",  get_sentiment(col("text"))) \
            .withColumn("frequent_words", tokenize_and_get_top_words(col("text")))

        review_df.printSchema()
        review_df.repartition(1).write.mode("append").parquet(f"{sample_output_path(sample)}/review")
        print("sample review ares = ", review_df.count())
        return review_df

    def read_reviews(self, spark):
        topicName = "reviews"
        schema = StructType([
            StructField("business_id", StringType(), True),
            StructField("cool", LongType(), True),
            StructField("date", StringType(), True),
            StructField("funny", LongType(), True),
            StructField("review_id", StringType(), True),
            StructField("stars", DoubleType(), True),
            StructField("text", StringType(), True),
            StructField("useful", LongType(), True),
            StructField("user_id", StringType(), True),
        ])
        stream_df = self.read_from_topic(spark, topicName)
        df_result = stream_df.select(from_json(col("json_string"), schema).alias("data")).select("data.*")
        writer = df_result.writeStream.outputMode("append").foreachBatch(self.process_review_data_df).start()
        writer.awaitTermination(30)


if __name__ == "__main__":
    if len(sys.argv) >= 4:
        server = sys.argv[1]
        topic = sys.argv[2]
        output_path = sys.argv[3]
        sample = float(sys.argv[4])
        spark = init_spark()
        consumer = Consumer(server, output_path)
        consumer.read_reviews(spark)
        consumer.read_tips(spark)
        consumer.read_checkins(spark)
        spark.stop()
    else:
        print("Invalid number of arguments. Please pass the server and topic name")

#%%

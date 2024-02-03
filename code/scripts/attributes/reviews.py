from pyspark.sql.functions import col, udf
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import explode, create_map
from pyspark.sql.types import IntegerType, MapType
from pyspark.sql.types import StringType



@udf(MapType(StringType(), IntegerType()))
def merge_maps_array(map_array):
    result = {}
    for m in map_array:
        for k, v in m.items():
            result[k] = result.get(k, 0) + v
    return result






def get_sentiments_count(review_df):
    df = review_df.select("user_id", "sentiment").groupBy("user_id", "sentiment").count() \
        .withColumnRenamed("count", "sentiment_count") \
        .withColumn("sentiment_map", create_map(col("sentiment"), col("sentiment_count"))) \
        .groupBy("user_id").agg(collect_list(col("sentiment_map")).alias("sentiment_map")) \
        .withColumn("sentiment_map", merge_maps_array(col("sentiment_map")))

    return df


def most_frequent_words(review_df):
    return review_df.select("user_id", explode("frequent_words").alias("frequent_words")) \
        .groupBy("user_id", "frequent_words").count() \
        .withColumn("frequent_words_map", create_map(col("frequent_words"), col("count"))) \
        .groupBy("user_id").agg(collect_list(col("frequent_words_map")).alias("frequent_words_map")) \
        .withColumn("frequent_words_map", merge_maps_array(col("frequent_words_map")))


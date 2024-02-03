
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import collect_list, avg
from pyspark.sql.functions import explode, create_map
from pyspark.sql.functions import size
from pyspark.sql.types import IntegerType, MapType, FloatType
from pyspark.sql.types import StringType


@udf(MapType(StringType(), IntegerType()))
def merge_maps_array(map_array):
    result = {}
    for m in map_array:
        for k, v in m.items():
            result[k] = result.get(k, 0) + v
    return result


def get_customer_category_counts(review_df, business_df):
    df = review_df.select("user_id", "business_id") \
        .join(business_df.select("business_id", "categories"), on=["business_id"]) \
        .select("user_id", explode("categories").alias("category")) \
        .groupBy("user_id", "category").count() \
        .withColumn("category_map", create_map(col("category"), col("count"))) \
        .groupBy("user_id").agg(collect_list(col("category_map")).alias("category_map")) \
        .withColumn("category_map", merge_maps_array(col("category_map"))) \
        .select("user_id", "category_map")

    return df


@udf(MapType(StringType(), FloatType()))
def merge_maps_array_float(map_array):
    result = {}
    for m in map_array:
        for k, v in m.items():
            result[k] = result.get(k, 0) + v
    return result


def get_customer_category_avg_rating(review_df, business_df):
    df = review_df.select("user_id", "business_id", "stars") \
        .filter(col("stars").isNotNull()) \
        .join(business_df.select("business_id", "categories"), on=["business_id"]) \
        .select("user_id", "stars", explode("categories").alias("category")) \
        .groupBy("user_id", "category").agg(avg(col("stars")).alias("avg_stars")) \
        .withColumn("category_map", create_map(col("category"), col("avg_stars"))) \
        .groupBy("user_id").agg(collect_list(col("category_map")).alias("category_map")) \
        .withColumn("category_avg_stars", merge_maps_array_float(col("category_map"))) \
        .select("user_id", "category_avg_stars")

    return df


@udf(StringType())
def home_city(items):
    if items is None:
        return None
    return max(items, key=lambda x: items.count(x))


@udf(MapType(StringType(), IntegerType()))
def traveling_city(items, home):
    if items is None:
        return None
    travel_map = {}
    for i in items:
        if i == home:
            continue
        travel_map[i] = travel_map.get(i, 0) + 1
    return travel_map


def get_customer_area(review_df, business_df):
    df = review_df.select("user_id", "business_id") \
        .join(business_df.select("business_id", "city"), on=["business_id"]) \
        .select("user_id", "city") \
        .groupBy("user_id").agg(collect_list(col("city")).alias("city_freq")) \
        .withColumn("city", home_city(col("city_freq"))) \
        .withColumn("travel_map", traveling_city(col("city_freq"), col("city"))) \
        .select("user_id", "city", "travel_map") \
        .join(business_df.select("city", "latitude", "longitude").dropDuplicates(["city"]), on=["city"]) \
        .select("user_id", "city", "travel_map", "latitude", "longitude") \
        .withColumnRenamed("city", "user_city") \

    return df

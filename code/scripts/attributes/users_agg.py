from pyspark.sql.functions import col
from pyspark.sql.functions import size


def get_friends_count(friends_df):
    df = friends_df.select("user_id", size(col("friends")).alias("friends_count"))
    return df


def get_customer_agg_value(spark, review_df):
    review_df.createOrReplaceTempView("review")
    return spark.sql("""
        select 
            user_id, 
            min(date) as first_seen, 
            max(date) as last_seen, 
            DATEDIFF(max(date), min(date)) as date_diff,
            count(distinct business_id) as different_business_count,
            avg(stars) as avg_rating,
            min(stars) as min_stars,
            max(stars) as max_stars
        from review
        group by user_id 
    """)


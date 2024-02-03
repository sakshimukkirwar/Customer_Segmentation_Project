import sys

from data_processing import *

from attributes.users_agg import *
from attributes.business import *
from attributes.reviews import *

from storage import *


def merge_attributes(spark, sample):
    # This method will aggregate all the features at a single page
    user_df = process_user_data(spark, sample)
    business_df = process_business_data(spark, sample)
    friends_df = process_friends_data(spark, sample)
    checkin_df = process_checkin_data(spark, sample)
    tip_df = process_tip_data(spark, sample)
    review_df = process_review_data(spark, sample)

    avg_catg_start_df = get_customer_category_avg_rating(review_df, business_df)
    customer_area_df = get_customer_area(review_df, business_df)
    user_agg_df = get_customer_agg_value(spark, review_df)
    user_category_df = get_customer_category_counts(review_df, business_df)
    friends_count_df = get_friends_count(friends_df)
    sentiment_count_df = get_sentiments_count(review_df)
    frequent_words_df = most_frequent_words(review_df)

    complete_user_df = user_df \
        .join(user_agg_df, on=["user_id"]) \
        .join(user_category_df, on=["user_id"]) \
        .join(friends_count_df, on=["user_id"]) \
        .join(sentiment_count_df, on=["user_id"]) \
        .join(frequent_words_df, on=["user_id"]) \
        .join(customer_area_df, on=["user_id"]) \
        .join(avg_catg_start_df, on=["user_id"])

    complete_user_df.printSchema()
    # complete_user_df.show()
    print("total counts are = ", complete_user_df.count(), "  user counts = ", user_df.count())

    complete_user_df.repartition(4).write.mode("overwrite").parquet(f"{sample_output_path(sample)}/combined")
    return spark.read.parquet(f"{sample_output_path(sample)}/combined")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: user_attributes.py <sample>")
        exit(-1)

    sample = float(sys.argv[1])
    sparkSession = init_spark()
    merged_df = merge_attributes(sparkSession, sample)
    save_spark_df_to_db(merged_df, "users")
    sparkSession.stop()

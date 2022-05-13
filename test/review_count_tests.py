from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from pandemic_recovery_batch import create_checkin_df_with_one_date_per_row, count_reviews, count_checkins, count_tips
from test.saff_squeeze_start_point import create_df_from_json, data_frame_to_json, save_results_to_expected
import pandas as pd


############################# SAFF SQUEEZE #################################
def test_keeps_mobile_reviews_without_checkins(spark: SparkSession) -> None:
    empty = [{
        "user_id": "",
        "business_id": "",
        "date": ""
    }]  # b_reviews_df = spark.read.json(spark.sparkContext.parallelize([browser_reviews]))
    b_reviews_df = spark.createDataFrame(pd.DataFrame(empty))

    mobile_reviews = [{
        "user_id": "my_user_id_2",
        "business_id": "mpf3x-BjTdTEA3yCZrAYPw",
        "date": "2022-04-14 00:01:03"
    }]
    m_reviews_df = spark.createDataFrame(pd.DataFrame(mobile_reviews))


    checkin_df = spark.createDataFrame(pd.DataFrame(empty))
    date = datetime(2022, 4, 14)

    reviews_df = count_reviews(checkin_df, m_reviews_df, b_reviews_df, date)  # <- This is what we care about

    inglewood_pizza = data_frame_to_json(reviews_df.where(reviews_df.business_id == "mpf3x-BjTdTEA3yCZrAYPw"))[0]
    assert inglewood_pizza["business_id"] == "mpf3x-BjTdTEA3yCZrAYPw"
    assert inglewood_pizza["num_reviews"] == 1
############################# SAFF SQUEEZE #################################

from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from pandemic_recovery_batch import create_checkin_df_with_one_date_per_row, count_reviews, count_checkins, count_tips
from test.saff_squeeze_start_point import create_df_from_json, data_frame_to_json, save_results_to_expected


############################# SAFF SQUEEZE #################################
def test_keeps_mobile_reviews_without_checkins(spark: SparkSession) -> None:
    browser_reviews = """
  {
    "user_id": "my_user_id",
    "business_id": "mpf3x-BjTdTEA3yCZrAYPw",
    "date": "2021-09-23 23:10:31"
  }
    """
    b_reviews_df = spark.read.json(spark.sparkContext.parallelize([browser_reviews]))

    mobile_reviews = """
  {
    "user_id": "my_user_id_2",
    "business_id": "mpf3x-BjTdTEA3yCZrAYPw",
    "date": "2022-04-14 00:01:03"
  }    
    """
    m_reviews_df = spark.read.json(spark.sparkContext.parallelize([mobile_reviews]))

    checkins = """
  {
    "business_id": "mpf3x-BjTdTEA3yCZrAYPw",
    "user_id": "mh_-eMZ6K5RLWhZyISBhwA",
    "date": "2010-09-13 21:43:09"
  }    
    """
    checkin_df = spark.read.json(spark.sparkContext.parallelize([checkins]))
    date = datetime(2022, 4, 14)

    reviews_df = count_reviews(checkin_df, m_reviews_df, b_reviews_df, date)  # <- This is what we care about


    inglewood_pizza = data_frame_to_json(reviews_df.where( reviews_df.business_id == "mpf3x-BjTdTEA3yCZrAYPw"))[0]
    assert inglewood_pizza["business_id"] == "mpf3x-BjTdTEA3yCZrAYPw"
    assert inglewood_pizza["num_reviews"] == 1
############################# SAFF SQUEEZE #################################




from datetime import datetime

from pyspark.sql import SparkSession, functions as F

from pandemic_recovery_batch import create_checkin_df_with_one_date_per_row, count_reviews, count_checkins, count_tips
from test.saff_squeeze_start_point import create_df_from_json, data_frame_to_json, save_results_to_expected


############################# SAFF SQUEEZE #################################
def test_keeps_mobile_reviews_without_checkins(spark: SparkSession) -> None:
    browser_reviews = """
  {
    "user_id": "mh_-eMZ6K5RLWhZyISBhwA",
    "business_id": "mpf3x-BjTdTEA3yCZrAYPw",
    "date": "2021-09-23 23:10:31"
  }
    """
    b_reviews_df = spark.read.json(spark.sparkContext.parallelize([browser_reviews]))

    mobile_reviews = """
  {
    "user_id": "_7bHUi9Uuf5__HHc_Q8guQ",
    "business_id": "mpf3x-BjTdTEA3yCZrAYPw",
    "date": "2022-04-14 00:01:03"
  }    
    """
    m_reviews_df = spark.read.json(spark.sparkContext.parallelize([mobile_reviews]))
    date = datetime(2022, 4, 14)

    checkin_df = create_df_from_json("fixtures/checkins_exploded.json", spark)
    reviews_df = count_reviews(checkin_df, m_reviews_df, b_reviews_df, date)  # <- This is what we care about


    inglewood_pizza = data_frame_to_json(reviews_df.where( reviews_df.business_id == "mpf3x-BjTdTEA3yCZrAYPw"))[0]
    assert inglewood_pizza["business_id"] == "mpf3x-BjTdTEA3yCZrAYPw"
    assert inglewood_pizza["num_reviews"] == 1
############################# SAFF SQUEEZE #################################




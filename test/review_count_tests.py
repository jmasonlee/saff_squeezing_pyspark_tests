from datetime import datetime

from chispa import assert_df_equality
from pyspark.sql import SparkSession, functions as F

from pandemic_recovery_batch import create_checkin_df_with_one_date_per_row, count_interactions_from_reviews, count_checkins, count_tips
from test.saff_squeeze_start_point import create_df_from_json, data_frame_to_json, save_results_to_expected
import pandas as pd


############################# SAFF SQUEEZE #################################
def test_keeps_mobile_reviews_without_checkins(spark: SparkSession) -> None:
    empty = [{
        "user_id": "",
        "business_id": "",
        "date": ""
    }]
    checkin_df = spark.createDataFrame(pd.DataFrame(empty))
    b_reviews_df = spark.createDataFrame(pd.DataFrame(empty))

    business_id = "my_business_id"
    mobile_reviews = [{
        "user_id": "my_user_id_2",
        "business_id": business_id,
        "date": "2022-04-14 00:01:03"
    }]
    m_reviews_df = spark.createDataFrame(pd.DataFrame(mobile_reviews))


    date = datetime(2022, 4, 14)

    reviews_df = count_interactions_from_reviews(checkin_df, m_reviews_df, b_reviews_df, date)  # <- This is what we care about

    expected_reviews = [{
        "business_id": business_id,
        "num_reviews": 1
    }]
    expected_reviews_df = spark.createDataFrame(pd.DataFrame(expected_reviews))

    assert_df_equality(reviews_df, expected_reviews_df, ignore_nullable=True)

############################# SAFF SQUEEZE #################################

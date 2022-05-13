from datetime import datetime

import pytest
from chispa import assert_df_equality
from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession, functions as F

from pandemic_recovery_batch import create_checkin_df_with_one_date_per_row, count_interactions_from_reviews, count_checkins, count_tips
from test.saff_squeeze_start_point import create_df_from_json, data_frame_to_json, save_results_to_expected
import pandas as pd


############################# SAFF SQUEEZE #################################
def test_keeps_mobile_reviews_without_checkins(spark: SparkSession) -> None:
    def __(map):
        return as_dataframe(map, spark)
    business_id = "my_business_id"
    mobile_reviews = [{
        "user_id": "my_user_id_2",
        "business_id": business_id,
        "date": "2022-04-14 00:01:03"
    }]
    date = datetime(2022, 4, 14)

    reviews_df = count_interactions_from_reviews(__(empty()), __(mobile_reviews), __(empty()),
                                                 date)  # <- This is what we care about

    expected_reviews = [{
        "business_id": business_id,
        "num_reviews": 1
    }]
    assert_dataframes_are_equal(reviews_df, __(expected_reviews))

def empty():
    return [{
        "user_id": "",
        "business_id": "",
        "date": ""
    }]

def as_dataframe(map, spark) -> DataFrame:
    return spark.createDataFrame(pd.DataFrame(map))

def assert_dataframes_are_equal(actual, expected):
    assert_df_equality(actual, expected, ignore_nullable=True)

############################# SAFF SQUEEZE #################################

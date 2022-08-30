from datetime import datetime


from chispa import assert_df_equality
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from test.test_dataframe import TestDataFrame

SCHEMA = StructType([StructField("review_id", StringType()), StructField("user_id", StringType()),
                StructField("business_id", StringType()), StructField("stars", FloatType()), StructField("funny", IntegerType()),
                StructField("cool", IntegerType()), StructField("text", StringType()),
                StructField("date", StringType())])

import pandas as pd

from pandemic_recovery_batch import count_interactions_from_reviews, create_checkin_df_with_one_date_per_row

DEFAULT_STRING = "default"
DEFAULT_INT = -1
DEFAULT_FLOAT = -1.0


def count_reviews(df: DataFrame, business_id: str, spark: SparkSession, date=None) -> int:
    """
    1) Pass the dataframe into the function we're testing
    2) We're going to return the number of reviews for the business
    """
    def __(map):
        return as_dataframe(map, spark)
    count_interactions_from_reviews(__(empty()), df, __(empty()), date)  # <- This is what we care about


def test_multiple_row_df_creation(spark):
    # reference_df = spark.createDataFrame({"business_id": business_id, "date": date, "user": user_id})
    input_df = TestDataFrame(spark).with_data(
        [{"business_id": "Crusty Crab", "date": "2000-01-02 03:04:05, 2000-01-01 04:05:06",
          "user_id": "Scooby-Doo"}]).create_df()

    df_actual = create_checkin_df_with_one_date_per_row(input_df)

    # row = {"user_id" : user_id, "date": date, "business_id": business_id}
    # df_actual = review_dataframe.with_row(row)
    df_expected = spark.createDataFrame(
        [
            {"user_id": "Scooby-Doo", "date": "2000-01-02 03:04:05", "business_id": "Crusty Crab"},
            {"user_id": "Scooby-Doo", "date": "2000-01-01 04:05:06", "business_id": "Crusty Crab"}
        ]
    )
    assert_df_equality(df_expected, df_actual, ignore_nullable=True, ignore_column_order=True)


def test_foo(spark):
    assert True
    # review_dataframe = ReviewDataFrame(spark)
    # # A PERSON Makes a new review at A TIME at THE BUSINESS
    # business = "Crusty Crab"
    # date = "2000-01-02 03:04:05"
    # df = review_dataframe.of(user_id="Scooby-Doo", date=date, business_id=business)
    # # This(=df?) should count as one review at THE BUSINESS
    # assert count_reviews(df=df, business_id=business, spark=spark, date=date) == 1
    #
    # # JACQUELINE Makes a new review at NOON_FRIDAY at INGLEWOOD PIZZA
    # # This should count as one review at INGLEWOOD PIZZA










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

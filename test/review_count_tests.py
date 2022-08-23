from datetime import datetime


from chispa import assert_df_equality
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

SCHEMA = StructType([StructField("review_id", StringType()), StructField("user_id", StringType()),
                StructField("business_id", StringType()), StructField("stars", FloatType()), StructField("funny", IntegerType()),
                StructField("cool", IntegerType()), StructField("text", StringType()),
                StructField("date", StringType())])

import pandas as pd



#There's a review without a checkin at the same time
# This thing is counted as a review
from pandemic_recovery_batch import count_interactions_from_reviews

DEFAULT_STRING = "default"
DEFAULT_INT = -1
DEFAULT_FLOAT = -1.0

class ReviewDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.schema = SCHEMA

    def of(self, review_id=DEFAULT_STRING,
           user_id=DEFAULT_STRING,
           business_id=DEFAULT_STRING,
           stars=DEFAULT_FLOAT,
           useful=DEFAULT_INT,
           funny=DEFAULT_INT,
           cool=DEFAULT_INT,
           text=DEFAULT_STRING,
           date=DEFAULT_STRING):
        data = [(
            review_id,
            user_id,
            business_id,
            stars,
            useful,
            funny,
            cool,
            text,
            date,
        )]
        return self.spark.createDataFrame(data=data, schema=self.schema)

    def with_row(self, row: dict):
        return self.of(**row) ###REVISIT HERE
        #TODO:Genericize


def count_reviews(df: DataFrame, business_id: str, spark: SparkSession, date=None) -> int:
    """
    1) Pass the dataframe into the function we're testing
    2) We're going to return the number of reviews for the business
    """
    def __(map):
        return as_dataframe(map, spark)
    count_interactions_from_reviews(__(empty()), df, __(empty()), date)  # <- This is what we care about


def test_multiple_row_df_creation(spark):
    review_dataframe = ReviewDataFrame(spark)
    business = "Crusty Crab"
    date = "2000-01-02 03:04:05"
    user = "Scooby-Doo"
    row = {"user_id" : user, "date": date, "business_id": business}
    df_actual = review_dataframe.with_row(row)
    df_expected = spark.createDataFrame(schema=SCHEMA, data=[(user, date, business)])
    assert_df_equality(df_expected, df_actual)


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

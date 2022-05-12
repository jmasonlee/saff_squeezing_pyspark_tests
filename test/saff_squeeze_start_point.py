import json
from datetime import datetime
from typing import List

import pytest
from chispa import assert_df_equality, assert_column_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, NumericType, LongType

from pandemic_recovery_batch import transform, count_reviews, create_checkin_df_with_one_date_per_row

@pytest.fixture()
def reviews_schema() -> StructType:
    return StructType([StructField('user_id', StringType()),
                       StructField('business_id', StringType()),
                       StructField('date', StringType())])


@pytest.fixture()
def empty_reviews_df(reviews_schema: StructType, spark:SparkSession) -> DataFrame:
    return spark.createDataFrame([], reviews_schema)

@pytest.fixture
def checkin_df_with_one_date_per_row(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            ("my_business_id", "my_user_id", "2014-04-12 23:04:47"),
            ("my_business_id", "my_user_id", "2022-04-14 00:31:02")
        ],
        StructType(
            [
                StructField('business_id', StringType()),
                StructField('user_id', StringType()),
                StructField('date', StringType())
            ]
        ))


def test_will_do_the_right_thing(spark: SparkSession) -> None:
    b_reviews_df = create_df_from_json("fixtures/browser_reviews.json", spark)
    checkin_df = create_df_from_json("fixtures/checkin.json", spark)
    tips_df = create_df_from_json("fixtures/tips.json", spark)
    business_df = create_df_from_json("fixtures/business.json", spark)
    m_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)

    actual_df = transform(
        business_df,
        checkin_df,
        b_reviews_df,
        tips_df,
        m_reviews_df,
        datetime(2022, 4, 14)
    )

    assert data_frame_to_json(actual_df) == expected_json()
    # with open("fixtures/expected.json", "w") as f:
    #     jsons = ''.join(
    #         json.dumps(line) if line else line
    #         for line in data_frame_to_json(actual_df)
    #     )
    #
    #     f.write(jsons)


def test_only_counts_mobile_reviews_without_matching_checkins(
        spark: SparkSession,
        checkin_df_with_one_date_per_row: DataFrame,
        reviews_schema: StructType,
        empty_reviews_df: DataFrame
) -> None:
    b_reviews_df = empty_reviews_df

    mobile_review_only = "mobile_review_only_business_id"
    has_mobile_review_and_checkin = "my_business_id"
    m_reviews_df = spark.createDataFrame(
        [
            ("my_user_id", mobile_review_only, "2022-04-14 00:01:03"),
            ("my_user_id", has_mobile_review_and_checkin, "2022-04-14 00:31:02")
        ],
        reviews_schema
    )
    date = datetime(2022, 4, 14)

    reviews_df = count_reviews(checkin_df_with_one_date_per_row, m_reviews_df, b_reviews_df, date)

    reviews_df = reviews_df.withColumn("expected_num_reviews",
                                       when(reviews_df.business_id == mobile_review_only, 1)
                                       .when(reviews_df.business_id == has_mobile_review_and_checkin, 0)
                                       .otherwise(None))
    assert_column_equality(reviews_df, "num_reviews", "expected_num_reviews")




def test_create_checkin_df_with_one_date_per_row(
        spark: SparkSession,
        checkin_df_with_one_date_per_row
):
    dates = "2014-04-12 23:04:47,2022-04-14 00:31:02"
    input_df = spark.createDataFrame(
        [("my_business_id", "my_user_id", dates)],
        StructType(
            [
                StructField('business_id', StringType()),
                StructField('user_id', StringType()),
                StructField('date', StringType())
            ]
        ))
    output_df = create_checkin_df_with_one_date_per_row(input_df)
    expected_output = checkin_df_with_one_date_per_row
    assert_df_equality(output_df, expected_output)


def create_df_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["business_id"])
    return output


def expected_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

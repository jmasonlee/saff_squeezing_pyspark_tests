import json
from datetime import datetime
from typing import List

import pytest
from chispa import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, NumericType

from pandemic_recovery_batch import transform, count_reviews, create_checkin_df_with_one_date_per_row


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
    b_reviews_df   = create_df_from_json("fixtures/browser_reviews.json", spark)
    checkin_df   = create_df_from_json("fixtures/checkin.json", spark)
    tips_df      = create_df_from_json("fixtures/tips.json", spark)
    business_df  = create_df_from_json("fixtures/business.json", spark)
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

def test_keeps_mobile_reviews_without_matching_checkins(
        spark: SparkSession,
        checkin_df_with_one_date_per_row: DataFrame
) -> None:
    reviews_schema = StructType([StructField('review_id', StringType()), StructField('user_id', StringType()),
                              StructField('business_id', StringType()), StructField('stars', StringType()),
                              StructField('useful', StringType()), StructField('funny', StringType()),
                              StructField('cool', StringType()), StructField('text', StringType()),
                              StructField('date', StringType()), ])
    b_reviews_df = spark.createDataFrame([],reviews_schema)
    m_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)
    date = datetime(2022, 4, 14)

    reviews_df = count_reviews(checkin_df_with_one_date_per_row, m_reviews_df, b_reviews_df, date)

    business_with_mobile_review_only = data_frame_to_json(reviews_df)[2]
    assert business_with_mobile_review_only["num_reviews"] == 1

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

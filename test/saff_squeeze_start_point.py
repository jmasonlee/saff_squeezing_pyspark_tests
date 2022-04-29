import json
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession

from pandemic_recovery_batch import transform

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

    expected_json = read_json()
    assert data_frame_to_json(actual_df) == expected_json

def test_will_count_reviews_without_matching_checkins(spark: SparkSession) -> None:
    browser_reviews_df = create_df_from_json("fixtures/browser_reviews.json", spark)
    checkin_df = create_df_from_json("fixtures/checkin.json", spark)
    tips_df = create_df_from_json("fixtures/tips.json", spark)
    business_df = create_df_from_json("fixtures/business.json", spark)
    mobile_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)

    actual_df = transform(
        business_df, checkin_df, browser_reviews_df, tips_df, mobile_reviews_df, datetime(2022, 4, 14)
    )

    actual_json = data_frame_to_json(actual_df)
    assert actual_json[4]["num_reviews"] == 2

def test_will_not_count_reviews_with_matching_checkins(spark: SparkSession) -> None:
    browser_reviews_df = create_df_from_json("fixtures/browser_reviews.json", spark)
    checkin_df = create_df_from_json("fixtures/checkin.json", spark)
    tips_df = create_df_from_json("fixtures/tips.json", spark)
    business_df = create_df_from_json("fixtures/business.json", spark)
    mobile_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)

    actual_df = transform(
        business_df, checkin_df, browser_reviews_df, tips_df, mobile_reviews_df, datetime(2022, 4, 14)
    )

    actual_json = data_frame_to_json(actual_df)
    assert actual_json[3]["num_reviews"] == 0


def create_df_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["business_id"])
    return output


def read_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

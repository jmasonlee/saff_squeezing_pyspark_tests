import json
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, DateType, StringType

from pandemic_recovery_batch import transform


def test_will_count_reviews_without_matching_checkins(spark: SparkSession) -> None:
    reviews_df = create_df_from_json("fixtures/reviews.json", spark)
    checkin_df = spark.createDataFrame(
        [], StructType([StructField("date", DateType()), StructField("business_id", StringType())]))
    tips_df = spark.createDataFrame(
        [], StructType([StructField("date", DateType()), StructField("business_id", StringType())]))
    business_df = create_df_from_json("fixtures/business.json", spark)
    mobile_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)

    actual_df = transform(
        business_df,
        checkin_df,
        reviews_df,
        tips_df,
        mobile_reviews_df,
        datetime(2022, 4, 14).strftime('%Y-%m-%d')
    )

    actual_json = data_frame_to_json(actual_df)
    assert actual_json[4]["num_reviews"] == 1


def create_df_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["business_id"])
    return output


def read_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

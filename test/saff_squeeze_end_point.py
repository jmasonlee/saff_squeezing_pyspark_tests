import json
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, DateType, StringType

from pandemic_recovery_batch import transform


def test_will_count_reviews_without_matching_checkins(spark: SparkSession) -> None:
    run_date = datetime(2022, 4, 14).strftime('%Y-%m-%d')
    reviews_df = spark.createDataFrame(
        [(run_date, "fake_business_id")], StructType([StructField("date", StringType()), StructField("business_id", StringType())]))
    checkin_df = spark.createDataFrame(
        [], StructType([StructField("date", DateType()), StructField("business_id", StringType())]))
    tips_df = spark.createDataFrame(
        [], StructType([StructField("date", DateType()), StructField("business_id", StringType())]))
    business_df = spark.createDataFrame(
        [("fake_business_id", "fake_business")], StructType([StructField("business_id", StringType()),StructField("name", StringType())]))
    mobile_reviews_df = spark.createDataFrame(
        [(run_date, "fake_business_id")], StructType([StructField("date", StringType()), StructField("business_id", StringType())]))

    actual_df = transform(
        business_df,
        checkin_df,
        reviews_df,
        tips_df,
        mobile_reviews_df,
        run_date
    )

    actual_json = data_frame_to_json(actual_df)
    assert actual_json[0]["num_reviews"] == 2


def create_df_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["business_id"])
    return output


def read_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

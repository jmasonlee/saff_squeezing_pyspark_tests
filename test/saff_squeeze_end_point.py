import json
from datetime import datetime
from typing import List

from chispa import assert_column_equality
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

from pandemic_recovery_batch import count_reviews


def test_will_count_reviews_without_matching_checkins(spark: SparkSession) -> None:
    run_date = datetime(2022, 4, 14).strftime('%Y-%m-%d')
    schema = StructType([
        StructField("date", StringType()),
        StructField("business_id", StringType())
    ])

    checkin_df   = spark.createDataFrame(data=[], schema=schema)

    review_data = [(run_date, "fake_business_id")]
    reviews_df   = spark.createDataFrame(data=review_data, schema=schema)
    m_reviews_df = spark.createDataFrame(data=review_data, schema=schema)

    reviews_df = count_reviews(checkin_df, m_reviews_df, reviews_df, run_date)
    reviews_df = reviews_df.withColumn("expected_num_reviews", F.lit(2))
    assert_column_equality(reviews_df, "num_reviews", "expected_num_reviews")


def create_df_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["business_id"])
    return output


def read_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

import json
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, DateType, StringType

from pandemic_recovery_batch import transform


def test_will_count_reviews_without_matching_checkins(spark: SparkSession) -> None:
    run_date = datetime(2022, 4, 14).strftime('%Y-%m-%d')

    enrichment_columns = StructType([
         StructField("date", StringType()),
         StructField("business_id", StringType())
    ])
    review_data = [(run_date, "fake_business_id")]

    business_df  = setup_business_df(spark)

    checkin_df   = setup_unused_df(enrichment_columns, spark)
    tips_df      = setup_unused_df(enrichment_columns, spark)

    reviews_df   = setup_review_df(enrichment_columns, review_data, spark)
    m_reviews_df = setup_review_df(enrichment_columns, review_data, spark)


    actual_df = transform(
        business_df,
        checkin_df,
        reviews_df,
        tips_df,
        m_reviews_df,
        run_date
    )

    actual_json = data_frame_to_json(actual_df)
    assert actual_json[0]["num_reviews"] == 2


def setup_review_df(enrichment_columns, review_data, spark):
    return spark.createDataFrame(data=review_data, schema=enrichment_columns)

def setup_unused_df(enrichment_columns, spark):
    return spark.createDataFrame(data=[], schema=enrichment_columns)

def setup_business_df(spark):
    base_business_info = [("fake_business_id", "fake_business")]
    important_business_columns = StructType([
        StructField("business_id", StringType()),
        StructField("name", StringType())
    ])
    return spark.createDataFrame(data=base_business_info, schema=important_business_columns)


def create_df_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["business_id"])
    return output


def read_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

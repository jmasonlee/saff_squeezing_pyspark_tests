import json
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F

from pandemic_recovery_batch import count_reviews, count_checkins, count_tips


def test_will_count_reviews_without_matching_checkins(spark: SparkSession) -> None:
    reviews_df = create_df_from_json("fixtures/reviews.json", spark)
    checkin_df = create_df_from_json("fixtures/checkin.json", spark)
    mobile_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)

    checkin_df = checkin_df.withColumn("checkins_list", F.split(checkin_df.date, ","))
    checkin_df = checkin_df.select(F.col("business_id"), F.explode(F.col("checkins_list")).alias("date"))

    reviews_df = count_reviews(checkin_df, mobile_reviews_df, reviews_df)

    actual_json = data_frame_to_json(reviews_df)
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

import json
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F

from pandemic_recovery_batch import transform, count_reviews, count_checkins, \
    count_tips


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

def test_keeps_mobile_reviews_without_matching_checkins(spark: SparkSession) -> None:
    b_reviews_df   = create_df_from_json("fixtures/browser_reviews.json", spark)
    checkin_df   = create_df_from_json("fixtures/checkin.json", spark)
    tips_df      = create_df_from_json("fixtures/tips.json", spark)
    business_df  = create_df_from_json("fixtures/business.json", spark)
    m_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)

    date = datetime(2022, 4, 14)
    checkin_df = checkin_df.withColumn("checkins_list", F.split(checkin_df.date, ","))
    checkin_df = checkin_df.withColumn("date", F.explode(checkin_df.checkins_list))
    checkin_df = checkin_df.withColumn("date", F.trim(checkin_df.date))
    checkin_df = checkin_df.select(
        F.col("business_id"),
        F.col("user_id"),
        F.col("date")
    )
    reviews_df = count_reviews(checkin_df, m_reviews_df, b_reviews_df, date)
    checkin_df = count_checkins(checkin_df, date)
    tips_df = count_tips(tips_df, date)
    pandemic_recovery_df = business_df.join(checkin_df, on="business_id", how='left').fillna(0)
    pandemic_recovery_df = pandemic_recovery_df.join(reviews_df, on="business_id", how='left').fillna(0)
    pandemic_recovery_df = pandemic_recovery_df.join(tips_df, on="business_id", how='left').fillna(0)
    pandemic_recovery_df = pandemic_recovery_df.withColumn("num_interactions",
                                                           pandemic_recovery_df.num_reviews +
                                                           pandemic_recovery_df.num_tips +
                                                           pandemic_recovery_df.num_checkins)
    business_with_mobile_review_only = data_frame_to_json(pandemic_recovery_df)[2]
    assert business_with_mobile_review_only["num_reviews"] == 1


def create_df_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["business_id"])
    return output


def expected_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

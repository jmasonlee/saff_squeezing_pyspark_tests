from datetime import datetime

from pyspark.sql import SparkSession

from pandemic_recovery_batch import construct_post_pandemic_recovery_df, \
    create_checkin_df_with_one_date_per_row, count_reviews, count_checkins, count_tips
from test.saff_squeeze_start_point import create_df_from_json, data_frame_to_json


def test_keeps_mobile_reviews_without_checkins(spark: SparkSession) -> None:
    b_reviews_df = create_df_from_json("fixtures/browser_reviews.json", spark)
    checkin_df   = create_df_from_json("fixtures/checkin.json", spark)
    tips_df      = create_df_from_json("fixtures/tips.json", spark)
    business_df  = create_df_from_json("fixtures/business.json", spark)
    m_reviews_df = create_df_from_json("fixtures/mobile_reviews.json", spark)

    date = datetime(2022, 4, 14)
    checkin_df = create_checkin_df_with_one_date_per_row(checkin_df)
    reviews_df = count_reviews(checkin_df, m_reviews_df, b_reviews_df, date)
    checkin_df = count_checkins(checkin_df, date)
    tips_df = count_tips(tips_df, date)
    actual_df = construct_post_pandemic_recovery_df(
        business_df, checkin_df, reviews_df, date, tips_df
    )

    inglewood_pizza = data_frame_to_json(actual_df)[6]
    assert inglewood_pizza["name"] == "Inglewood Pizza"
    assert inglewood_pizza["num_reviews"] == 1


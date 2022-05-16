from datetime import datetime

from pyspark.sql import SparkSession

from pandemic_recovery_batch import transform
from test.saff_squeeze_start_point import create_df_from_json, read_json, data_frame_to_json


def test_only_counts_mobile_reviews_done_separately_from_checkins(spark: SparkSession) -> None:
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
    #overwrite_expected(actual_df)
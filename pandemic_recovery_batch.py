from datetime import datetime
from typing import List

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def transform(business_df: DataFrame,
              checkin_df: DataFrame,
              browser_reviews_df: DataFrame,
              tips_df: DataFrame,
              mobile_reviews_df: DataFrame,
              run_date: datetime = datetime.today()):

    checkin_df = checkin_df.withColumn("checkins_list", F.split(checkin_df.date, ","))
    checkin_df = checkin_df.select(F.col("business_id"), F.explode(F.col("checkins_list")).alias("date"))

    reviews_df = count_reviews(checkin_df, mobile_reviews_df, browser_reviews_df, run_date)
    checkin_df = count_checkins(checkin_df, run_date)

    tips_df = count_tips(tips_df, run_date)

    return construct_post_pandemic_recovery_df(
        business_df, checkin_df, reviews_df, run_date, tips_df
    )


def construct_post_pandemic_recovery_df(business_df, checkin_df, reviews_df, run_date, tips_df):
    entity_with_activity_df = business_df.join(checkin_df, on="business_id", how='left').fillna(0)
    entity_with_activity_df = entity_with_activity_df.join(reviews_df, on="business_id", how='left').fillna(0)
    entity_with_activity_df = entity_with_activity_df.join(tips_df, on="business_id", how='left').fillna(0)
    entity_with_activity_df = entity_with_activity_df.withColumn("num_interactions",
                                                                 entity_with_activity_df.num_reviews +
                                                                 entity_with_activity_df.num_tips +
                                                                 entity_with_activity_df.num_checkins)
    entity_with_activity_df = entity_with_activity_df.withColumn("dt", F.lit(run_date))
    entity_with_activity_df = entity_with_activity_df.select(
        "business_id", "name", "num_tips", "num_checkins", "num_reviews", "num_interactions", "dt"
    )
    return entity_with_activity_df


def count_tips(tips_df, run_date):
    tips_df = tips_df.filter(tips_df.date == run_date.date())
    tips_df = tips_df.groupby("business_id").count()
    tips_df = tips_df.withColumnRenamed("count", "num_tips")
    return tips_df


def count_checkins(checkin_df, run_date):
    checkin_df = checkin_df.filter(checkin_df.date == run_date.date())
    checkin_df = checkin_df.groupby("business_id").count()
    checkin_df = checkin_df.withColumnRenamed("count", "num_checkins")
    return checkin_df


def count_reviews(checkin_df, mobile_reviews_df, browser_reviews_df, run_date):
    mobile_reviews_df = mobile_reviews_df.join(checkin_df, on=['business_id'], how="left_anti").select(mobile_reviews_df.columns)
    reviews_df = mobile_reviews_df.union(browser_reviews_df)
    reviews_df = reviews_df.filter(reviews_df.date == run_date.date())
    reviews_df = reviews_df.groupby("business_id").count()
    reviews_df = reviews_df.withColumnRenamed("count", "num_reviews")
    return reviews_df



from datetime import datetime
from typing import List

from pyspark.sql import functions as F, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def count_dates_since_date(dates: List[str], recent_limit: datetime) -> int:
    return len(list(filter(lambda date: is_after_date(date, recent_limit), dates)))


def is_after_date(date_to_check: str, limiting_date: datetime) -> bool:
    date_to_check = date_to_check.lstrip()
    date_to_check = datetime.strptime(date_to_check, '%Y-%m-%d %H:%M:%S')
    return date_to_check.year > limiting_date.year


def transform(business_df: DataFrame,
              checkin_df: DataFrame,
              reviews_df: DataFrame,
              tips_df: DataFrame,
              mobile_reviews_df: DataFrame,
              run_date: str = datetime.today().strftime('%Y-%m-%d')):

    reviews_df = mobile_reviews_df.join(checkin_df, on=['business_id', 'date']).select(reviews_df.columns)
    reviews_df = reviews_df.union(reviews_df)
    reviews_df = reviews_df.filter(reviews_df.date > datetime(2020, 12, 31))
    reviews_df = reviews_df.groupby("business_id").count()
    reviews_df = reviews_df.withColumnRenamed("count", "num_reviews")

    count_recent_dates_udf = udf(lambda dates: count_dates_since_date(dates, datetime(2020, 12, 31)), IntegerType())
    checkin_df = checkin_df.withColumn("checkins_list", F.split(checkin_df.date, ","))
    checkin_df = checkin_df.withColumn("num_checkins", count_recent_dates_udf(F.col("checkins_list")))
    checkin_df = checkin_df.drop("date", "checkins_list")

    tips_df = tips_df.filter(tips_df.date > datetime(2020, 12, 31))
    tips_df = tips_df.groupby("business_id").count()
    tips_df = tips_df.withColumnRenamed("count", "num_tips")

    entity_with_activity_df = business_df.join(checkin_df, on="business_id", how='left').fillna(0)
    entity_with_activity_df = entity_with_activity_df.join(reviews_df, on="business_id", how='left').fillna(0)
    entity_with_activity_df = entity_with_activity_df.join(tips_df, on="business_id", how='left').fillna(0)
    entity_with_activity_df = entity_with_activity_df.withColumn("num_interactions",
                                                                 entity_with_activity_df.num_reviews +
                                                                 entity_with_activity_df.num_tips +
                                                                 entity_with_activity_df.num_checkins)
    entity_with_activity_df = entity_with_activity_df.withColumn("dt", F.lit(run_date))

    return entity_with_activity_df

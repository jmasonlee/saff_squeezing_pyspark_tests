import datetime

from chispa import assert_df_equality
from pyspark import SparkContext
from pyspark_dataframe_wrappers.test_dataframe import TestDataFrame

from pandemic_recovery_batch import count_checkins


def test_will_only_count_checkins_from_rundate(spark: SparkContext) -> None:
    today = datetime.datetime(2001, 1, 1)
    yesterday = today - datetime.timedelta(days=1)

    checkins_df = TestDataFrame(spark).create_test_dataframe_from_table(
        f"""
        date        | business_id
        {today}     | 1
        {today}     | 1
        {yesterday} | 1
        """
    ).create_spark_df()

    actual_df = count_checkins(checkins_df, run_date=today)
    expected_df = TestDataFrame(spark).create_test_dataframe_from_table(
        """
        business_id | num_checkins
        1           | 2
        """
    ).create_spark_df()
    assert_df_equality(expected_df, actual_df, ignore_nullable=True)
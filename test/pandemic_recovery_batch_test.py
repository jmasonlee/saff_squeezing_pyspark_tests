import datetime

from chispa import assert_df_equality
from pyspark import SparkContext
from pyspark_dataframe_wrappers.test_dataframe import TestDataFrame

from pandemic_recovery_batch import count_checkins


def test_will_only_count_checkins_from_rundate(spark: SparkContext) -> None:
    run_date = datetime.datetime(2001, 1, 1)
    one_day = datetime.timedelta(days=1)
    business_id = "business_1"
    checkins_df = TestDataFrame(spark).create_test_dataframe_from_table(
        f"""
        date                    | business_id
        {run_date}              | {business_id}
        {run_date}              | {business_id}
        {run_date - one_day}    | {business_id}
        """
    ).create_spark_df()

    actual_df = count_checkins(checkins_df, run_date)
    expected_df = TestDataFrame(spark).create_test_dataframe_from_table(
        f"""
        business_id     | num_checkins
        {business_id}   | 2
        """
    ).create_spark_df()
    assert_df_equality(expected_df, actual_df, ignore_nullable=True)
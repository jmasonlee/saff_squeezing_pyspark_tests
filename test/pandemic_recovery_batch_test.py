import datetime

import pyspark.sql.types
from chispa import assert_df_equality
from pyspark import SparkContext
from pyspark_dataframe_wrappers import TestDataFrame

from pandemic_recovery_batch import count_checkins


def test_will_only_count_checkins_from_rundate(spark: SparkContext) -> None:
    run_date = datetime.datetime(2001, 1, 1)
    one_day = datetime.timedelta(days=1)
    business_id = "business_1"
    checkins_df = TestDataFrame(spark).with_base_data(business_id=business_id).create_test_dataframe_from_table(
        f"""
        date
        {run_date}            
        {run_date}            
        {run_date - one_day}  
        """
    ).create_spark_df()


    actual_df = count_checkins(checkins_df, run_date)
    expected_df = (
        TestDataFrame(spark)
            .with_base_data(business_id=business_id)
            .with_explicit_schema()
            .with_nullable_data([{"?num_checkins": 2}])
            #.with_nullable_data([{"num_checkins": 2}, {"num_checkins": None}])
            .with_data([{"Optional(num_checkins)": 2, "column2": "foo"}])
            .with_data([{"num_checkins": 2}], nullable=["num_checkins"])
            .with_data([{"num_checkins": 2, "column2": "foo"}], nullable=["num_checkins"])
            .with_data([{"num_checkins": 2}], nullable=True)
            .with_data([{"num_checkins": 2?}])
            .with_data([{"num_checkins": Some(2)}])
            .with_data([{Optional("num_checkins"): 2}])
            .with_data([{Wrapper("num_checkins"): 2}])
            .with_data([{Nullable_String("num_checkins"): 2}])
            .with_data([{Column(name="num_checkins", nullable=True, type=pyspark.sql.types.NumericType): 2}])
            .with_data([
                {Numeric(name="num_checkins", nullable=True): 2},
                {Numeric(name="num_checkins", nullable=True): 5}
            ])
            .with_column(Numeric(name="num_checkins", nullable=True),[2,5])
            .create_spark_df()
    )
    assert_df_equality(expected_df, actual_df)

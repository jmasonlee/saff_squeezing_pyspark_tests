import datetime

import pytest
from chispa import assert_df_equality
from pyspark.sql.types import *

from pandemic_recovery_batch import construct_post_pandemic_recovery_df
from test.test_dataframe import create_empty_df


# Informative error message when someone tries to build an empty dataframe with schema and without types
# Allow not nullable fields in empty dfs
@pytest.mark.skip("Blocked by error not allowing us to build empty dfs without nullable fields")
def test_construct_post_pandemic_recovery_df(spark):
    empty_df = create_empty_df(spark)
    business_df = create_empty_df(spark,
                                  StructType(
                                      [
                                          StructField('business_id', StringType())
                                      ])
                                  )
    all_df = create_empty_df(spark,
                             StructType(
                                 [
                                     StructField('business_id', StringType()),
                                     StructField('name', StringType()),
                                     StructField('num_reviews', IntegerType()),
                                     StructField('num_tips', IntegerType()),
                                     StructField('num_checkins', IntegerType()),
                                 ])
                             )
    output_df = construct_post_pandemic_recovery_df(all_df, business_df, business_df, datetime.date.today(),
                                                    business_df)

    expected_df = create_empty_df(spark,
                                  StructType(
                                      [
                                          StructField('business_id', StringType()),
                                          StructField('name', StringType()),
                                          StructField('num_tips', IntegerType()),
                                          StructField('num_checkins', IntegerType()),
                                          StructField('num_reviews', IntegerType()),
                                          StructField('num_interactions', IntegerType()),
                                          StructField('dt', StringType(), False)
                                      ]
                                  ))

    assert_df_equality(expected_df, output_df)


def test_can_create_an_empty_df_with_a_non_nullable_field(spark):
    create_empty_df(spark, StructType([ StructField('_', StringType(), False)]))


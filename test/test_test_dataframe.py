from collections import namedtuple
from typing import List, Dict, Any

import pytest
from chispa import assert_df_equality
from pyspark.sql.types import DateType, IntegerType, StructType, StructField, StringType

from test.test_dataframe import TestDataFrame, Field


# Bug: createDataFrame returns a new dataframe without the base_data or schema of the parent dataframe
# We have no tests for exception handling
# createDataFrame shouldn't return TestDataFrame, it should return a dataframe


# want to create a dataframe specifying only the "category 1" data with "category 2" data set as default values
# we need to handle columns in category 2 that are required to be unique

# category 1: data we care about/that is being tested
# category 2: data we need but don't care about
# category 3: data we do not want/need


def test_create_test_dataframe(spark):
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")
    test_df = base_data \
        .create_test_dataframe(date=[
        "2000-01-02 03:04:05",
        "2000-01-01 04:05:06"
    ]) \
        .create_spark_df()

    df_actual = spark.createDataFrame([
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-02 03:04:05"},
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 04:05:06"}
    ])

    assert_df_equality(test_df, df_actual, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)


def test_add_column_to_schema(spark):
    test_df = TestDataFrame(spark).set_type_for_column("name", StringType())
    # make sure this includes a name column of type String
    assert test_df.explicit_schema.fields[0].name == "name"
    assert test_df.explicit_schema.fields[0].dataType == StringType()


@pytest.mark.skip()
def test_multiple_columns(spark):
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")

    test_df = (base_data
               .create_test_dataframe_from_table(
                    """
                    | date                | stars |
                    | 2000-01-02 03:04:05 | 5     |
                    | 2000-01-01 04:05:06 | 3     |
                    """
               ))

    df_actual = spark.createDataFrame([
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-02 03:04:05", "stars": 5},
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 04:05:06", "stars": 3}
    ])

    assert_df_equality(test_df.create_spark_df(), df_actual, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)

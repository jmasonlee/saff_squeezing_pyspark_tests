import pytest
from chispa import assert_df_equality
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

from test.test_dataframe import TestDataFrame, df_from_string


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
        | 2000-01-01 05:06:07 | 4     |
        """
    ))

    df_actual = spark.createDataFrame([
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-02 03:04:05", "stars": 5},
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 04:05:06", "stars": 3},
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 05:06:07", "stars": 4}
    ])

    assert_df_equality(test_df.create_spark_df(), df_actual, ignore_nullable=True, ignore_column_order=True,
                       ignore_row_order=True)


def test_dataframe_from_string(spark):
    # I want a dataframe from a new method that we haven't made up yet that takes in a string

    new_df = df_from_string(spark,
        """
            date                | stars
            2000-01-02 03:04:05 | 5
            2000-01-01 04:05:06 | 3
            2000-01-01 05:06:07 | 4
        """
                            )

    expected_df = spark.createDataFrame(
        schema = StructType(
            [
                StructField("date", StringType()),
                StructField("stars", IntegerType()),
            ]
        ),
        data=[
            {"date": "2000-01-02 03:04:05", "stars": 5},
            {"date": "2000-01-01 04:05:06", "stars": 3},
            {"date": "2000-01-01 05:06:07", "stars": 4}
        ]
    )
    expected_df = expected_df.withColumn("date", to_timestamp(expected_df.date))
    assert_df_equality(new_df, expected_df)

import functools

# Balazs idea
from datetime import datetime

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from pandemic_recovery_batch import count_interactions_from_reviews


class TestDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.data = [{}]
        self.schema = None
        self.base_data = {}

    def __enter__(self):
        return self.create_df()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def with_base_data(self, **kwargs) -> "TestDataFrame":
        self.base_data = kwargs
        return self

    def set_type_for_column(self, column: str, type: DataType.__class__) -> "TestDataFrame":
        return self

    def with_schema_from(self, reference_df) -> "TestDataFrame":
        self.schema = reference_df.schema
        return self

    def create_df(self):
        return self.spark.createDataFrame(schema=self.schema, data=self.data)

    def with_data(self, rows: list[dict]) -> "TestDataFrame":
        self.data = rows
        return self

    def create_test_dataframe(self, **kwargs):
        column_name = list(kwargs.keys())[0]
        column_values = kwargs[column_name]

        new_rows = []
        for row_from_column in column_values:
            new_rows.append(self.base_data | {column_name: row_from_column})

        new_test_dataframe = TestDataFrame(self.spark)
        return new_test_dataframe.with_data(new_rows)

    def create_test_dataframe_from_table(self, table) -> DataFrame:
        date = "date"
        stars = "stars"
        date_data = ["2000-01-02 03:04:05", "2000-01-01 04:05:06"]
        stars_data = [5, 3]
        columns_data = ["date", "stars"]
        col1 = columns_data[0]
        col2 = columns_data[1]
        rows = [self.base_data | {col1: date_data[i], col2: stars_data[i]} for i in range(len(date_data))]

        return self.spark.createDataFrame(rows)


class EmptyDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.schema = []

    def __enter__(self):
        return self.spark.createDataFrame(schema=self.schema, data=[{}])

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def with_schema_from(self, reference_df):
        self.schema = reference_df.schema
        return self

def test_context(spark):
    mobile_review_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    with TestDataFrame(spark).with_schema_from(mobile_review_df) as __:
        reviews_df = count_interactions_from_reviews(__, mobile_review_df, __, datetime(2022, 4, 14))
        assert reviews_df.count() == 1

def test_composition(spark):
    mobile_review_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    __ = TestDataFrame(spark).with_schema_from(mobile_review_df).create_df()
    reviews_df = count_interactions_from_reviews(__, mobile_review_df, __, datetime(2022, 4, 14))
    assert reviews_df.count() == 1
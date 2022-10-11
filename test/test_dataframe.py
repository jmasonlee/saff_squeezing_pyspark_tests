import functools

# Balazs idea
from datetime import datetime

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from pandemic_recovery_batch import count_interactions_from_reviews


class Field:
    def __init__(self, name: str, type: DataType):
        self.name = name
        self.type = type


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
        #col_headers = ["date", "stars"]
        date_data = ["2000-01-02 03:04:05", "2000-01-01 04:05:06"]
        stars_data = [5, 3]

        col_headers = ["date", "stars"]
        original_rows = [
            ("2000-01-02 03:04:05", 5),
            ("2000-01-01 04:05:06", 3)
        ]

        base_values = list(self.base_data.values())

        rows = []
        for i in range(len(date_data)):
            row = base_values.extend(original_rows[i])
            rows.append(row)
            # rows.append(base_values | {col_headers[0]: date_data[i], col_headers[1]: stars_data[i]})

        # join self.schema with type
        return self.spark.createDataFrame(data=rows, schema=self.schema)


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
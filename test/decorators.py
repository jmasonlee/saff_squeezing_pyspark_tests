import functools

# Balazs idea
from datetime import datetime

import pytest

from pandemic_recovery_batch import count_interactions_from_reviews


class TestDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.schema = []

    def __enter__(self):
        return self.create_df()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def with_schema_from(self, reference_df):
        self.schema = reference_df.schema
        return self

    def create_df(self):
        return self.spark.createDataFrame(schema=self.schema, data=[{}])


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


#
def add_empty_dfs(func, *args, **kwargs):
    def inner(applesauce):
        @functools.wraps(func)
        def wrapper_do_twice(*args, **kwargs):
            func(*args, **kwargs)
            return func(*args, **kwargs)
        return wrapper_do_twice
    return inner

@pytest.mark.skip()
def test_decorator(spark):
    @add_empty_dfs(applesauce='important_df')
    def function_under_test(important_df, empty_df):
        assert important_df.columns == empty_df.columns
        assert empty_df.count() == 0

    important_df = spark.createDataFrame(data=[{'key': 'value'}])
    empty_df = spark.createDataFrame(data=[{}])
    function_under_test(important_df=important_df, empty_df=empty_df)

#Balazs idea

#with empty_dataframe_form(important_df) as empy:
#    transform(...)
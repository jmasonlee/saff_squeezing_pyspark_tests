from typing import List, Dict, Any

import pytest
from chispa import assert_df_equality

from test.test_dataframe import TestDataFrame

# Bug: createDataFrame returns a new dataframe without the base_data or schema of the parent dataframe

def test_create_test_dataframe(spark):
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")
    test_df = base_data \
        .create_test_dataframe(date=[
        "2000-01-02 03:04:05",
        "2000-01-01 04:05:06"
    ]) \
        .create_df()

    df_actual = spark.createDataFrame([
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-02 03:04:05"},
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 04:05:06"}
    ])

    assert_df_equality(test_df, df_actual, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)


@pytest.mark.skip
def test_multiple_columns(spark):
    base_data = TestDataFrame(spark).with_base_data(user_id="Scooby-Doo", business_id="Crusty Crab")
    test_df = base_data \
        .create_test_dataframe(date=[
        "2000-01-02 03:04:05",
        "2000-01-01 04:05:06"
    ]) \
        .create_df()

    df_actual = spark.createDataFrame([
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-02 03:04:05"},
        {"user_id": "Scooby-Doo", "business_id": "Crusty Crab", "date": "2000-01-01 04:05:06"}
    ])

    assert_df_equality(test_df, df_actual, ignore_nullable=True, ignore_column_order=True, ignore_row_order=True)


def applesauce(**kwargs) -> List[Dict]:
    for item_key, item_value in kwargs.items():
        key_array = [item_key] * len(item_value)
        print(list(zip(key_array, item_value)))

    # TODO for next time:
    # make a dict out of zipped (key_array, item_value) pairs
            #, lambda key, value: (key, value)

    # [dict(zip(('x', 'y'), col)) for col in zip(d['x'], d['y'])]
    # zip(('k1', 'k2'), ('v1', 'v2'))
    # [(k1, v1), (k2, v2)]
    # [(k1, v1), (k2, v2)]
    return [dict(zip(('k1', 'k2'), col)) for col in zip(kwargs['k1'], kwargs['k2'])]

    # dict1 = dict({'k1': 'v1'}, **{'k2': 'v3'})
    # dict2 = {'k1': 'v2', 'k2': 'v4'}
    # return [dict1, dict2]


def test_applesauce():
    actual_applesauce = applesauce(k1=['v1', 'v2'], k2=['v3', 'v4'])
    expected_applesauce = [{'k1': 'v1', 'k2':'v3'}, {'k1': 'v2', 'k2': 'v4'}]
    assert actual_applesauce == expected_applesauce

    # additional_columns = {k1: [v1, v2], k2: [v3, v4]}
    # 1st iteration select: v1, v3
    # 2nd iteration select: v2, v4
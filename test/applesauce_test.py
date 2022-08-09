from datetime import datetime


from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, LongType

from pandemic_recovery_batch import count_interactions_from_reviews


def create_df(spark, schema, data):
    return spark.createDataFrame(schema=schema, data=data)


SCHEMA = StructType([
            StructField('user_id', StringType()),
            StructField('business_id', StringType()),
            StructField('date', StringType())])

SCHEMA2 = StructType([
            StructField('business_id', StringType()),
            StructField('num_reviews', LongType(), False)])

def test_keeps_mobile_reviews_without_checkins(spark):
    mobile_df = create_df(spark, SCHEMA, [{'user_id': 'uid', 'business_id': 'bid', 'date': '2022-04-14'}])
    __ = create_df(spark, SCHEMA, [])

    reviews_df = count_interactions_from_reviews(__, mobile_df, __, datetime(2022, 4, 14))

    expected_df = create_df(spark, SCHEMA2, [{ 'business_id': 'bid', 'num_reviews': 1}])
    assert_df_equality(reviews_df, expected_df)

def test_does_not_count_mobile_reviews_with_checkins(spark):
    mobile_df = create_df(spark, SCHEMA, [{ 'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    checkin_df = create_df(spark, SCHEMA, [{'user_id': 'uid', 'business_id': 'bid', 'date': '2022-04-14'}])
    __ = create_df(spark, SCHEMA, [])

    reviews_df = count_interactions_from_reviews(checkin_df, mobile_df, __, datetime(2022, 4, 14))

    expected_df = create_df(spark, SCHEMA2, [])
    assert_df_equality(reviews_df, expected_df)
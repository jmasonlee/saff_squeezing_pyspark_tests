from datetime import datetime


from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

from pandemic_recovery_batch import count_interactions_from_reviews


def create_df(spark, schema, data):
    return spark.createDataFrame(schema=schema, data=data)


SCHEMA = StructType([
            StructField('user_id', StringType()),
            StructField('business_id', StringType()),
            StructField('date', StringType()),
            StructField('useful', IntegerType())
])

SCHEMA2 = StructType([
            StructField('business_id', StringType()),
            StructField('num_reviews', LongType(), False)])

def test_keeps_mobile_reviews_without_checkins(spark):
    mobile_df = create_df(spark, SCHEMA, [{'user_id': 'uid', 'business_id': 'bid', 'date': '2022-04-14'}])
    __ = create_df(spark, SCHEMA, [])

    reviews_df = count_interactions_from_reviews(__, mobile_df, __, datetime(2022, 4, 14))

    expected_df = create_df(spark, SCHEMA2, [{ 'business_id': 'bid', 'num_reviews': 1}])
    # Add a column name 'expected_num_reviews' to the reviews dataframe where every value is 1

    #reviews_df = reviews_df.withColumn('expected_num_reviews', F.lit(1))
    #assert_column_equality(reviews_df, 'num_reviews', 'expected_num_reviews')

    #What if we have multiple lines and multiple executors or cores and the rows or columns get shuffled

    #We want the resulting dataframe to have exactly one row
    #We want the row with business id bid to have num_reviews as 1



# create a dataframe that only specifies the information important to my test
# 1. just specify data
# 2. just specify schema
# 3. create an empty dataframe without data/schema that will "just work"

# function(dataframe ... other args) -> dataframe (this is a "transformation")
# these dataframes go into a function
# they come out as

EMPTY = spark.createDataFrame()


def df_has_rows(df, rows):
    return all(any(row.items() <= df_row.asDict().items() for df_row in df.collect()) for row in rows)

def test_keeps_mobile_reviews_without_checkins(spark):
    mobile_review_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    __ = spark.createDataFrame(schema=mobile_review_df.schema, data=[])

    reviews_df = count_interactions_from_reviews(__, mobile_review_df, __, datetime(2022, 4, 14))

    assert reviews_df.count() == 1
    assert df_has_rows(reviews_df, [{'business_id': 'bid', 'num_reviews': 1}])

def test_count_reviews_schema(spark):
    important_input_columns = StructType([
        StructField('user_id', StringType()),
        StructField('business_id', StringType()),
        StructField('date', StringType())
    ])
    mobile_df = create_df(spark, important_input_columns, [])
    browser_df = create_df(spark, important_input_columns, [])
    checkin_df = create_df(spark, important_input_columns, [])

    reviews_df = count_interactions_from_reviews(checkin_df, mobile_df, browser_df, datetime(2022, 4, 14))

    expected_output_schema = StructType([
        StructField('business_id', StringType()),
        StructField('num_reviews', LongType(), False)])
    expected_df = create_df(spark, expected_output_schema, [])
    assert_df_equality(reviews_df, expected_df)


def test_does_not_count_mobile_reviews_with_checkins(spark):
    mobile_review_df = spark.createDataFrame(data=[{ 'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    checkin_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    __ = spark.createDataFrame(schema=StructType([]), data=[])

    reviews_df = count_interactions_from_reviews(checkin_df, mobile_review_df, __, datetime(2022, 4, 14))

    assert reviews_df.count() == 0

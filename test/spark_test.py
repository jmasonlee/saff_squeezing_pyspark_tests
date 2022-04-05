import json
from datetime import datetime
from functools import reduce
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def count_dates_since_date(dates: List[str], recent_limit: datetime) -> int:
    dates = convert_strings_to_dates(dates)
    return len(list(filter(lambda date: date.year > recent_limit.year, dates)))


def convert_strings_to_dates(dates: List[str]) -> List[datetime]:
    dates = map(lambda date_string: date_string.lstrip(), dates)
    dates = map(lambda string: datetime.strptime(string, '%Y-%m-%d %H:%M:%S'), dates)
    return dates


def test_will_do_the_right_thing(spark):
    reviews_df = create_df_from_json("fixtures/reviews.json", spark)
    checkin_df = create_df_from_json("fixtures/checkin.json", spark)
    # Import data on tips
    # Import data on users- New events source
    # Import data on businesses
    business_df = create_df_from_json("fixtures/business.json", spark)

    # Read each review event
    # Join it with business info
    # Join it with user info
    # Was there a recent, corresponding tip or checkin - extra method
    count_recent_dates_udf = udf(lambda dates: count_dates_since_date(dates, datetime(2020, 12, 31)), IntegerType())
    checkin_df = checkin_df.withColumn("checkins_list", F.split(checkin_df.date, ","))
    checkin_df = checkin_df.withColumn("recent_checkin_count", count_recent_dates_udf(F.col("checkins_list")))
    checkin_df = checkin_df.drop("date", "checkins_list")

    reviews_df = reviews_df.filter()
    reviews_df = reviews_df.groupby("business_id").count()

    entity_with_activity_df = business_df.join(checkin_df, on="business_id")
    entity_with_activity_df = entity_with_activity_df.join(reviews_df, on="business_id")
    # Reformat to nest all columns except review or checkin ID
    # Output JSON

    # Check output JSON against expected
    expected_json = read_json()
    assert data_frame_to_json(entity_with_activity_df) == expected_json


def create_df_from_json(json_file, spark):
    return create_data_frame_from_json(json_file, spark)


def create_data_frame_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["business_id"])
    return output


def read_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

import json
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def count_dates_since_date(dates: List[str], recent_limit: datetime) -> int:
    return len(list(filter(lambda date: is_after_date(date, recent_limit), dates)))


def is_after_date(date_to_check: str, limiting_date: datetime) -> bool:
    date_to_check = date_to_check.lstrip()
    date_to_check = datetime.strptime(date_to_check, '%Y-%m-%d %H:%M:%S')
    return date_to_check.year > limiting_date.year


def test_will_do_the_right_thing(spark):
    reviews_df = create_df_from_json("fixtures/reviews.json", spark)
    checkin_df = create_df_from_json("fixtures/checkin.json", spark)
    # Import data on tips
    business_df = create_df_from_json("fixtures/business.json", spark)

    # Read each review event
    # Join it with business info
    # Join it with user info
    # Was there a recent, corresponding tip or checkin - extra method
    count_recent_dates_udf = udf(lambda dates: count_dates_since_date(dates, datetime(2020, 12, 31)), IntegerType())
    checkin_df = checkin_df.withColumn("checkins_list", F.split(checkin_df.date, ","))
    checkin_df = checkin_df.withColumn("num_checkins", count_recent_dates_udf(F.col("checkins_list")))
    checkin_df = checkin_df.drop("date", "checkins_list")

    reviews_df = reviews_df.filter(reviews_df.date > datetime(2020, 12, 31))
    reviews_df = reviews_df.groupby("business_id").count()
    reviews_df = reviews_df.withColumnRenamed("count", "num_reviews")

    entity_with_activity_df = business_df.join(checkin_df, on="business_id", )
    entity_with_activity_df = entity_with_activity_df.join(reviews_df, on="business_id")
    # Reformat to nest all columns except review or checkin ID
    # Output JSON

    # Check output JSON against expected
    # expected_json = read_json()
    # assert data_frame_to_json(entity_with_activity_df) == expected_json
    with open("fixtures/expected.json", "w") as f:
        jsons = ''.join(
            json.dumps(line) if line else line
            for line in data_frame_to_json(entity_with_activity_df)
        )

        f.write(jsons)



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

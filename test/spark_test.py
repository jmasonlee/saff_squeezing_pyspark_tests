import json
from typing import List

from pyspark.sql import DataFrame


def test_will_do_the_right_thing(spark):
    df = create_df_from_json("fixtures/reviews.json", spark)
    #Import data on checkin events
    checkin_df = create_df_from_json("fixtures/checkin.json", spark)
    #Import data on tips - New events source
    #Import data on users
    #Import data on businesses

    # Read each review event
    # Join it with business info
    # Join it with user info
    # Was there a recent, corresponding tip or checkin - extra method
    df = df.join(checkin_df, on="business_id")
    # Reformat to nest all columns except review or checkin ID
    # Output JSON

    # Check output JSON against expected
    expected_json = read_json()
    assert data_frame_to_json(df) == expected_json


def create_df_from_json(json_file, spark):
    return create_data_frame_from_json(json_file, spark)


def create_data_frame_from_json(json_file, spark):
    return spark.read.option("multiline", "true").json(json_file)


def data_frame_to_json(df: DataFrame) -> List:
    output = [json.loads(item) for item in df.toJSON().collect()]
    output.sort(key=lambda item: item["review_id"])
    return output

def read_json():
    with open("fixtures/expected.json") as f:
        return json.loads(f.read())

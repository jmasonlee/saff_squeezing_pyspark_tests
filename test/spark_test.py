import json
from typing import List

from pyspark.sql import DataFrame


def test_will_do_the_right_thing(spark):
    #Import data on review events
    df = spark.read.option("multiline","true").json("fixtures/reviews.json")
    #Import data on checkin events - New events source
    #Import data on tips
    #Import data on users
    #Import data on businesses

    # Concatenate each review or checkin event
    # Join it with business info
    # Join it with user info
    # Was there a corresponding tip - extra method
    # Reformat to nest all columns except review or checkin ID
    # Output JSON

    # Check output JSON against expected
    expected_json = read_json()
    assert data_frame_to_json(df) == expected_json

def data_frame_to_json(df: DataFrame) -> List:
    output = []
    for item in df.toJSON().collect():
        output.append(json.loads(item))
    return output

def read_json():
    with open("fixtures/reviews.json") as f:
        return json.loads(f.read())

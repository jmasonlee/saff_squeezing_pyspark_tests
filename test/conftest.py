import os
import sys

import pytest
from _pytest.fixtures import FixtureRequest
from pyspark import SparkConf
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request: FixtureRequest):
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    conf = (SparkConf().set("spark.default.parallelism", "1")
        .setMaster("local")
        .setAppName("sample_pyspark_testing_starter"))

    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    request.addfinalizer(lambda: spark.stop())
    return spark

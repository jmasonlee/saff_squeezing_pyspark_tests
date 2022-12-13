# Balazs idea
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DataType, StructType

from pandemic_recovery_batch import count_interactions_from_reviews


class Field:
    def __init__(self, name: str, type: DataType):
        self.name = name
        self.type = type

    def __eq__(self, other):
        return type(other) == Field and self.name == other.name and type(self.type) == type(other.type)


class TestDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.data = [{}]
        self.explicit_schema: Optional[StructType] = StructType([])
        self.fields = []
        self.base_data = {}

    def __enter__(self):
        return self.create_spark_df()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def with_base_data(self, **kwargs) -> "TestDataFrame":
        self.base_data = kwargs
        return self

    def set_type_for_column(self, column: str, type: DataType.__class__) -> "TestDataFrame":
        self.explicit_schema.add(column, type)
        return self

    def with_schema_from(self, reference_df: DataFrame) -> "TestDataFrame":
        self.explicit_schema = reference_df.schema
        return self

    def create_spark_df(self) -> DataFrame:
        spark_data = [self.base_data | row for row in self.data]
        dataframe = self.spark.createDataFrame(data=spark_data)

        if self.explicit_schema.fields:
            for column in self.explicit_schema.fields:
                dataframe = dataframe.withColumn(column.name, col(column.name).cast(column.dataType))

        return dataframe

    def with_data(self, rows: list[dict]) -> "TestDataFrame":
        self.data = rows
        return self

    def create_test_dataframe(self, **kwargs) -> "TestDataFrame":
        column_name = list(kwargs.keys())[0]
        column_values = kwargs[column_name]

        new_rows = []
        for row_from_column in column_values:
            new_rows.append({column_name: row_from_column})

        return self.with_data(new_rows)

    def create_test_dataframe_from_table(self, table) -> "TestDataFrame":
        table_df = self.df_from_string(table)

        self.data = [row.asDict() for row in table_df.collect()]
        return self

    def df_from_string(self, table):
        rows = table.strip().split('\n')
        rdd = self.spark.sparkContext.parallelize(rows)
        return self.spark.read.options(delimiter='|',
                                       header=True,
                                       ignoreLeadingWhiteSpace=True,
                                       ignoreTrailingWhiteSpace=True,
                                       inferSchema=True).csv(rdd)



class EmptyDataFrame:
    def __init__(self, spark):
        self.spark = spark
        self.schema = []

    def __enter__(self):
        return self.spark.createDataFrame(schema=self.schema, data=[{}])

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def with_schema_from(self, reference_df):
        self.schema = reference_df.explicit_schema
        return self

def test_context(spark):
    mobile_review_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    with TestDataFrame(spark).with_schema_from(mobile_review_df) as __:
        reviews_df = count_interactions_from_reviews(__, mobile_review_df, __, datetime(2022, 4, 14))
        assert reviews_df.count() == 1

def test_composition(spark):
    mobile_review_df = spark.createDataFrame(data=[{'business_id': 'bid', 'user_id': 'uid', 'date': '2022-04-14'}])
    __ = TestDataFrame(spark).with_schema_from(mobile_review_df).create_spark_df()
    reviews_df = count_interactions_from_reviews(__, mobile_review_df, __, datetime(2022, 4, 14))
    assert reviews_df.count() == 1

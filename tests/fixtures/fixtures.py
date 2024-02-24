import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    print("---- Setup Spark Session ---")
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("RepartiPy-Unit-Tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.port.maxRetries", "10")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    print("--- Tear down Spark Session ---")
    spark.stop()


@pytest.fixture
def input_data(spark):
    input_schema = StructType(
        [
            StructField("ID", IntegerType(), True),
            StructField("Location", StringType(), True),
            StructField("Date", StringType(), True),
        ]
    )
    input_data = [
        (1, "Seoul", "2024-01-01"),
        (2, "Daejeon", "2024-01-02"),
        (3, "Daegu", "2024-01-03"),
        (4, "Busan", "2024-01-04"),
        (5, "Gwanju", "2024-01-05"),
        (6, "Jeju", "2024-01-06"),
    ]
    input_df = spark.createDataFrame(data=input_data, schema=input_schema)
    input_df.coalesce(1)
    return input_df


@pytest.fixture
def empty_input_data(spark):
    input_schema = StructType(
        [
            StructField("ID", IntegerType(), True),
            StructField("Location", StringType(), True),
            StructField("Date", StringType(), True),
        ]
    )
    input_data = []
    input_df = spark.createDataFrame(data=input_data, schema=input_schema)
    return input_df

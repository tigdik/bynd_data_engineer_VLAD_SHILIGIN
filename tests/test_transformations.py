from pyspark.sql import SparkSession
from src.transformations import clean_column_names, remove_duplicates

def _make_spark():
    return (
        SparkSession.builder
        .appName("unit-test")
        .master("local[1]")
        .getOrCreate()
    )

def test_clean_column_names():
    spark = _make_spark()
    df = spark.createDataFrame(
        [("Alice", 1)],
        ["Full Name ", "Score-Card"]
    )
    out = clean_column_names(df)
    assert out.columns == ["full_name", "score_card"]

def test_remove_duplicates():
    spark = _make_spark()
    df = spark.createDataFrame(
        [("A",), ("A",), ("B",)],
        ["col"]
    )
    out = remove_duplicates(df)
    assert out.count() == 2

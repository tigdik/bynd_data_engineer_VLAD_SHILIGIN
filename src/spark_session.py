"""Utility for creating a single SparkSession."""
from pyspark.sql import SparkSession

_spark = None

def get_spark(app_name: str = "ByndPipeline") -> SparkSession:
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .getOrCreate()
        )
    return _spark

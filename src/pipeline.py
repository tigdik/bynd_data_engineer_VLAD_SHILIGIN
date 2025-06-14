"""ETL pipeline: read CSV → clean → transform → load to Postgres."""
import logging
from pyspark.sql import DataFrame

from .config import settings
from .spark_session import get_spark
from .data_cleaning import clean as clean_data
from .transformations import transform

TABLE_NAME = "mock_data"
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# -------------------------------------------------------------------------
# Extract
# -------------------------------------------------------------------------
def extract() -> DataFrame:
    spark = get_spark()
    logger.info("Reading CSV from %s", settings.source_data_path)
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(settings.source_data_path)
    )


# -------------------------------------------------------------------------
# Load
# -------------------------------------------------------------------------
def load(df: DataFrame) -> None:
    jdbc_url = (f"jdbc:postgresql://{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}")
    logger.info("Writing %d records to %s.%s", df.count(), settings.postgres_db, TABLE_NAME)
    ( df.write
        .mode("overwrite")
        .option("truncate", "true")
        .option("driver", "org.postgresql.Driver")
        .jdbc(jdbc_url, TABLE_NAME, properties={
            "user": settings.postgres_user,
            "password": settings.postgres_password,
        })        )
    logger.info("Load complete")


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    df_raw = extract()
    df_clean = clean_data(df_raw)   # <‑‑ cleaning stage
    df_curated = transform(df_clean)
    load(df_curated)


if __name__ == "__main__":
    main()

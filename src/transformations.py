"""Business/domain‑level enrichments – run AFTER `data_cleaning.clean`.

For the scope of this exercise we leave this minimal, but this is where you
would:
* parse nested JSON objects
* derive KPIs
* join reference datasets
* aggregate/roll‑up, etc.
"""

from pyspark.sql import DataFrame

def transform(df: DataFrame) -> DataFrame:
    """Identity transform placeholder – extend as needed."""
    return df

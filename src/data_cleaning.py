"""Low‑level data cleaning utilities: column hygiene & de‑duplication.

These steps should be *source‑agnostic*: they make raw files valid and
consistent, but don’t inject business semantics (those live in
`transformations.py`)."""
import re
import logging
from pyspark.sql import DataFrame, functions as F

logger = logging.getLogger(__name__)

_NON_ALNUM_RE = re.compile(r"[^0-9a-zA-Z]+")


# ---------------------------------------------------------------------------
# Column standardisation
# ---------------------------------------------------------------------------
def _snake_case(col_name: str) -> str:
    return _NON_ALNUM_RE.sub("_", col_name.strip()).lower()


def clean_column_names(df: DataFrame) -> DataFrame:
    """Convert every column to snake_case alphanumerics only."""
    for old in df.columns:
        df = df.withColumnRenamed(old, _snake_case(old))
    return df


# ---------------------------------------------------------------------------
# Duplicate column resolution
# ---------------------------------------------------------------------------
def _resolve_account_created_at(df: DataFrame) -> DataFrame:
    """The CSV ships with two *visually identical* columns differing only
    in case. We keep one canonical column called `account_created_at`.

    If the two columns have *identical values* row‑by‑row we simply drop the
    redundant one. Otherwise we coalesce them (preferring the lowercase) and
    emit a warning so that issues surface in logs/CI.
    """
    lower_col = "account created at"         # as in original header
    upper_col = "account Created at"        # duplicate variant

    if lower_col not in df.columns or upper_col not in df.columns:
        return df  # nothing to do

    # Compare values quickly – if any row differs, diff_count > 0
    diff_count = (
        df.filter((F.col(lower_col) != F.col(upper_col)) |  # noqa: E501
                  (F.col(lower_col).isNull() ^ F.col(upper_col).isNull()))
          .limit(1)      # short‑circuit for perf
          .count()
    )

    if diff_count == 0:
        logger.info("Duplicate 'account created at' columns identical; dropping redundant %s", upper_col)
        df = df.drop(upper_col)
    else:
        logger.warning("Duplicate columns not identical – coalescing values and dropping %s", upper_col)
        df = (
            df.withColumn(lower_col, F.coalesce(F.col(lower_col), F.col(upper_col)))
              .drop(upper_col)
        )
    return df


# ---------------------------------------------------------------------------
# Duplicated row removal
# ---------------------------------------------------------------------------
def remove_duplicates(df: DataFrame) -> DataFrame:
    """Drop completely duplicated rows."""
    return df.dropDuplicates()


# ---------------------------------------------------------------------------
# Pipeline entry
# ---------------------------------------------------------------------------
def clean(df: DataFrame) -> DataFrame:
    """End‑to‑end cleaning pipeline for raw CSV input.

    1. Resolve known duplicate columns.
    2. Standardise column names.
    3. Remove duplicate rows.

    Returns a new *immutable* DataFrame (piped transformation)."""
    return (
        df.transform(_resolve_account_created_at)
          .transform(clean_column_names)
          .transform(remove_duplicates)
    )

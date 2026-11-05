from pyspark.sql import functions as F
from common_utils.logging import get_logger

logger = get_logger("goldfunctions")

def date_key(column):
    """Turn a date or timestamp column into an integer key, e.g. 2026-04-01 -> 20260401."""
    return F.date_format(column, "yyyyMMdd").cast("int")


def write_gold(spark, df, target_table):
    """Overwrite a Gold Delta table and log its row count."""

    schema = target_table.rsplit(".", 1)[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    logger.info("Written [%s] with %s rows", target_table, df.count())

def build_snapshot(fact_df, snapshot_df):
    """
    For every snapshot date, keep the one version of each record
    that was valid on that date (uses the SCD2 columns from Silver).
    """
    return (
        fact_df.crossJoin(snapshot_df)
        .filter(
            (F.col("valid_from") <= F.col("snapshot_date")) &
            (F.col("valid_to").isNull() | (F.col("valid_to") > F.col("snapshot_date")))
        )
        .drop("valid_from", "valid_to", "is_current", "last_updated_timestamp")
    )


def check_no_duplicates(df, key_column):
    """Fail if any record appears more than once in the same snapshot."""

    duplicates = (
        df.groupBy("snapshot_date", key_column)
        .count()
        .filter("count > 1")
        .count()
    )

    if duplicates > 0:
        raise ValueError(f"{duplicates} duplicate ({key_column}, snapshot_date) rows found")

    logger.info("No duplicate (%s, snapshot_date) rows found", key_column)
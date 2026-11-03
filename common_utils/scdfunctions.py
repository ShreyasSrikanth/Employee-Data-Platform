from delta.tables import DeltaTable

from common_utils.logging import get_logger


logger = get_logger("scdfunctions")


def scd_type_1(
    spark,
    source_df,
    target_table,
    key_columns
):
    """
    Perform SCD Type 1 merge.

    Existing records are updated.
    New records are inserted.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    source_df : DataFrame
        Transformed source DataFrame.

    target_table : str
        Fully qualified Delta target table.

    key_columns : list
        List of columns used to identify a record.

    Returns
    -------
    None
    """

    logger.info(
        "Starting SCD Type 1 for target [%s]",
        target_table
    )

    logger.info(
        "SCD Type 1 key columns: %s",
        key_columns
    )

    # --------------------------------------------------------
    # Validate key columns
    # --------------------------------------------------------

    missing_keys = [
        column
        for column in key_columns
        if column not in source_df.columns
    ]

    if missing_keys:

        raise ValueError(
            f"Missing SCD Type 1 key columns in source DataFrame: "
            f"{missing_keys}"
        )

    # --------------------------------------------------------
    # Check target table
    # --------------------------------------------------------

    if not spark.catalog.tableExists(target_table):

        logger.info(
            "Target table [%s] does not exist",
            target_table
        )

        return False

    # --------------------------------------------------------
    # Get Delta target
    # --------------------------------------------------------

    target = DeltaTable.forName(
        spark,
        target_table
    )

    # --------------------------------------------------------
    # Build merge condition
    # --------------------------------------------------------

    merge_condition = " AND ".join(
        [
            f"target.`{column}` = source.`{column}`"
            for column in key_columns
        ]
    )

    logger.info(
        "SCD Type 1 merge condition: %s",
        merge_condition
    )

    # --------------------------------------------------------
    # Execute MERGE
    # --------------------------------------------------------

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        "SCD Type 1 completed successfully for [%s]",
        target_table
    )

    return True


from pyspark.sql import functions as F
from pyspark.sql.window import Window


def scd_type_2(
    spark,
    source_df,
    target_table,
    key_columns,
    version_column="batch_date"
):
    """
    Perform SCD Type 2 build.

    Every version of a record is kept.
    Each version gets valid_from, valid_to and is_current.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    source_df : DataFrame
        Transformed source DataFrame containing all versions
        (all batches unioned together).

    target_table : str
        Fully qualified Delta target table.

    key_columns : list
        List of columns used to identify a record.

    version_column : str
        Column that orders the versions (default: batch_date).

    Returns
    -------
    None
    """

    logger.info(
        "Starting SCD Type 2 for target [%s]",
        target_table
    )

    logger.info(
        "SCD Type 2 key columns: %s, version column: %s",
        key_columns,
        version_column
    )

    # --------------------------------------------------------
    # Validate key and version columns
    # --------------------------------------------------------

    missing_columns = [
        column
        for column in key_columns + [version_column]
        if column not in source_df.columns
    ]

    if missing_columns:

        raise ValueError(
            f"Missing SCD Type 2 columns in source DataFrame: "
            f"{missing_columns}"
        )

    # --------------------------------------------------------
    # Build validity columns
    # --------------------------------------------------------

    version_window = (
        Window
        .partitionBy(*key_columns)
        .orderBy(version_column)
    )

    scd2_df = (
        source_df
        .withColumn("valid_from", F.col(version_column))
        .withColumn("valid_to", F.lead(version_column).over(version_window))
        .withColumn("is_current", F.col("valid_to").isNull())
    )

    # --------------------------------------------------------
    # Write target table (full rebuild from all batches)
    # --------------------------------------------------------

    (
        scd2_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    logger.info(
        "SCD Type 2 completed successfully for [%s]",
        target_table
    )

    return True
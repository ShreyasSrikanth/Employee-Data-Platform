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
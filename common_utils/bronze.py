# options will be provided in dictionary format

import pyspark.sql.functions as F
from common_utils.logging import get_logger

logger = get_logger("common_utils_bronze")

def read_raw(raw_path, file_format, options = None):
    """
    Read raw_files fro volume
    raw_path: /Volumes/retaildataplatform/bronze/raw_data/sqlserver_customers/load_date=2026-09-07/
    file format: json or csv or parquet
    options: key value

    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    logger.info('Reading from path %s:', raw_path)
    logger.info('File Format: %s', file_format)

    reader = spark.read.format(file_format)
    for key,value in (options or {}).items():
        reader = reader.option(key,value)

    logger.info('read complete and stored in Dataframe df')
    return reader.load(raw_path)
    


def bronze_ingestor(df,mode,target_table):

    """
    Add audit columns and write in bronze as delta table

    df              : Dataframe read from raw volume
    target_table    : catalog.schema.table
    mode            :'overwrite' or 'append'

    Return the no. of rows written

    """

    logger.info("Adding columns in dataframe")
    df = df.withColumn("last_updated_timestamp", F.current_timestamp() )\
        .withColumn("file_path", F.col("_metadata.file_path"))

    logger.info("Writing to table %s",target_table)
    logger.info("Selected mode is %s", mode)

    df.write.format('delta').mode(mode).saveAsTable(target_table)

    return df.count()



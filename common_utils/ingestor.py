import urllib.request

def setup_catalog (spark, catalog):
    spark.sql(f"create catalog if not exists {catalog}")

def setup_schema(spark, catalog, schema):
    spark.sql(f"create schema if not exists {catalog}.{schema}")

def setup_volume(spark, catalog, schema, volume):
    spark.sql(f"create volume if not exists {catalog}.{schema}.{volume}")

def setup_directory(dbutils, output_path):
    dbutils.fs.mkdirs(output_path)

def read_and_write_raw(urllib, base_url, folder_path, gdrive_file_ids):
    for file_name,file_id in gdrive_file_ids.items():
        url = base_url.format(file_id=file_id)
        urllib.request.urlretrieve(url, f"{folder_path}/{file_name}")

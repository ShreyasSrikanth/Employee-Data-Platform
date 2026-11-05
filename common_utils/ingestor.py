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



def list_gdrive_entries(urllib, folder_url, folder_id):
    """List the files and subfolders of a public Google Drive folder.

    Returns a list like:
    [{"name": "DimPeople.csv", "id": "1pjA...", "is_folder": False}, ...]
    """
    import re

    # step 1: download the folder's web page as text

    url = folder_url.format(folder_id=folder_id)
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    html = urllib.request.urlopen(request, timeout=60).read().decode("utf-8", "ignore")

    # step 2: pull out the id, link and name of every item on the page
    pattern = (
        r'<div class="flip-entry" id="entry-([-\w]+)"'   # item id
        r'.*?<a href="([^"]+)"'                          # item link
        r'.*?<div class="flip-entry-title">([^<]+)</div>'  # item name
    )

    matches = re.findall(pattern, html, re.S)

    # step 3: turn each match into a simple dictionary
    entries = []

    for item_id, link, name in matches:
        subfolder = "/drive/folders/" in link
        entries.append({
            "id": item_id,
            "name": name,
            "is_folder": subfolder
        })

    return entries
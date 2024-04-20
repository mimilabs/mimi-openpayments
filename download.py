# Databricks notebook source

import requests
from pathlib import Path
import datetime
from dateutil.relativedelta import *
import zipfile

# COMMAND ----------

url = "https://download.cms.gov/openpayments"
volumepath = "/Volumes/mimi_ws_1/openpayments/src"
volumepath_zip = f"{volumepath}/zipfiles"
retrieval_range = 9 # in years; yearly files

# COMMAND ----------

ref_monthyear = datetime.datetime.now()
files_to_download = [] # an array of tuples

# 2 year lag
for yr_diff in range(2, retrieval_range): 
    yr = (ref_monthyear - relativedelta(years=yr_diff)).strftime('%y').lower()
    files_to_download.append(f"PGYR{yr}_P011824.ZIP")

files_to_download.append("PHPRFL_P011824.ZIP") # supplemental

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

# Display the zip file links
for filename in files_to_download:
    # Check if the file exists
    if Path(f"{volumepath_zip}/{filename}").exists():
        # print(f"{filename} exists, skipping...")
        continue
    else:
        print(f"{filename} downloading...")
        download_file(f"{url}/{filename}", filename, volumepath_zip)

# COMMAND ----------

files_downloaded = [x for x in Path(volumepath_zip).glob("PGYR*")]

# COMMAND ----------

for file_downloaded in files_downloaded:
    if file_downloaded.stem in ["PGYR21_P011824", "PGYR22_P011824"]:
        # a zip file where Python's zipfile cannot read...
        continue
    print(file_downloaded)
    with zipfile.ZipFile(file_downloaded, "r") as zip_ref:
        for member in zip_ref.namelist():
            if not Path(f"{volumepath}/main/{member}").exists():
                print(f"Extracting {member}...")
                zip_ref.extract(member, path=f"{volumepath}/main")

# COMMAND ----------

files_downloaded = [x for x in Path(volumepath_zip).glob("PHPRFL*")]

# COMMAND ----------

for file_downloaded in files_downloaded:
    with zipfile.ZipFile(file_downloaded, "r") as zip_ref:
        for member in zip_ref.namelist():
            if not Path(f"{volumepath}/supplement/{member}").exists():
                print(f"Extracting {member}...")
                zip_ref.extract(member, path=f"{volumepath}/supplement")

# COMMAND ----------



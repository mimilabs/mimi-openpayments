# Databricks notebook source
# MAGIC %md
# MAGIC # Open Payments
# MAGIC
# MAGIC - OP_DTL_GNRL_PGYR{Year}_P01182024.csv (general)
# MAGIC - OP_DTL_RSRCH_PGYR{Year}_P01182024.csv (research)
# MAGIC - OP_DTL_OWNRSHP_PGYR{Year}_P01182024.csv (ownership)
# MAGIC - OP_REMOVED_DELETED_PGYR{Year}_P01182024.csv (deleted)

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, to_date, regexp_replace
from datetime import datetime
from dateutil.parser import parse
import pandas as pd

path = "/Volumes/mimi_ws_1/openpayments/src/main" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "openpayments" # delta table destination schema


# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## General Payments

# COMMAND ----------

tablename = "general" # destination table

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"

# COMMAND ----------

files = []
for filepath in Path(f"{path}").glob("OP_DTL_GNRL_PGYR*"):
    year = filepath.stem[16:20]
    dt = parse(f"{year}-12-31").date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"number_of_payments_included_in_total_amount", "program_year"}
double_columns = {"total_amount_of_payment_usdollars"}
date_columns = {"payment_publication_date", "date_of_payment"}
legacy_columns = {}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("double"))
        elif col_new in date_columns:
            df = df.withColumn(col_new, to_date(col(col_old), "MM/dd/yyyy"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Research Payments

# COMMAND ----------

tablename = "research" # destination table

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"
files = []
for filepath in Path(f"{path}").glob("OP_DTL_RSRCH_PGYR*"):
    year = filepath.stem[17:21]
    dt = parse(f"{year}-12-31").date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"program_year"}
double_columns = {"total_amount_of_payment_usdollars"}
date_columns = {"payment_publication_date", "date_of_payment"}
legacy_columns = {}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("double"))
        elif col_new in date_columns:
            df = df.withColumn(col_new, to_date(col(col_old), "MM/dd/yyyy"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ownership

# COMMAND ----------

tablename = "ownership" # destination table

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"
files = []
for filepath in Path(f"{path}").glob("OP_DTL_OWNRSHP_PGYR*"):
    year = filepath.stem[19:23]
    dt = parse(f"{year}-12-31").date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"program_year"}
double_columns = {"total_amount_invested_usdollars", "value_of_interest"}
date_columns = {"payment_publication_date"}
legacy_columns = {}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("double"))
        elif col_new in date_columns:
            df = df.withColumn(col_new, to_date(col(col_old), "MM/dd/yyyy"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deleted and Removed

# COMMAND ----------

tablename = "deleted"

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"
files = []
for filepath in Path(f"{path}").glob("OP_REMOVED_DELETED_PGYR*"):
    
    year = filepath.stem[23:27]
    dt = parse(f"{year}-12-31").date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"program_year"}
legacy_columns = {}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("int"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------



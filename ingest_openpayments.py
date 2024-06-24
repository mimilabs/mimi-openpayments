# Databricks notebook source
# MAGIC %md
# MAGIC # Open Payments
# MAGIC
# MAGIC - OP_DTL_GNRL_PGYR{Year}_P01182024.csv (general)
# MAGIC - OP_DTL_RSRCH_PGYR{Year}_P01182024.csv (research)
# MAGIC - OP_DTL_OWNRSHP_PGYR{Year}_P01182024.csv (ownership)
# MAGIC - OP_REMOVED_DELETED_PGYR{Year}_P01182024.csv (deleted)

# COMMAND ----------

# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/ingestion_utils

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.openpayments.general;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.openpayments.research;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.openpayments.ownership;
# MAGIC --DROP TABLE IF EXISTS mimi_ws_1.openpayments.deleted;

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_date, regexp_replace, when
catalog = "mimi_ws_1" # delta table destination catalog
schema = "openpayments" # delta table destination schema
path = f"/Volumes/mimi_ws_1/{schema}/src/main" # where all the input files are located

# COMMAND ----------

# MAGIC %md
# MAGIC ## General Payments

# COMMAND ----------

tablename = "general" # destination table

# COMMAND ----------

files = []
for filepath in Path(f"{path}").glob("OP_DTL_GNRL_PGYR*"):
    year = filepath.stem[16:20]
    dt = parse(f"{year}-12-31").date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"number_of_payments_included_in_total_amount", "program_year"}
double_columns = {"total_amount_of_payment_us_dollars"}
date_columns = {"payment_publication_date", "date_of_payment"}
legacy_columns = {}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("quote", "\"")
            .option("escape", "\"")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        if col_new in int_columns:
            df = df.withColumn(col_new, col(col_old).cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, col(col_old).cast("double"))
        elif col_new in date_columns:
            df = df.withColumn(col_new, to_date(col(col_old), "MM/dd/yyyy"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("mimi_src_file_date", lit(item[0]))
          .withColumn("mimi_src_file_name", lit(item[1].name))
          .withColumn("mimi_dlt_load_date", lit(datetime.today().date())))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    mimi_src_date = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_date = '{mimi_src_date}'")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Research Payments

# COMMAND ----------

tablename = "research" # destination table

# COMMAND ----------

files = []
for filepath in Path(f"{path}").glob("OP_DTL_RSRCH_PGYR*"):
    year = filepath.stem[17:21]
    dt = parse(f"{year}-12-31").date()
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
            .option("quote", "\"")
            .option("escape", "\"")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, col(col_old).cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, col(col_old).cast("double"))
        elif col_new in date_columns:
            df = df.withColumn(col_new, to_date(col(col_old), "MM/dd/yyyy"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("mimi_src_file_date", lit(item[0]))
          .withColumn("mimi_src_file_name", lit(item[1].name))
          .withColumn("mimi_dlt_load_date", lit(datetime.today().date())))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    mimi_src_date = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_date = '{mimi_src_date}'")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ownership

# COMMAND ----------

tablename = "ownership" # destination table

# COMMAND ----------

files = []
for filepath in Path(f"{path}").glob("OP_DTL_OWNRSHP_PGYR*"):
    year = filepath.stem[19:23]
    dt = parse(f"{year}-12-31").date()
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
            .option("quote", "\"")
            .option("escape", "\"")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, col(col_old).cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, col(col_old).cast("double"))
        elif col_new in date_columns:
            df = df.withColumn(col_new, to_date(col(col_old), "MM/dd/yyyy"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("mimi_src_file_date", lit(item[0]))
          .withColumn("mimi_src_file_name", lit(item[1].name))
          .withColumn("mimi_dlt_load_date", lit(datetime.today().date())))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    mimi_src_date = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_date = '{mimi_src_date}'")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deleted and Removed

# COMMAND ----------

tablename = "deleted"

# COMMAND ----------

files = []
for filepath in Path(f"{path}").glob("OP_REMOVED_DELETED_PGYR*"):
    year = filepath.stem[23:27]
    dt = parse(f"{year}-12-31").date()
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
            .option("quote", "\"")
            .option("escape", "\"")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, col(col_old).cast("int"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("mimi_src_file_date", lit(item[0]))
          .withColumn("mimi_src_file_name", lit(item[1].name))
          .withColumn("mimi_dlt_load_date", lit(datetime.today().date())))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    mimi_src_date = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("replaceWhere", f"mimi_src_file_date = '{mimi_src_date}'")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------



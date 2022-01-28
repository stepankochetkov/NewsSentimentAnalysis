# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls(f"wasbs://{containerName}@{storageAccountName}.blob.core.windows.net/silver raw")

# COMMAND ----------

def get_dir_content(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths
    

paths = get_dir_content(f"wasbs://{containerName}@{storageAccountName}.blob.core.windows.net/silver raw")
[print(p) for p in paths]

# COMMAND ----------

schemaGoogleNews = StructType([\
                              StructField("status", StringType(), False),\
                              StructField("totalResults", IntegerType(), False), \
                              StructField("id")])

# COMMAND ----------

df = spark.read.parquet("wasbs://devdata@storageskdev0001.blob.core.windows.net/silver raw/Google News/Russia OR Russian/2022-01-17/Results_Russia OR Russian_2022-01-17.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------



# Databricks notebook source
# MAGIC %md ## Imports

# COMMAND ----------

from pyspark.sql.functions import explode, col
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md ## Configs & Helper Functions

# COMMAND ----------

# MAGIC %run "../HelperFunctions"

# COMMAND ----------

# MAGIC %run "../Configurations"

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("source", "Google News", "Source:")
dbutils.widgets.text("topic", "Russia OR Russian", "Topic:")
dbutils.widgets.text("date", "2022-01-17", "Date: YYYY-MM-DD")

# COMMAND ----------

source = dbutils.widgets.get("source")
topic = dbutils.widgets.get("topic")
date = dbutils.widgets.get("date")

# COMMAND ----------

# MAGIC %md ## Schema

# COMMAND ----------

# Ouput Dataframe Schema
schemaGoogleNews = StructType(fields=[
    StructField("sourceId", StringType(), True),
    StructField("sourceName", StringType(), True),
    StructField("author", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("urlToImage", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("content", StringType(), True)])

# COMMAND ----------

# MAGIC %md ## Main Program

# COMMAND ----------

# Bronze Configurations
bronzePath = f'wasbs://{bronzeContainerName}@{bronzeStorageAccountName}.blob.core.windows.net/bronze/'
bronzeBlobServiceClient = create_blob_service_client(bronzeConnectionString)
bronzeBlobName = f'bronze/{source}/{topic}/{date}/Results_{topic}_{date}.json'
print(bronzeBlobName)
bronzeBlobClient = create_blob_client(bronzeBlobServiceClient, bronzeContainerName, bronzeBlobName)

# COMMAND ----------

# Silver Raw Confugurations
silverRawPath = f'wasbs://{silverRawContainerName}@{silverRawStorageAccountName}.blob.core.windows.net/silver raw/'
silverRawBlobServiceClient = create_blob_service_client(silverRawConnectionString)
silverRawBlobName = f'silver raw/{source}/{topic}/{date}/Results_{topic}_{date}.parquet'
print(silverRawBlobName)
silverRawBlobClient = create_blob_client(silverRawBlobServiceClient, silverRawContainerName, silverRawBlobName)

# COMMAND ----------

# Get source data
df = download_json(bronzeBlobClient)

# COMMAND ----------

# Flat the dataframe
articles = df.select(explode("articles").alias("articles")).select("articles.*")
df_flat = articles.select(col("source.*"), "author", "title", "description", "urlToImage", "publishedAt", "content").toDF("sourceId", "sourceName", "author", "title", "description", "urlToImage", "publishedAt", "content")
df_output = spark.createDataFrame(df_flat.rdd, schema=schemaGoogleNews)

# COMMAND ----------

# Upload data to blob storage
upload_parquet(silverRawBlobClient, df_output)

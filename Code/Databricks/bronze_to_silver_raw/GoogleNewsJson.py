# Databricks notebook source
# MAGIC %md ## Imports

# COMMAND ----------

from pyspark.sql.functions import explode, col

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
    StructField("sourceId", StringType(), False),
    StructField("sourceName", StringType(), False),
    StructField("author", StringType(), False),
    StructField("title", StringType(), False),
    StructField("description", StringType(), False),
    StructField("urlToImage", StringType(), False),
    StructField("publishedAt", StringType(), False),
    StructField("content", StringType(), False)])

# COMMAND ----------

# MAGIC %md ## Main Program

# COMMAND ----------

# Bronze Configurations
bronzePath = f'wasbs://{bronzeContainerName}@{bronzeStorageAccountName}.blob.core.windows.net/bronze/'
bronzeBlobServiceClient = create_blob_service_client(bronzeConnectionString)
bronzeBlobName = bronzePath + f'{source}/{topic}/{date}/Results_{topic}_{date}.json'
bronzeBlobClient = create_blob_client(bronzeBlobServiceClient, bronzeBlobName)

# COMMAND ----------

# Silver Raw Confugurations
silverRawPath = f'wasbs://{silverRawContainerName}@{silverRawStorageAccountName}.blob.core.windows.net/silver raw/'
silverRawBlobServiceClient = client_blob_service_client(silverRawConnectionString)
silverRawBlobName = silverRawPath + f'{source}/{topic}/{date}/Results_{topic}_{date}.parquet'
silverRawBlobClient = create_blob_client(silverRawBlobServiceClient, silverRawBlobName)

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

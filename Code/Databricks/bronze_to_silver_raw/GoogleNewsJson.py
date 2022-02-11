# Databricks notebook source
from pyspark.sql.functions import explode, col

# COMMAND ----------

# MAGIC %run "../HelperFunctions"

# COMMAND ----------

# MAGIC %run "../Configurations"

# COMMAND ----------

spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

# COMMAND ----------

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

bronzePath = "wasbs://devdata@storageskdev0001.blob.core.windows.net/bronze/Google News/Russia OR Russian/2022-01-17/Results_Russia OR Russian_2022-01-17.json"

# COMMAND ----------

silverRawPath = "wasbs://devdata@storageskdev0001.blob.core.windows.net/silver raw/Google News/Russia OR Russian/2022-01-17/Results_Russia OR Russian_2022-01-17"

# COMMAND ----------

# df = spark.read.json(bronzePath, schema=schemaGoogleNews)
df = spark.read.json(bronzePath)

# COMMAND ----------

articles = df.select(explode("articles").alias("articles")).select("articles.*")
flatten_df = articles.select(col("source.*"), "author", "title", "description", "urlToImage", "publishedAt", "content").toDF("sourceId", "sourceName", "author", "title", "description", "urlToImage", "publishedAt", "content")

# COMMAND ----------

df_output = spark.createDataFrame(flatten_df.rdd, schema=schemaGoogleNews)

# COMMAND ----------

df_output.write.format("delta").mode('overwrite').parquet(silverRawPath)

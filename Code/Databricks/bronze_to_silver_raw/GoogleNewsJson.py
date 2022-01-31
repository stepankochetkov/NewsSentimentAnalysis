# Databricks notebook source
from pyspark.sql.functions import explode, col

# COMMAND ----------

# MAGIC %run "../HelperFunctions"

# COMMAND ----------

# MAGIC %run "../Configurations"

# COMMAND ----------

schemaGoogleNews = StructType(fields=[
    StructField("articles", ArrayType(
        StructType([
            StructField("source", 
                        StructType([StructField("id", StringType(), True),
                                    StructField("name", StringType(),True)])),
            StructField("author", StringType(), False),
            StructField("title", StringType(), False),
            StructField("description", StringType(), False),
            StructField("urlToImage", StringType(), False),
            StructField("publishedAt", StringType(), False),
            StructField("content", StringType(), False)]))),
    StructField("status", StringType(), False),
    StructField("totalResults", IntegerType(), False),])

# COMMAND ----------

bronzePath = "wasbs://devdata@storageskdev0001.blob.core.windows.net/bronze/Google News/Russia OR Russian/2022-01-17/Results_Russia OR Russian_2022-01-17.json"

# COMMAND ----------

silverRawPath = "wasbs://devdata@storageskdev0001.blob.core.windows.net/silver raw/Google News/Russia OR Russian/2022-01-17/Results_Russia OR Russian_2022-01-17.parquet"

# COMMAND ----------

df = spark.read.json(bronzePath, schema=schemaGoogleNews)

# COMMAND ----------

articles = df.select(explode("articles").alias("articles")).select("articles.*")
flatten_df = articles.select(col("source.*"), "author", "title", "description", "urlToImage", "publishedAt", "content").toDF("sourceId", "sourceName", "author", "title", "description", "urlToImage", "publishedAt", "content")

# COMMAND ----------

flatten_df.write.mode("overwrite").parquet(silverRawPath)

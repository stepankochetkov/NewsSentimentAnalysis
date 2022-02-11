# Databricks notebook source
from azure.storage.blob import BlobServiceContainer, BlobClient, BlobContainer
import json
import pandas as pd

# COMMAND ----------

# Create Blob Service Client using Connection String
def create_blob_service_client(connection_string):
    return BlobServiceClient.from_connection_string(connection_string)

# COMMAND ----------

# Get blob client
def create_blob_client(blob_service_client, container_name, blob_name):
    return blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# COMMAND ----------

# Download json file into a spark dataframe
def download_json(blob_client):
    stream = blob_client.download_blob()
    content = json.loads(stream.readall())
    df = spark.read.json(sc.parallelize([content]))
    return df

# COMMAND ----------

# Upload spark dataframe as parquet file to blob storage
def upload_parquet(blob_client, df):
    pandasDF = df.toPandas()
    buffer = io.BytesIO()
    pandasDF.to_parquet(buffer)
    blob_client.upload_blob(buffer.getvalue(), overwrite=True)

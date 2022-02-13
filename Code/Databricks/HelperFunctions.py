# Databricks notebook source
import azure
import json
import pandas as pd
import io

from azure.storage.blob import BlobServiceClient

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
    jsonDataDict = json.loads(stream.readall())
    jsonData = json.dumps(jsonDataDict)
    jsonDataList = []
    jsonDataList.append(jsonData)
    jsonRDD = sc.parallelize(jsonDataList)
    df = spark.read.json(jsonRDD)
    return df

# COMMAND ----------

# Download paqruet file as a pandas dataframe
def download_parquet(blob_client):
    blob_downloader = blob_client.download_blob()
    stream = io.BytesIO()
    blob_downloader.readinto(stream)
    return pd.read_paqruet(stream)

# COMMAND ----------

# Upload spark dataframe as parquet file to blob storage
def upload_parquet(blob_client, df):
    pandasDF = df.toPandas()
    buffer = io.BytesIO()
    pandasDF.to_parquet(buffer)
    blob_client.upload_blob(buffer.getvalue(), overwrite=True)

# COMMAND ----------

# Upload pandas dataframe as csv to blob storage
def upload_csv(blob_client, df):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    blob_client.upload_blob(buffer.get_value(), overwrite=True)

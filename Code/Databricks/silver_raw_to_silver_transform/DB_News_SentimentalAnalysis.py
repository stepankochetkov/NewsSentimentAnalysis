# Databricks notebook source
# MAGIC %md ## Imports

# COMMAND ----------

from flair.models import TextClassifier
from flair.data import Sentence

# COMMAND ----------

# MAGIC %md ## Configs & Helper Functions

# COMMAND ----------

# MAGIC %run "../Configurations"

# COMMAND ----------

# MAGIC %run "../HelperFunctions"

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text('source', 'Google News', 'Source:')
dbutils.widgets.text('topic', 'Russia OR Russian', 'Topic:')
dbutils.widgets.text('date', '2022-01-17', 'Date:')

# COMMAND ----------

source = dbutils.widgets.get('source')
topic = dbutils.widgets.get('topic')
date = dbutils.widgets.get('date')

# COMMAND ----------

# MAGIC %md ### Common Functions for this notebook

# COMMAND ----------

def analyze_sentiment(df, column_name):
    for i in range(0, len(df.iloc[:, 0])):
        sentence = Sentence(df.loc[i, column_name])
        classifier.predict(sentence)
        result_str = str(sentence.labels[0]).split(" ")
        sentiment = result_str[0]
        grade = result_str[1].replace("(", "").replace(")", "")
        df.loc[i, f'{column_name} sentiment result'] = sentiment
        df.loc[i, f'{column_name} sentiment grade'] = grade
    return df

# COMMAND ----------

# MAGIC %md ## Main program

# COMMAND ----------

# Silver Raw Confugurations
silverRawBlobServiceClient = create_blob_service_client(silverRawConnectionString)
silverRawBlobName = f'silver raw/{source}/{topic}/{date}/Results_{topic}_{date}.parquet'
silverRawBlobClient = create_blob_client(silverRawBlobServiceClient, silverRawContainerName, silverRawBlobName)

# COMMAND ----------

# Silver Transform Confugurations
silverTransformBlobServiceClient = create_blob_service_client(silverRawConnectionString)
silverTransformBlobName = f'silver transform/{source}/{topic}/{date}/Results_{topic}_{date}.csv'
silverTransformBlobClient = create_blob_client(silverTransformBlobServiceClient, silverTransformContainerName, silverTransformBlobName)

# COMMAND ----------

df = download_parquet(silverRawBlobClient)

# COMMAND ----------

classifier = TextClassifier.load('en-sentiment')

# COMMAND ----------

df = analyze_sentiment(df, "title")

# COMMAND ----------

upload_csv(silverTransformBlobClient, df)

# Databricks notebook source
# MAGIC %md ## Import

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

# Parameters for this notebook to expect
dbutils.widgets.text("source", "Google News", "Source:")
dbutils.widgets.text("topic", "Russia OR Russian", "Topic:")
dbutils.widgets.text("date", "2022-01-17", "Date: YYYY-MM-DD")

# COMMAND ----------

source = dbutils.widgets.get("source")
topic = dbutils.widgets.get("topic")
date = dbutils.widgets.get("date")

# COMMAND ----------

# MAGIC %md ## Functions

# COMMAND ----------

# Expects a pandas dataframe and the name of a column to add 2 extra columns: sentiment and sentiment grade. Utilizes Flair NLP algorithm.
def sentimental_analysis(df, column_name):
    for i in range(0, len(df.iloc[0, :])):
        sentence = Sentence(df.loc[i, column_name])
        classifier.predict(sentence)
        result = str(sentence.labels[0]).split(" ")
        sentiment = result[0]
        sentiment_grade = result[1].replace("(", "").replace(")", "")
        df.loc[i, f'{column_name}_sentiment'] = sentiment
        df.loc[i, f'{column_name}_sentiment_grade'] = sentiment_grade
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
silverTransformBlobName = f'silver raw/{source}/{topic}/{date}/Results_{topic}_{date}.csv'
silverTransformBlobClient = create_blob_client(silverTransformBlobServiceClient, silverTransformContainerName, silverTransformBlobName)

# COMMAND ----------

# Load SR file
df = download_parquet(silverRawBlobClient)

# COMMAND ----------

# Sentimental analysis of titles
classifier = TextClassifier.load('en-sentiment')
df = sentimental_analysis(df, 'title')

# COMMAND ----------

# Upload an updated dataframe as a csv file to the blob storage
upload_csv(silverTransformBlobClient, df)

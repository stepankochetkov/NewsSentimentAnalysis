# Databricks notebook source
# Bronze Configurations
bronzeStorageAccountName = "storageskdev0001"
bronzeStorageAccountKey = dbutils.secrets.get(scope="keyvaultskdev0001", key="BlobStorageAccountKeyDev")
bronzeContainerName = "devdata"
bronzeConnectionString = f'DefaultEndpointsProtocol=https;AccountName={bronzeStorageAccountName};AccountKey={bronzeStorageAccountKey};EndpointSuffix=core.windows.net'

# COMMAND ----------

# Silver Raw Configurations
silverRawStorageAccountName = "storageskdev0001"
silverRawStorageAccountKey = dbutils.secrets.get(scope="keyvaultskdev0001", key="BlobStorageAccountKeyDev")
silverRawContainerName = "devdata"
silverRawConnectionString = f'DefaultEndpointsProtocol=https;AccountName={bronzeStorageAccountName};AccountKey={bronzeStorageAccountKey};EndpointSuffix=core.windows.net'

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", "false")

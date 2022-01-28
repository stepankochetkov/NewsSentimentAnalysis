# Databricks notebook source
storageAccountName = "storageskdev0001"
storageAccountAccessKey = dbutils.secrets.get(scope="keyvaultskdev0001", name="BlobStorageAccountKeyDev")
containerName = "devdata"

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{storageAccountName}.blob.core.windows.net",
  f"{storageAccountAccessKey}")

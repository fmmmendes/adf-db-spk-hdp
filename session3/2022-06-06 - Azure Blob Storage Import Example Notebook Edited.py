# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook shows you how to create and query a table or DataFrame loaded from data stored in Azure Blob storage.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Set the data location and type
# MAGIC 
# MAGIC There are two ways to access Azure Blob storage: account keys and shared access signatures (SAS).
# MAGIC 
# MAGIC To get started, we need to set the location and type of the file.
# MAGIC 
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage   
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough#single-user

# COMMAND ----------

import os

# COMMAND ----------

from pyspark.sql.functions import *	

# COMMAND ----------

storage_account_name = "adfdatabricksstorage"
storage_account_access_key = os.environ['ACCESS_KEY']
container = "extract"

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

dbutils.fs.ls(f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Read the data
# MAGIC 
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC 
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

file_location = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/2022-03.csv"
file_type = "csv"

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option("header", "true").load(file_location)

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df_aux = df.withColumn("date_ref", to_date(col("started_At"),"yyyy-MM-dd"))

# COMMAND ----------

display(df_aux)

# COMMAND ----------

df_group = df_aux.groupBy("date_ref").agg(countDistinct("ride_id"))

# COMMAND ----------

display(df_group)

# COMMAND ----------

container_outp = "databricks"
file_location_outp = f"wasbs://{container_outp}@{storage_account_name}.blob.core.windows.net/df_group.csv"

df_group.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(file_location_outp)

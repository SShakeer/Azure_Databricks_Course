# Databricks notebook source
print("Hello World")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting using Service Principle

# COMMAND ----------

# Required variables
storage_account_name = "eminentadlsgen2"
client_id = "2b40ffe2-fd49-4f53-9d7f-ea134f2f237d"
tenant_id = "61022000-1776-4c3a-b2c7-00ca679f7d6f"
client_secret = "8Z68Q~citYqr2GK3y16ZW_5HCeFUoOetJJfWRarX"
container_name = "batch12"
mount_point = "/mnt/raw"

# Set up OAuth 2.0 configurations
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Mount the ADLS Gen2 container to Databricks
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = mount_point,
  extra_configs = configs
)


# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("/mnt/raw")

# COMMAND ----------

df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/raw/emp.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.format("csv").mode("overwrite").save("/mnt/raw/emp_output.csv")

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/data/test")

# COMMAND ----------

dbutils.fs.rm("/mnt/data/test/emp.csv",True)

# COMMAND ----------

dbutils.fs.unmount("/mnt/data")

# COMMAND ----------

df=spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/data/emp.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mounting Using Access keys

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.<storage-account>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope>", key="<storage-account-access-key>"))

# COMMAND ----------

SP: We have conected to one container
Access: Entire ADLS access

# COMMAND ----------

# MAGIC %md
# MAGIC ### mounting using SAS

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))
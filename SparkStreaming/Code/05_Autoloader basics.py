# Databricks notebook source
# MAGIC %md
# MAGIC #### Why Autoloader?
# MAGIC * A framework to efficiently process new data files from cloud storage.
# MAGIC 1. Amazon S3
# MAGIC 2. Azure Data Lake Storage Gen2
# MAGIC 3. Google Cloud Storage
# MAGIC 4. Databricks File System
# MAGIC (JSON, CSV, PARQUET, AVRO, ORC, TEXT, BINARY FILE)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Optimized file listing
# MAGIC    1. Cloud-native APIs
# MAGIC    2. Fewer API calls
# MAGIC    3. Incremental listing
# MAGIC    4. Optional file notification service
# MAGIC 2. Simplified schema evolution
# MAGIC 3. Simplified data rescue

# COMMAND ----------

dbutils.fs.rm('dbfs:/user/hive/warehouse/stream.db',True)
#dbutils.fs.mkdir('dbfs:/FileStore/streaming')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS stream CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS stream

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/streaming/schemaInfer",True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## AutoLoader

# COMMAND ----------

source_dir = 'dbfs:/FileStore/streaming/'

# COMMAND ----------

df = spark.readStream\
        .format('cloudFiles')\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f'{source_dir}/schemaInfer')\
        .option("cloudFiles.inferColumnTypes","true")\
        .option('header','true')\
        .load(source_dir)

# COMMAND ----------

dbutils.fs.ls(f'{source_dir}/schemaInfer')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/streaming/schemaInfer/_schemas/')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM JSON.`dbfs:/FileStore/streaming/schemaInfer/_schemas/0`

# COMMAND ----------

# MAGIC %md
# MAGIC ### SchemaHints

# COMMAND ----------

df = spark.readStream\
        .format('cloudFiles')\
        .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f'{source_dir}/schemaInfer')\
        .option("cloudFiles.inferColumnTypes","true")\
        .option('cloudFiles.schemaHints',"Citizens LONG")\
        .option('header','true')\
        .load(source_dir)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM JSON.`dbfs:/FileStore/streaming/schemaInfer/_schemas/0`

# COMMAND ----------

df.display()

# COMMAND ----------

write_query = (df.writeStream.format("delta").option("checkpointLocation", f"{source_dir}/chekpoint").option("mergeSchema", "true").outputMode("append").trigger(availableNow = True).toTable("stream.invoices_raw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stream.invoices_raw
# MAGIC

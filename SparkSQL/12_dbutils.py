# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.mount("","")
#dbutils.fs.unmounts("/mnt/data")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls '/databricks-datasets'

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC tail '/databricks-datasets/airlines/part-0000'

# COMMAND ----------

# MAGIC %lsmagic
# MAGIC

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/emp.csv")
dbutils.fs.ls("/FileStore/tables")
dbutils.fs.mkdirs("/FileStore/tables/Databricks202508")
#dbutils.fs.rm("/FileStore/tables/Databricks202506",True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/FileStore/tables/Databricks202508'

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/wordcount.txt")

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

dbutils.notebook.run("/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data",120)

# COMMAND ----------

<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes

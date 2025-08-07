# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("Deptno","10",["10","20","30"])

# COMMAND ----------

dbutils.widgets.text("deptno","")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text parametername default "";

# COMMAND ----------

value=getArgument("parametername")
print(value)


# COMMAND ----------

deptno=getArgument("deptno")
print(deptno)

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.format("csv").option("header","true").option("inferSchema","true").\
    load("/FileStore/tables/emp12.csv")

param=dbutils.widgets.get("deptno")
print(param)
df.filter(col("deptno")==dbutils.widgets.get("deptno")).show()

# COMMAND ----------

df.show()

# COMMAND ----------

dbutils.widgets.removeAll()

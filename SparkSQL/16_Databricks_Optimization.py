# Databricks notebook source
# MAGIC %sql
# MAGIC create table default.managed_emp
# MAGIC (
# MAGIC   empno int,
# MAGIC   ename string,
# MAGIC   sal int
# MAGIC )
# MAGIC using delta
# MAGIC location '/mnt/data/emp';

# COMMAND ----------

# MAGIC %md
# MAGIC #### 02_Always inferschema while reading files
# MAGIC * Best practice while reading the external files into spark is to inferschema explicitely while reading the file. if you not provide schema, spark will check the inside data and allocate the schema and which is costly operation

# COMMAND ----------

df=spark.read.format("csv").option("header","true").\
  option("inferSchema","true").load("emp.csv")

  #emp.csv -100GB
df=spark.read.format("csv").option("header","true").\
  option(schema,emp_schema).load("emp.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 03_All purpose cluster Vs job cluster
# MAGIC * We should use all purpose cluster for the development work
# MAGIC * use job cluster to schedule pipelines or work flows in production environment
# MAGIC * Job cluster cost is less i.e 0.07$ per DBU
# MAGIC * All perpose cluster is costly than job cluster
# MAGIC * cost is 0.4$ per DBU

# COMMAND ----------

# MAGIC %md
# MAGIC #### 04_Reduce the idle time
# MAGIC * By default, cluter idle time will be 1.30 hrs. in order to reduce the overall cost of cluster, we need to reduce idle time to 15 mins

# COMMAND ----------

# MAGIC %md
# MAGIC #### 05_Use bigdata file formats instead of row level file formats
# MAGIC
# MAGIC 1. Parquet
# MAGIC 2. ORC
# MAGIC 3. avro
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####06_Broadcast the smaller table
# MAGIC *  When one of the tables involved in a join is way smaller than the others, we can broadcast it to all the executors, so it will be fully present there and Spark will be able to join it to the other table partitions without shuffling it
# MAGIC
# MAGIC *  However, the default table size threshold is 10MB. We can increase this number by tuning the spark.sql.autoBroadcastJoinThreshold
# MAGIC
# MAGIC * Not recommended to increase broadcast capacity much since it puts the driver under pressure and it can result in OOM errors
# MAGIC *  Moreover, this approach increases the IO between driver and executors

# COMMAND ----------

# MAGIC %md
# MAGIC ####6.1. Shuffle Hash Join:
# MAGIC * When ever your joining 2 huge tables, we will use shuffle hash join hint.
# MAGIC
# MAGIC In shuffle hash join, data in dataframes will be arranged based on hash key and join will be performed.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 07_Data Partitioning
# MAGIC *  Whenever Spark processes data in memory, it breaks that data down into parts, and these parts are processed in the cores of the executors. These are the Spark partitions. Let’s examine how to examine the partitions for any given DataFrame.
# MAGIC
# MAGIC *  Suppose you have a large dataset with customer information, and you often filter data based on customer IDs. 
# MAGIC Partition the data by customer ID to improve query performance
# MAGIC * Use the partitionBy transformation to distribute data across partitions based on a specific column

# COMMAND ----------

df.write.format("parquet").mode("overwrite").partitionBy("deptno").save("path")



# COMMAND ----------

# MAGIC %sql
# MAGIC Transaction tables (fact table)-createdate,updatedt,orderdate (10)
# MAGIC master tables (dim)

# COMMAND ----------

# MAGIC %md
# MAGIC #####***********

# COMMAND ----------

# MAGIC %md
# MAGIC #### 08_use coalesce and repartitions whereever required
# MAGIC * before writing dataframe to target, it is recommended to reduce number of partitions 
# MAGIC
# MAGIC * coalesce(): 
# MAGIC
# MAGIC Using this, we will reduce the no of partition in the dataframe.
# MAGIC Coalesce will improve performance by reducing no.of partitions
# MAGIC It will not cause data shuffling
# MAGIC
# MAGIC Syntax:
# MAGIC  df.coalesce(10)
# MAGIC
# MAGIC  * repartition():
# MAGIC
# MAGIC  Using rp, we can increase or decrease no of partitions.
# MAGIC
# MAGIC  It will cause lot of data shuffling between the partitions.
# MAGIC
# MAGIC  100 --- 200 Partitons
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

Step1-100
step2 - 500
step3- 1000
step4= step3df.coalsce(100
)



# COMMAND ----------

p1-1gb-task1
p2-1gb-task2
p3-30GB-task3
p4-1gb-task4

# COMMAND ----------

df.coalsce(1).write.format("parquet").mode("overwrrite").save("emp.parquet")

# COMMAND ----------


dbutils.fs.ls('/databricks-datasets/')

# COMMAND ----------

df=spark.read.format("csv").option("header","true").load("dbfs:/databricks-datasets/weather")

df.cache()

# COMMAND ----------

Oracle ---24 JOB (2TB)
20 mins




# COMMAND ----------

df.rdd.getNumPartitions()


# COMMAND ----------

df1=df.repartition(5)


# COMMAND ----------

df1.rdd.getNumPartitions()

# COMMAND ----------

step1 - 200 p
step2 - 400 p
step3 - step3.coalesce(100)
step4 -
step5

# COMMAND ----------

Repartiton:
  We can increase or decrease no of partitions.

  df.repartition(400)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 09_Cache & Persist
# MAGIC * use cache and presist to reduce io operations between the transformations

# COMMAND ----------

# MAGIC %md
# MAGIC Step1
# MAGIC Step2.cache(memory_only) 
# MAGIC Step3
# MAGIC Step4(2)
# MAGIC Step5(2)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark import StorageLevel

df.persist() or df.cache()

df.persist(StorageLevel.DISK_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY) ==cache()
df.persist(StorageLevel.)
#df.persist(StorageLevel.DISK_ONLY)
#df.persist(StorageLevel.DISK_ONLY)
#df.persist(StorageLevel.DISK_ONLY)
#df.persist(StorageLevel.DISK_ONLY)


# COMMAND ----------

step1 - df1.cache()
step2 - 
step3 - df3.cache()
step4 -
step5 -

# COMMAND ----------

# MAGIC %md
# MAGIC ####10. Adaptive Query Execution (AQE):
# MAGIC * Enable Adaptive Query Execution to dynamically optimize the query plan at runtime based on the actual data characteristics, such as adjusting join strategies and optimizing shuffle partitions.
# MAGIC
# MAGIC
# MAGIC SET spark.sql.adaptive.enabled = true;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Shuffle Partition Size:
# MAGIC
# MAGIC Adjust the shuffle partition size based on the size of the data being processed. A common default is 200 partitions, but tuning this number (via spark.sql.shuffle.partitions) based on data size can improve performance.
# MAGIC

# COMMAND ----------

#1000GB
#200P

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "500")

# COMMAND ----------

# MAGIC %md
# MAGIC ####12. Predicate Pushdown:
# MAGIC * Use filter() early in your query to allow predicate pushdown. This reduces the amount of data read and processed.
# MAGIC
# MAGIC df.filter(df["column"] > 100)  # Push the filter early

# COMMAND ----------

df=spark.read.format("csv").option("header","true").load("dbfs:/databricks-datasets/weather")
# 100 columns,100GB
# Dept : SAP,ORACLE,SQLSERVER,JAVA,PYTHON
df2=df.filter(col("dname")=="SAP")  --20GB






# COMMAND ----------

# MAGIC %md
# MAGIC ####13. VACUUM:
# MAGIC
# MAGIC * Periodically run VACUUM to delete old data files that are no longer referenced by Delta Lake. This helps to free up storage and improve performance.
# MAGIC
# MAGIC VACUUM delta.`/path/to/table` RETAIN 168 HOURS  # Retain files for 7 days
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####14. Use DataFrames and Datasets & Avoid Shuffles:
# MAGIC
# MAGIC * Spark Core - RDD
# MAGIC * Spark SQL - Dataframe
# MAGIC
# MAGIC *  Prefer DataFrames/Datasets over RDDs as they provide optimizations via Catalyst Optimizer and Tungsten execution engine, which optimize query plans and memory usage.
# MAGIC
# MAGIC
# MAGIC *  Shuffles (data redistribution across partitions) are costly. Minimize shuffles by avoiding operations like groupBy(), join(), or distinct() unless necessary. If shuffles are required, make sure to optimize partitioning and cache intermediate results to minimize the impact.

# COMMAND ----------

df.distinct()
df.groupBy()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct empno from emp; --2
# MAGIC select  empno from emp;--1sec

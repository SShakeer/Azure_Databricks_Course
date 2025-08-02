# Databricks notebook source
Delta Lake is an open-source storage layer that brings reliability to data lakes by adding a transactional storage layer on top of data stored in cloud storage (on AWS S3, Azure Storage, and GCS). It allows for ACID transactions, data versioning, and rollback capabilities. It allows you to handle both batch and streaming data in a unified way.

Delta tables are built on top of this storage layer and provide a table abstraction, making it easy to work with large-scale structured data using SQL and the DataFrame API.

# COMMAND ----------

# MAGIC %md
# MAGIC * Delta Lake is the technology at the heart of the Databricks Lakehouse platform. It is an open source 
# MAGIC technology that enables building a data lakehouse on top of existing storage systems. 
# MAGIC
# MAGIC * While Delta Lake was initially developed exclusively by Databricks, it's been open sourced for almost 3 
# MAGIC years.
# MAGIC
# MAGIC * Delta Lake is optimized for cloud object storage
# MAGIC * Decoupling storage and compute
# MAGIC
# MAGIC * Rather than locking you into a traditional database system where cost continue to scale as your data increases in size, Delta Lake decouples computing and storage costs and provides 
# MAGIC optimized performance on data regardless of scale

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Features:
# MAGIC * Atomicity:
# MAGIC   In Delta Lake, every transaction is atomic, meaning it either completes entirely or not at all.
# MAGIC   This is critical when performing large batch jobs or streaming operations.
# MAGIC
# MAGIC
# MAGIC * Consistancy:
# MAGIC   Delta Lake guarantees consistency by enforcing constraints on data. Once a transaction is committed, Delta Lake ensures that the data adheres to a valid schema and any other rules applied to it.
# MAGIC
# MAGIC
# MAGIC * Isolation:
# MAGIC   transactions appear to execute one after another, even if multiple users are performing operations concurrently.
# MAGIC   
# MAGIC * Durability:
# MAGIC   
# MAGIC   If you update a Delta table and commit the transaction, the changes are saved to the underlying storage system. In case of any system failures, Delta Lake can recover from the log and restore the consistent state of the table.
# MAGIC
# MAGIC   * Unified Batch and Streaming Data:
# MAGIC   
# MAGIC    Delta Lake is designed from the ground up to allow a single system 
# MAGIC to support both batch and stream processing of data.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### HIVE:
# MAGIC
# MAGIC * Databricks uses a Hive metastore by default to register databases, tables, and views. Apache Hive is a 
# MAGIC distributed, fault-tolerant data warehouse system that enables analytics at a massive scale, allowing users to 
# MAGIC read, write, and manage petabytes of data using SQL. 
# MAGIC
# MAGIC * Hive is built on top of Apache Hadoop, which is an open-source framework used to efficiently store and 
# MAGIC process large datasets. As a result, Hive is closely integrated with Hadoop, and is designed to work quickly 
# MAGIC on petabytes of data. What makes Hive unique is the ability to query large datasets, leveraging Apache Tez 
# MAGIC or MapReduce, with a SQL-like interface. 

# COMMAND ----------

dbutils.fs.ls("/user/hive/warehouse/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delta Internals
# MAGIC * directory contains a number of Parquet data files and a directory named _delta_log
# MAGIC * Records in Delta Lake tables are stored as data in Parquet files
# MAGIC *  Each transaction results in a new JSON file being written to the Delta
# MAGIC

# COMMAND ----------

#display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC * Uses of the transaction log:
# MAGIC -----------------------------
# MAGIC
# MAGIC Rather than overwriting, Delta Lake uses the 
# MAGIC transaction log to indicate whether or not files are valid in a current version of the table. When we query a 
# MAGIC Delta Lake table, the query engine uses the transaction logs to resolve all the files that are valid in the current 
# MAGIC version, and ignores all other data files.

# COMMAND ----------

#display(spark.sql(f"SELECT * FROM 
#json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

# COMMAND ----------

# MAGIC %md
# MAGIC * Internal or managed tables:
# MAGIC
# MAGIC   Databricks manages both the metadata and the data for a managed table.
# MAGIC
# MAGIC * Managed tables are the default 
# MAGIC * when creating a table. The data for a managed table resides in the LOCATION of the database it is registered to.
# MAGIC
# MAGIC * DESCRIBE EXTENDED managed_table_in_db;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

* External Tables:
  -----------------
  
* Databricks only manages the metadata for unmanaged (external) tables; when you drop a table, you do not 
affect the underlying data. Unmanaged tables will always specify a LOCATION during table creation



# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC USE db_name_default_location; 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS ( 
# MAGIC path = '${da.paths.working_dir}/flights/departuredelays.csv', 
# MAGIC header = "true", 
# MAGIC SELECT * FROM temp_delays; 
# MAGIC mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed 
# MAGIC lines are encountered 
# MAGIC ); 
# MAGIC CREATE OR REPLACE TABLE external_table LOCATION 'path/external_table' AS 
# MAGIC SELECT * FROM external_table; 
# MAGIC
# MAGIC */

# COMMAND ----------

# writing df as external table

'''df.write.option("path", "/path/to/empty/directory").saveAsTable("table_name") '''


# COMMAND ----------

# DBTITLE 1,Create a table
All tables created on Azure Databricks use Delta Lake by default.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create database if not exists azure;
# MAGIC --create schema if not exists azure;

# COMMAND ----------



# COMMAND ----------

#dbutils.fs.rm("/user/hive/warehouse/azure.db",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC use azure;
# MAGIC create table if not exists test123
# MAGIC (
# MAGIC id int,
# MAGIC name string
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/user/hive/warehouse/azure.db/test123")

# COMMAND ----------

dbutils.fs.ls("/user/hive/warehouse/azure.db/test123/_delta_log")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc history test123;

# COMMAND ----------

# MAGIC %sql
# MAGIC use azure;
# MAGIC insert into test123 values (100,'ABC')

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into test123 values (200,'BALR')

# COMMAND ----------

# MAGIC %sql
# MAGIC desc formatted test123;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;
# MAGIC --show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table test_external
# MAGIC (
# MAGIC   id int,
# MAGIC   ename string
# MAGIC )
# MAGIC location '/mnt/raw/test';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.azure.test123

# COMMAND ----------

# MAGIC %sql
# MAGIC create table sample
# MAGIC (
# MAGIC   id int not null,
# MAGIC   
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from hive_metastore.azure.test;
# MAGIC
# MAGIC delete from test where id=100;

# COMMAND ----------

dbutils.fs.ls('user/hive/warehouse/azure.db')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls("databricks-datasets")

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/learning-spark-v2/people")

# COMMAND ----------

# DBTITLE 1,Using Python
# Load the data from its source.
df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

# Write the data to a table.
#table_name = "people_10m"

df.write.format("delta").mode("overwrite").saveAsTable("azure.people_10m")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from azure.people_10m

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS people_10m;

# COMMAND ----------

# DBTITLE 1,Using SQL
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS people_10m
# MAGIC AS 
# MAGIC SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from default.people_10m

# COMMAND ----------

# DBTITLE 1,creating empty table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS people20m (
# MAGIC   id INT,
# MAGIC   firstName STRING,
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC );

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/people20m",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc history people20m;
# MAGIC

# COMMAND ----------

# DBTITLE 1,create empty table using python
from delta.tables import *
# Create table in the metastore
DeltaTable.createIfNotExists(spark) \
  .tableName("default.people30m") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .execute()

'''
# Create or replace table with path and add properties
DeltaTable.createOrReplace(spark) \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .property("description", "table with people data") \
  .location("/tmp/delta/people30m") \
  .execute()
  '''

# COMMAND ----------

# DBTITLE 1,Inserting
# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW people_updates (
# MAGIC   id, firstName, middleName, lastName, gender, birthDate, ssn, salary
# MAGIC ) AS VALUES
# MAGIC   (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
# MAGIC   (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
# MAGIC   (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
# MAGIC   (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
# MAGIC   (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
# MAGIC   (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from people_updates

# COMMAND ----------

'''
MERGE INTO people20m
USING people_updates
ON people20m.id = people_updates.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into people20m values (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25', '567-89-0123', 89900);

# COMMAND ----------

# DBTITLE 1,Read a table
people_df = spark.read.table("azure.people_10m")

display(people_df)

## or

#people_df = spark.read.load(table_path)

#display(people_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM people_10m;
# MAGIC
# MAGIC --SELECT * FROM delta.`<path-to-table`;

# COMMAND ----------

# DBTITLE 1,Write to a table
# MAGIC %sql
# MAGIC INSERT INTO people10m SELECT * FROM more_people

# COMMAND ----------

df.write.mode("append").saveAsTable("people10m")

# COMMAND ----------

# MAGIC %sql
# MAGIC --delete from people_10m where deptno=10;
# MAGIC truncate table people_10m;
# MAGIC insert into people_10m
# MAGIC select 
# MAGIC id,
# MAGIC firsname,
# MAGIC middlename
# MAGIC from people_10m;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE people10m 
# MAGIC SELECT * FROM more_people

# COMMAND ----------

# DBTITLE 1,Update a table SQL Approache
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC UPDATE people_10m SET gender = 'Female' WHERE gender = 'F';
# MAGIC UPDATE people_10m SET gender = 'Male' WHERE gender = 'M';
# MAGIC
# MAGIC
# MAGIC
# MAGIC --UPDATE delta.`/tmp/delta/people-10m` SET gender = 'Female' WHERE gender = 'F';
# MAGIC --UPDATE delta.`/tmp/delta/people-10m` SET gender = 'Male' WHERE gender = 'M';

# COMMAND ----------

# DBTITLE 1,Update python approache
from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/tmp/delta/people-10m')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "gender = 'F'",
  set = { "gender": "'Female'" }
)

# Declare the predicate by using Spark SQL functions.
deltaTable.update(
  condition = col('gender') == 'M',
  set = { 'gender': lit('Male') }
)

# COMMAND ----------

# DBTITLE 1,DELETE
DELETE FROM people10m WHERE birthDate < '1955-01-01'

DELETE FROM delta.`/tmp/delta/people-10m` WHERE birthDate < '1955-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY test123

# COMMAND ----------

# DBTITLE 1,Query an earlier version of the table (time travel)
# MAGIC %sql
# MAGIC SELECT * FROM test123 VERSION AS OF 2;
# MAGIC --SELECT * FROM people_10m VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history test10;

# COMMAND ----------

df1 = spark.read.format('delta').option('timestampAsOf', '2025-06-03T02:25:12.000+00:00').table("azure.test123")

display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC --time travelling
# MAGIC SELECT * FROM test123 TIMESTAMP AS OF '2025-06-03T02:25:12.000+00:00'

# COMMAND ----------

df2 = spark.read.format('delta').option('versionAsOf', 0).table("people_10m")

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC VACCUM:
# MAGIC
# MAGIC VACUUM removes all files from the table directory that are not managed by Delta, as well as data files that are no longer in the latest state of the transaction log for the table and are older than a retention threshold
# MAGIC
# MAGIC
# MAGIC Syntax:*****
# MAGIC
# MAGIC VACUUM peope_10m [RETAIN 720 HOURS]
# MAGIC
# MAGIC --to improve performance by deleting un used delta files
# MAGIC

# COMMAND ----------

# DBTITLE 1,Clean up snapshots with VACUUM
# MAGIC %sql
# MAGIC VACUUM people_10m

# COMMAND ----------

# DBTITLE 1,Incremental_load
data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Gani", "company20",2345],
        [40, "RAM", "Microst",7889], 
        [50, "Chiru", "Oracle",50000],
       [60,"ManiSharma","CTS",2000],
       [70,"Thaman","CTS",20000],
       [80,"RCB","Databricks",30000],
       [90,"BLR","Databricks",30000],
       ]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

#dbutils.fs.rm("dbfs:/user/hive/warehouse/emp_target",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table emp_target
# MAGIC (
# MAGIC empno int,
# MAGIC ename string,
# MAGIC org string,
# MAGIC sal int
# MAGIC );

# COMMAND ----------

df.createOrReplaceTempView("emp_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from emp_source;
# MAGIC
# MAGIC --select * from emp_target;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO emp_target
# MAGIC USING emp_source
# MAGIC ON emp_source.empno = emp_target.empno
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET 
# MAGIC emp_target.empno=emp_source.empno,
# MAGIC emp_target.ename=emp_source.ename,
# MAGIC emp_target.org=emp_source.org,
# MAGIC emp_target.sal=emp_source.sal
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from emp_target;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO emp_target1
# MAGIC USING emp_source
# MAGIC ON emp_source.empno = emp_target1.empno
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET 
# MAGIC emp_target1.empno=emp_source.empno,
# MAGIC emp_target1.ename=emp_source.ename,
# MAGIC emp_target1.org=emp_source.org,
# MAGIC emp_target1.sal=emp_source.sal
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC Insert into emp_target1 values(emp_source.empno,emp_source.ename,emp_source.org,emp_source.sal);
# MAGIC
# MAGIC

# COMMAND ----------

select * from emp_target1

# COMMAND ----------

# DBTITLE 1,dataframe approache
# MAGIC %sql
# MAGIC create table emp2_target
# MAGIC (
# MAGIC empno int,
# MAGIC ename string,
# MAGIC org string,
# MAGIC sal int
# MAGIC ) using delta
# MAGIC location "/FileStore/tables/merge_target2"

# COMMAND ----------

data = [[10, "Najeeb", "company10",1000],
        [20, "Reddy", "company18",2000], 
        [30, "Gani", "company20",2345],
        [40, "Ramdev", "company11",7889], 
        [50, "Chiru", "company14",8990],
       [60,"Maheer","Oracle",40200]]

columns = ['empno', 'ename', 'org','sal']
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.createOrReplaceTempView("emp_source1")

# COMMAND ----------

from delta.tables import *
df_delta=DeltaTable.forPath(spark,"/FileStore/tables/merge_target2")

df_delta.alias("target").merge(
source =df.alias("source"),
  condition="target.empno=source.empno"
).whenMatchedUpdate(set=
                   {
                     "empno":"source.empno",
                     "ename":"source.ename",
                     "org":"source.org",
                     "sal":"source.sal"
                   }).whenNotMatchedInsert(values=
                                          {
                                            "empno":"source.empno",
                                            "ename":"source.ename",
                                            "org":"source.org",
                                            "sal":"source.sal"
                                          }
                                          ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from catalog.db.emp2_target

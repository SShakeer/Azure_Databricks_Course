# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

emp_df.filter("deptno==10").show()

# COMMAND ----------

from pyspark.sql.functions import *
df2=emp_df.withColumn("city",lit("Bangalore")).withColumn("state",lit("KA"))


df3=df2.withColumnRenamed("mgr","manager").withColumnRenamed("hiredate","Jdate")

# COMMAND ----------

from pyspark.sql.functions import *
emp_df2=emp_df.withColumnRenamed("mgr","manager").withColumnRenamed("hiredate","joining_date").withColumn("city",lit("Bangalore")).withColumn("new_salary",col('sal')*10)

# COMMAND ----------

emp_df.display()

# COMMAND ----------

emp_df.select("*").distinct().count()

# COMMAND ----------

emp_df.dropDuplicates()

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Pyspark code to find dept wise total salary

# COMMAND ----------

# MAGIC %sql
# MAGIC select deptno,sum(sal) as Total from emp group by deptno;

# COMMAND ----------

emp_df2=emp_df.groupBy('deptno').agg(sum(col("sal")).alias("total")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write Pyspark code to find max salary from each dept

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select deptno,max(sal) as maximum from emp group by deptno;
# MAGIC

# COMMAND ----------

df_groping=emp_df.groupBy('deptno').agg(max(col("sal")).alias("max_salary")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select deptno,min(sal) as min_salary,
# MAGIC max(sal) as max_salary,
# MAGIC avg(sal) as avg_salary,
# MAGIC sum(sal) as total_salary,
# MAGIC count(*) as total_emp_count from emp group by deptno;

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val df=spark.read.format("csv").option("header","true").load("/FileStore/tables/emp12.csv")
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

df=emp_df.groupBy("deptno").agg(max("sal").alias("Max_salary"),min("sal").alias("min_salary"),sum("sal").alias("total_salary"),avg("sal").alias("avg_salary"),count("empno").alias("total_count"))

#df.show()


df2=df.filter(col("total_count")>3)
df2.show()

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select deptno,
# MAGIC sum(sal) as Total_salary,
# MAGIC max(sal) as max_salary,
# MAGIC min(sal) as min_sal ,
# MAGIC avg(sal) as avg_sal,
# MAGIC count(empno) as emp_count
# MAGIC from emp 
# MAGIC group by deptno
# MAGIC having count(empno)>3

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/rank_output.csv")
#dbutils.fs.rm("/FileStore/tables/rank_output.csv",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(sal) from emp

# COMMAND ----------

emp_df.agg(sum("sal").alias("Total")).show()

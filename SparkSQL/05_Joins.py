# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

emp_df.alias('c').select('c.*').show()

# COMMAND ----------

emp_df2=emp_df.withColumnRenamed("deptno","deptno_emp")

# COMMAND ----------

# DBTITLE 1,INNER JOIN OR EQUI JOIN
#inner join


emp12=emp_df2.join(dept_df, emp_df2.deptno_emp == dept_df.deptno,"inner")


emp12.display()

emp13=emp12.select("job","dname","ename","sal","loc").show()

# COMMAND ----------

#df_join=emp_df.join(dept_df,"deptno")
df_join=emp_df.join(dept_df,emp_df["deptno"]==dept_df["deptno"],"inner")

# COMMAND ----------

df_join.select("ename","empno","sal","dname","loc").show()

# COMMAND ----------

# as both data frames have deptno using same name, we can pass column name as string as well
emp_df. \
    join(dept_df, 'deptno'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno). \
    select(emp_df['*'], dept_df['loc'], dept_df['dname']). \
    show()

# COMMAND ----------

# using alias names
emp_df.alias('e'). \
    join(dept_df.alias('d'), emp_df.deptno == dept_df.deptno). \
    select('e.*', 'dname', 'loc'). \
    show()

# COMMAND ----------

# DBTITLE 1,display dept wise employee count
emp_df.alias('e'). \
    join(dept_df.alias('d'), emp_df.deptno == dept_df.deptno). \
    groupBy('d.dname','d.loc'). \
    count(). \
    show()


# COMMAND ----------

#emp_df.groupBy("deptno").count().show()
emp_df.groupBy("deptno").min('sal').show()

# COMMAND ----------

# DBTITLE 1,LEFT OUTER
emp_df. \
    join(dept_df, 'deptno', 'left_outer').\
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno==dept_df.deptno, 'left').\
    show()

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")
dept_df.createOrReplaceTempView("dept")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC e.ename,
# MAGIC e.sal,
# MAGIC e.deptno,
# MAGIC d.dname,
# MAGIC d.loc from emp e inner join dept d on e.deptno=d.deptno;
# MAGIC

# COMMAND ----------

query=spark.sql("select e.ename,e.sal,e.deptno,d.dname, d.loc from emp e left outer join dept d on e.deptno=d.deptno")


# COMMAND ----------

# left or left_outer or leftouter are same.

emp_df. \
    join(dept_df, 'deptno', 'left'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, 'deptno', 'left'). \
    show()

# COMMAND ----------

df1=emp_df.coalesce(1)
df2=dept_df

df3=df1.join(df2, df1.deptno == df2.deptno, 'left')

df4=df3.filter('ename IS NOT NULL')

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'left'). \
    select(emp_df['*'], dept_df['dname'], dept_df['loc']). \
    show()

# COMMAND ----------

emp_df.alias('e'). \
    join(dept_df.alias('d'), emp_df.deptno == dept_df.deptno, 'left'). \
    filter('d.dname IS NOT NULL'). \
    select('e.*', 'dname', 'loc').show()
    #show()

# COMMAND ----------

# DBTITLE 1,RIGHT OUTER
emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'right'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'right_outer'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, 'deptno', 'right'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'right'). \
    select(emp_df['*'], dept_df['dname'], dept_df['loc']). \
    show()

# COMMAND ----------

'''
from pyspark.sql.functions import *
dept_df.alias('d'). \
    join(emp_df.alias('e'), emp_df.deptno == dept_df.deptno, 'right'). \
    groupBy('d.deptno'). \
    agg(sum(when(emp_df['sal'].isNull(), 0).otherwise(emp_df['sal'])).alias('total_salary')). \
    show()'''

# COMMAND ----------

# DBTITLE 1,FULL OUTER
emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'fullouter'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, emp_df.deptno == dept_df.deptno, 'full_outer'). \
    show()

# COMMAND ----------

emp_df.join(dept_df, 'deptno', 'left'). \
    union(
        emp_df. \
            join(dept_df, 'deptno', 'right')
    ). \
    distinct(). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC Suitable for scenarios where one of the datasets is small enough to be broadcasted to all executor nodes. The smaller dataset is broadcasted to all nodes in the cluster, and then each node joins the broadcasted dataset with partitions of the larger dataset locally. Efficient when one dataset is significantly smaller than the other.

# COMMAND ----------

# DBTITLE 1,BROAD CAST JOIN

from pyspark.sql.functions import broadcast
# df1=big
# df2=small 

emp_df.join(broadcast(dept_df), emp_df.deptno == dept_df.deptno).show()

# COMMAND ----------

# DBTITLE 1,CROSS JOIN

# number of records in first data frame multipled by number of records in second data frame
emp_df. \
    crossJoin(dept_df). \
    count()

# COMMAND ----------

emp_df. \
    join(dept_df, how='inner'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * A left semi join in Spark SQL is a type of join operation that returns only the columns from the left dataframe that have matching values in the right dataframe. 
# MAGIC
# MAGIC It is used to find the values in one dataframe that have corresponding values in another dataframe

# COMMAND ----------

# DBTITLE 1,leftsemi


emp_df. \
    join(dept_df, 'deptno', 'leftsemi'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * When you join two DataFrames using Left Anti Join (leftanti), it returns only columns from the left DataFrame for non-matched records.

# COMMAND ----------

dept_df. \
    join(emp_df, 'deptno', 'leftanti'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, 'deptno', 'leftanti'). \
    show()

# COMMAND ----------

emp_df. \
    join(dept_df, 'deptno', 'rightsemi'). \
    show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### shuffle hash join
# MAGIC
# MAGIC it will map through MR fundamentals
# MAGIC map through two different dataframes
# MAGIC use the fields in the join condition as output keys
# MAGIC shuffle both dataframes using output keys
# MAGIC in the reduce phase , join the two datasets , now any of the rows of the both tables with same key are on the same machine
# MAGIC and are sorted.
# MAGIC
# MAGIC we will use when 1 is huge tables and another is little bit huge.
# MAGIC
# MAGIC Efficient when one dataset is smaller but not small enough to broadcast.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC how to detect shuffle problems
# MAGIC
# MAGIC 1. check the spark ui pages for task level detils
# MAGIC 2. tasks take more time to execute than others
# MAGIC 3. speculative task will be launching
# MAGIC

# COMMAND ----------

#df1 = spark.read.csv("large_dataset.csv", header=True) 
#df2 = spark.read.csv("medium_dataset.csv", header=True) 
joinedDF = emp_df.hint("SHUFFLE_HASH").join(dept_df, "deptno") 

joinedDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sort Merge Join
# MAGIC
# MAGIC Usecase: 2 big dfs
# MAGIC
# MAGIC Default join strategy for large datasets that do not fit into memory. Both datasets are sorted by the join key and then merged. This requires a shuffle to sort the data, which can be expensive. Suitable for large datasets where both tables are too large to broadcast

# COMMAND ----------

Syntax:
df1 = spark.read.csv("dataset1.csv", header=True) 
df2 = spark.read.csv("dataset2.csv", header=True)

joinedDF = df1.join(df2, "key") 

joinedDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Approache of JOINS

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")
dept_df.createOrReplaceTempView("dept")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC e.ename,
# MAGIC e.deptno,
# MAGIC e.sal,
# MAGIC d.dname,
# MAGIC d.loc from emp e inner join dept d on e.deptno=d.deptno;
# MAGIC

# COMMAND ----------

df=spark.sql("select e.ename,e.deptno,e.sal,d.dname,d.loc from emp e inner join dept d on e.deptno=d.deptno")

# COMMAND ----------



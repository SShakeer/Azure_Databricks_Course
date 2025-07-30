# Databricks notebook source
# MAGIC %run "/Users/basha.ora11@gmail.com/AzureDatabricksCourse/data"

# COMMAND ----------

help(emp_df.select)

# COMMAND ----------

emp_df.show(5)

# COMMAND ----------

emp_df.select('*').show()

# COMMAND ----------

emp_df.select('empno', 'ename', 'hiredate','sal').show()
#df2.show()

# COMMAND ----------

emp_df.select(['empno', 'ename', 'sal']).show()

# COMMAND ----------

emp_df.alias('e').select('e.*').show()

# COMMAND ----------

dept_df.alias('d').select('d.deptno', 'd.dname', 'd.loc').show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import *
emp_df.select(col("ename"),col("sal"),col("deptno"),col("hiredate"))

# COMMAND ----------

from pyspark.sql.functions import col
emp_df.select(col("ename"),col("sal").alias("salary"),col("deptno"),col("hiredate")).show()

# COMMAND ----------

from pyspark.sql.functions import col,min,max
#dept_df.select(col('deptno'), col('dname'), col('loc')).show()
dept_df.select(col('deptno'), col('dname'), col('loc').alias("location")).show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------


emp_df.select(
    col('empno'), 
    'ename', 
    'job',
    lit('Banaglore').alias('city'),
    concat('ename', lit(' and his job is '), 'job').alias('new_name')
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## selectExpr

# COMMAND ----------

help(emp_df.selectExpr)

# COMMAND ----------

emp_df.selectExpr('*').show()

# COMMAND ----------

# Defining alias to the dataframe
emp_df.alias('e').selectExpr('e.*').show()

# COMMAND ----------

emp_df.selectExpr('empno', 'job', 'hiredate').show()

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select empno,
# MAGIC job,hiredate,sal*10 as Total from emp

# COMMAND ----------

emp_df.selectExpr('empno', 'job', 'hiredate',"sal*10 as total").show()

# COMMAND ----------

emp_df.select(emp_df['sal'], emp_df['deptno']).show()

# COMMAND ----------

emp_df.selectExpr('sal', 'deptno', 'comm').show()

# COMMAND ----------

#300
colums1 = ['deptno', 'sal', 'comm','hiredate']

emp_df.select(*colums1).show()

# COMMAND ----------

from pyspark.sql.functions import date_format
df2=emp_df.select('sal', col('deptno'), 'comm','hiredate',date_format('hiredate','yyyyMMdd').alias("hdate"),date_format('hiredate','yyyy').alias("year"))
df2.show()

# COMMAND ----------

df13=emp_df.select('sal', col('deptno'), 'comm',date_format('hiredate','yyyyMMdd').cast('int').alias("hdate")).printSchema()

# COMMAND ----------

emp_df.createOrReplaceTempView("emp")
dept_df.createOrReplaceTempView("dept")

# COMMAND ----------

df2=spark.sql("""select 
ename,
sal,
d.deptno,
d.dname
 from emp e inner join dept d on e.deptno=d.deptno""")


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Write PySpark Code to filter 10 dept data and save output as csv file?

# COMMAND ----------


from pyspark.sql.functions import *

df=spark.read.format("csv").option("header",True).option("inferSchema",True).load("dbfs:/FileStore/tables/emp12.csv")

df_select=df.select("name","salary","comm","deptno")

df_filter=df_select.filter(col("deptno")==10)

df_filter.write.format("csv").mode("overwrite").save("dbfs:/FileStore/tables/azureoutput.csv")


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/azureoutput.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME and Create new columns

# COMMAND ----------

emp_df.columns

# COMMAND ----------

df2=emp_df.withColumn("city",lit("Bangalore")).withColumn("state",lit("KA")).withColumn("today",current_timestamp()).withColumn("today2",current_date())


df2.display()




# COMMAND ----------

df3=df2.withColumnRenamed("mgr","Manager").withColumnRenamed("hiredate","Joiningdate").display()

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/FileStore/tables/emp12.csv")

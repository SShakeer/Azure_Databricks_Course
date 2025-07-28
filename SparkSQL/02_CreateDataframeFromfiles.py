# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC * Reading files using direct APIs such as csv, json, etc under spark.read.
# MAGIC * Reading files using format and load under spark.read.
# MAGIC * Specifying options as arguments as well as using functions such as option and options.
# MAGIC * Supported file formats.
# MAGIC * csv - source ,|$
# MAGIC * text
# MAGIC * json
# MAGIC * parquet - emp.parquet (500GB, 250GB)
# MAGIC * orc - 
# MAGIC * Other common file formats.
# MAGIC * xml
# MAGIC * avro - when ever your data schema will change , that time we will go for avro
# MAGIC * Important file formats for certification - csv, json, parquet
# MAGIC * Reading compressed files

# COMMAND ----------

We can read the data from CSV files into Spark Data Frame using multiple approaches.
Approach 1: spark.read.csv('path_to_folder')
Approach 2: spark.read.format('csv').load('path_to_folder')
We can explicitly specify the schema as string or using StructType.
We can also read the data which is delimited or separated by other characters than comma.
If the files have header we can create the Data Frame with schema by using options such as header and inferSchema. It will pick column names from the header while data types will be inferred based on the data.
If the files does not have header we can create the Data Frame with schema by passing column names using toDF and by using inferSchema option.

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/emp12.csv')

# COMMAND ----------

df_emp=spark.read.format("csv").load("dbfs:/FileStore/tables/emp12.csv")

# COMMAND ----------

df_emp.show(5)

# COMMAND ----------

df_emp=spark.read.format("csv").option('header',True).option("inferSchema","true").load("dbfs:/FileStore/tables/emp12.csv")

# COMMAND ----------

df_emp.printSchema()

# COMMAND ----------

df_emp.display()

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/emp12.csv").show()

# COMMAND ----------

df_emp.display()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/FileStore/tables/'

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

emp = spark.read.csv('path')

# COMMAND ----------

df_emp.columns

# COMMAND ----------

df_emp.dtypes

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

schema10 = """
    order_id INT,
    order_date TIMESTAMP,
    order_customer_id INT,
    order_status STRING
"""

# COMMAND ----------

df2=spark.read.schema(schema10).csv('/FileStore/tables/orders')

# COMMAND ----------

df2.show()

# COMMAND ----------

df2.createOrReplaceTempView("orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from orders limit 10
# MAGIC select count(*) from orders where order_status='CLOSED'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_date(),current_schema(),current_timestamp()

# COMMAND ----------

df_emp.createOrReplaceTempView("emp")
#df_dept=createOrReplaceTempView("dept")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select deptno,sum(salary) as TotalSalary from emp group by deptno
# MAGIC order by deptno
# MAGIC --select * from emp
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

df=spark.sql("select deptno,sum(salary) as TotalSalary from emp group by deptno order by deptno")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database hadoop;
# MAGIC

# COMMAND ----------

df_emp.write.format("delta").mode("overwrite").saveAsTable("hadoop.dept_total")

# COMMAND ----------

# MAGIC %sql
# MAGIC use hadoop;
# MAGIC desc formatted dept_total;

# COMMAND ----------

#df.write.format("csv").mode("overwrite").save("dbfs:/FileStore/tables/dept0000.csv")

# COMMAND ----------

#spark.read.csv('/public/retail_db/orders', schema=schema).show()

# COMMAND ----------

#dbutils.fs.head("dbfs:/FileStore/tables/dept0000.csv/part-00000-tid-8322202638415977360-afa00ff9-aafa-4a68-8fc8-7dd942241949-64-1-c000.csv")

# COMMAND ----------

columns=['empno','hiredate','ename']

# COMMAND ----------

df=spark.read.csv('/FileStore/tables/samplecsv.csv').toDF("ename","hiredare","empno")

# COMMAND ----------

df.show()

# COMMAND ----------

'''
df=spark.read.\
  schema(schema)\
    .csv(f'/user/{username}/retail_db_pipe/orders', sep='|').show()
    '''

# COMMAND ----------

#spark.read.csv(f'/user/{username}/retail_db_pipe/orders', sep='|', schema=schema).show()

# COMMAND ----------

#Using Options while reading CSV Files into Spark Data Frame

# COMMAND ----------

#Using Options while reading CSV Files into Spark Data Frame

orders = spark. \
    read. \
    csv(
        f'dbfs:/FileStore/tables/samplecsv.csv',
        sep=',',
        header=True,
        inferSchema=True
    ). \
    toDF('empno', 'date','ename').show()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

'''
df = spark. \
    read. \
    format('csv'). \
    load(
        f'/user/{username}/retail_db_pipe/orders',
        sep='|',
        header=None,
        inferSchema=True
    ). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')
    '''

# COMMAND ----------

orders = spark. \
    read. \
    option('sep', '|'). \
    option('header', None). \
    option('inferSchema', True). \
    csv(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

options = {
    'sep': '|',
    'header': None,
    'inferSchema': True
}

# COMMAND ----------

'''orders = spark. \
    read. \
    options(**options). \
    csv(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')
    '''

# COMMAND ----------

'''orders = spark. \
    read. \
    options(**options). \
    format('csv'). \
    load(f'/user/{username}/retail_db_pipe/orders'). \
    toDF('order_id', 'order_date', 'order_customer_id', 'order_status')
    '''

# COMMAND ----------

df_emp=spark.read.format("avro").option('header',True).option('inferSchema',True).load("dbfs:/FileStore/tables/emp12.avro")

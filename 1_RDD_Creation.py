# Databricks notebook source
# MAGIC %md
# MAGIC # 1.SparkCore
# MAGIC ## 1.1.RDD
# MAGIC ### 1.1.1. How to create RDD

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Create empty RDD

# COMMAND ----------

#from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName('demo program').getOrCreate()

rdd = spark.sparkContext.emptyRDD
print(type(rdd))

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. creating RDD from dataframe

# COMMAND ----------


df=spark.createDataFrame(data =(('robert',35),('Mike',45)), schema=( 'name','age'))
new_rdd= df.rdd.collect()

print(new_rdd)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Create RDD using collections

# COMMAND ----------

#Create RDD from parallelize    
data = [1,2,3,4,5,6,7,8,9,10,200]
print(type(data))
rdd=spark.sparkContext.parallelize(data)
rdd.collect()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Create RDD using local files

# COMMAND ----------

# MAGIC %md
# MAGIC DBFS - Databricks File System

# COMMAND ----------

rdd= sc.textFile('/FileStore/tables/emp_txt2',3)
#rdd= sc.textFile('/FileStore/tables/emp_txt2')
#2 is no of partitions
rdd.collect()

# COMMAND ----------

rdd.getNumPartitions()
#128MB



# COMMAND ----------

# MAGIC %md
# MAGIC ####5.MAP
# MAGIC *  Return a new RDD by applying a function to each element of this RDD. 
# MAGIC *  Number of records in input is equal to output. 
# MAGIC

# COMMAND ----------

data = ["Project",
"Gutenberg’s",
"Alice’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s"]

rdd=spark.sparkContext.parallelize(data)
rdd2=rdd.map(lambda x: (x,1))

for element in rdd2.collect():
    print(element)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC

# COMMAND ----------



# COMMAND ----------

##Example:3

# COMMAND ----------

data = [
 (7839,'KING','PRESIDENT',1234,'1981-11-17',5000,0,10),
(7566,'JONES','MANAGER',7839,'1981-04-02',2975,0,20),
(7788,'SCOTT','ANALYST',7566,'1982-12-09',3000,0,20),
(7876,'ADAMS','CLERK',7788,'1983-01-12',1100,0,20),
(7902,'FORD','ANALYST',7566,'1981-12-03',3000,0,20),
(7369,'SMITH','CLERK',7902,'1980-12-17',800,0,20),
(7698,'BLAKE','MANAGER',7839,'1981-05-01',2850,5,30),
(7499,'ALLEN','SALESMAN',7698,'1981-02-20',1600,300,30),
(7521,'WARD','SALESMAN',7698,'1981-02-22',1250,500,30),
(7654,'MARTIN','SALESMAN',7698,'1981-09-28',1250,1400,30),
(7844,'TURNER','SALESMAN',7698,'1981-09-08',1500,0,30),
(7900,'JAMES','CLERK',7698,'1981-12-03',950,5,30),
(7782,'CLARK','MANAGER',7839,'1981-06-09',2450,5,10),
(7934,'MILLER','CLERK',7782,'1982-01-23',1300,5,10)
]

columns = ["empno","ename","job","mgr","hiredate","sal","comm","deptno"]
df = spark.createDataFrame(data=data, schema = columns)

# COMMAND ----------

print(type(data))
print(data)

# COMMAND ----------

df1 = df.rdd.map(lambda x: (x[0],x[1],x[2],x[5]))
df1.collect()

# COMMAND ----------

df2=df1.map(lambda x:(x[0],x[3]*2,x[1]))
df2.collect()

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC ###1. Creating Data Frames
# MAGIC
# MAGIC We will learn how to create dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.1.create dataframe with Range

# COMMAND ----------



# COMMAND ----------

df_emp = spark.range(1,10)

# COMMAND ----------

df_emp.show()

# COMMAND ----------

df_emp.display()
#display(df)

# COMMAND ----------

df =spark.range(10)   

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.2. createDataFrame():

# COMMAND ----------

lst1=[
    [10,"REDDY",20000,'BANGALORE'],
    [20,"GANI",30000,'CHENNAI'],
    [30,"SIVA",20000,'HYDERABAD']
  ]

schema1=['deptno','ename','salary','city']
df=spark.createDataFrame(lst1,schema1)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

lst1=[
    [10,"REDDY",20000,'BANGALORE'],
    [20,"GANI",30000,'CHENNAI'],
    [30,"SIVA",20000,'HYDERABAD']
  ]

scema1=['deptno','ename','salary','city']
  
df=spark.createDataFrame(data=lst1,schema=scema1)
display(df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ####1.3.creates a DataFrame from a collection(list, dict), RDD or Python Pandas.

# COMMAND ----------

#This creates a DataFrame from a collection(list, dict), RDD or Python Pandas.
lst = (('Robert',35),('James',25)) 
#df=spark.createDataFrame(data=lst)##With Out Schema 
df = spark.createDataFrame(lst,('Name','Age'))  ##With Schema


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.4.Dataframe with dictionary

# COMMAND ----------

#using dictinary
dict = ({"name":"robert","age":25}, {"name" : "james","age" : 31}) 
df = spark.createDataFrame(dict)
display(df)
#df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####1.5. using RDD

# COMMAND ----------

#using rdd
lst3 = (('Robert',35),('James',25)) 

rdd = sc.parallelize(lst3)

rdd.collect()

print(type(rdd))





# COMMAND ----------

schema10=['name','age']

#df =  spark.createDataFrame(data=rdd,schema=('name string, age long'))
df =  spark.createDataFrame(rdd,schema10)
df.show()
#DF=RDD+schema

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.6. How to create spark dataframe from pandas dataframe

# COMMAND ----------

#Using Python Pandas DamaFrame*
#Pandas dataframe is a two dimensional structure with named rows and columns. So data is aligned in a tabular fashion in rows and columns

import pandas as pd

data = (('tom', 10), ('nick', 15), ('juli', 14)) 
df_pandas = pd.DataFrame(data,columns=('Name','Age')) 

df_pandas


# COMMAND ----------

df = spark.createDataFrame(df_pandas)
display(df)

#display(df_pandas)

# COMMAND ----------



# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = spark.createDataFrame(df_pandas)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.7. Create dataframe from list of lists

# COMMAND ----------

# DBTITLE 1,Create dataframe from list of lists
#create data frame from list of lists
users_list = [[1, 'Scott'], [2, 'Donald'], [3, 'Mickey'], [4, 'Elvis']]
type(users_list)
type(users_list[1])


# COMMAND ----------

df10=spark.createDataFrame(users_list, 'user_id int, user_first_name string').show()


# COMMAND ----------

# from pyspark.sql import Row
# users_rows = [Row(*user) for user in users_list]
# spark.createDataFrame(users_rows, 'user_id int, user_first_name string')


# COMMAND ----------

# MAGIC %md
# MAGIC ####1.8. Create dataframe from list of tuples

# COMMAND ----------

# DBTITLE 1,Create dataframe from list of tuples
users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]
spark.createDataFrame(users_list, 'user_id int, user_first_name string')

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.9. Create dataframe from list of Dict

# COMMAND ----------

# DBTITLE 1,Create Dataframe from List of Dict
users_list = [
    {'user_id': 1, 'user_first_name': 'Scott'},
    {'user_id': 2, 'user_first_name': 'Donald'},
    {'user_id': 3, 'user_first_name': 'Mickey'},
    {'user_id': 4, 'user_first_name': 'Elvis'}
]

df=spark.createDataFrame(users_list).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.9. create dataframe using schema

# COMMAND ----------


import datetime
users = [
    {
        "id": 1,
        "first_name": "Venkat",
        "last_name": "rama",
        "email": "venkat@gmail.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Hanu",
        "last_name": "Reddy",
        "email": "abc@gmail.com",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Ram",
        "last_name": "Charan",
        "email": "power@gmail.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "scott",
        "last_name": "tiger",
        "email": "tiger@gmail.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    }
]


# COMMAND ----------

users

# COMMAND ----------

data_schema = '''
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    is_customer BOOLEAN,
    amount_paid FLOAT,
    customer_from DATE,
    last_updated_ts TIMESTAMP
'''

# COMMAND ----------

df11=spark.createDataFrame(users, data_schema)

# COMMAND ----------

df11.printSchema()

# COMMAND ----------

df11.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.10. create dataframe with spark schema
# MAGIC
# MAGIC Very Impartant

# COMMAND ----------

from pyspark.sql.types import *

data_schema = StructType([
    StructField('id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('is_customer', BooleanType()),
    StructField('amount_paid', FloatType()),
    StructField('customer_from', DateType()),
    StructField('last_updated_ts', TimestampType())
])

df12=spark.createDataFrame(data=users, schema=data_schema)
#df111=df12.rdd.collect()
df12.display()


# COMMAND ----------

#df12.dtypes
df12.columns

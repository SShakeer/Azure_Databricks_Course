# Databricks notebook source
# DBTITLE 1,STRUCT Demo
# emp = [
#     (7566, "ALLEN","DATA_ENGINEER","01-JAN-2012",5000,[('home': '123445', 'office': '756775')]),
# (7567, "SMITH","SALESMAN","01-JAN-2022",50000,[('home': '54355', 'office': '78908')
#     ])]

# COMMAND ----------

emp2 = [
     (100, "Mark", "DE", 1250, 
      "India", ['abc@gmail.com', '123@yahoo.com'], 
     ('KRpuram', 'Bangalore', 'Karnataka', 560091)
     ),
     (200, "SMITH", "Salesman", 7500, 
      "USA", ['123@abc.com', 'abcd@xyz.com'], 
      ('Ammerpet', 'Hyderabad', 'AP', 517654)
     )
]

# COMMAND ----------

emp = [
     (100, "Mark", "DE", 1250, 
      "India", ['abc@gmail.com', '123@yahoo.com'], 
      {"Home": "+91 567656556", "Office": "+91 987654790"}, 
     ('KRpuram', 'Bangalore', 'Karnataka', 560091)
     ),
     (200, "SMITH", "Salesman", 7500, 
      "USA", ['123@abc.com', 'abcd@xyz.com'], 
      {"Home": "+91 98871111", "Office": "+91 98760222"}, 
      ('Ammerpet', 'Hyderabad', 'AP', 517654)
     )
]

# COMMAND ----------

schema="""empno INT, ename STRING, job STRING,
        salary int, country STRING, email_ids ARRAY<STRING>,
        phone MAP<STRING, STRING>,
        address STRUCT<street: STRING, city: STRING, state: STRING, pincode: INT>
    """

# COMMAND ----------

df=spark.createDataFrame(emp,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df2 = spark.createDataFrame(
    emp,
   """empno INT, ename STRING, job STRING,
        salary int, country STRING, email_ids ARRAY<STRING>,
        phone MAP<STRING, STRING>,
        address STRUCT<street: STRING, city: STRING, state: STRING, pincode: INT>
    """
)

# COMMAND ----------



# COMMAND ----------

emp2 = [
     (100, "Mark", "DE", 1250, 
      "India", ['abc@gmail.com', '123@yahoo.com'], 
     ('KRpuram', 'Bangalore', 'Karnataka', 560091)
     ),
     (200, "SMITH", "Salesman", 7500, 
      "USA", ['123@abc.com', 'abcd@xyz.com'], 
      ('Ammerpet', 'Hyderabad', 'AP', 517654)
     )
]

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema = StructType([
    StructField('empno', IntegerType()),
    StructField('ename', StringType()),
    StructField('job', StringType()),
    StructField('salary', StringType()),
    StructField('country', StringType()),
    StructField('email_ids', ArrayType(StructType([
        StructField('personal', StringType()),
        StructField('office', StringType())
    ]))),
    StructField('address', StructType(StructType([
        StructField('street', StringType()),
        StructField('city', StringType()),
        StructField('state', StringType()),
        StructField('pincode', IntegerType())
    ])))
])

# COMMAND ----------

emp3 = [
     (100, "Mark", "DE", 1250, 
      "India", ['abc@gmail.com', '123@yahoo.com']
     ),
     (200, "SMITH", "Salesman", 7500, 
      "USA", ['123@abc.com', 'abcd@xyz.com']
     )
]

# COMMAND ----------

emp3

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema = StructType([
        StructField("empno", StringType(), True),
        StructField("ename", StringType(), True),
        StructField("job", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("country", StringType(), True),
        StructField("email_ids", ArrayType(StringType(), True)),
        StructField('phone', MapType(StringType(),StringType()),True),
        StructField('address', StructType(StructType([
        StructField('street', StringType()),
        StructField('city', StringType()),
        StructField('state', StringType()),
        StructField('pincode', IntegerType())
    ])))

    ])

# COMMAND ----------

df=spark.createDataFrame(emp,schema)

# COMMAND ----------

df.select('empno','salary','country','phone.Office', 'phone.Home','address.street','address.city','address.state').show()

# COMMAND ----------

df.select('empno','ename','job','salary', col('phone')['home'].alias("home"), col('phone')['office'].alias("office")).show()

# COMMAND ----------

# DBTITLE 1,MAP Demo

emp = [
    {
        "em": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "phone_numbers": {"mobile": "+1 234 567 8901", "home": "+1 234 567 8911"},
      ""
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers": {"mobile": "+1 234 567 8923", "home": "+1 234 567 8934"},
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },

# COMMAND ----------

df=spark.read.format("csv").load("/FileStore/tables/mapdata2-1.csv",header=True,inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

employees_df = spark.createDataFrame(
    df,
    schema="""employee_id INT, employee_first_name STRING, employee_last_name STRING,
        employee_salary FLOAT, employee_nationality STRING, employee_email_ids ARRAY<STRING>,
        employee_phone_numbers MAP<STRING, STRING>, employee_ssn STRING,
        employee_address STRUCT<street: STRING, city: STRING, state: STRING, postal_code: INT>
    """
)

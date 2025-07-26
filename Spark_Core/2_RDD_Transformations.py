# Databricks notebook source
#from pyspark.sql import SparkSession

# Start a Spark session
#spark = SparkSession.builder.master("local").appName("EmployeeData").getOrCreate()

# Sample data
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

# Convert to RDD
rdd = spark.sparkContext.parallelize(data)

# Show the RDD
rdd.collect()


# COMMAND ----------

rdd.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MAP 
# MAGIC
# MAGIC when ever you want to apply logic to all elements in the collection

# COMMAND ----------

# Extract only employee names
employee_names = rdd.map(lambda x: x[1])
#select ename from emp

# Collect and show the names
employee_names.collect()


# COMMAND ----------

# Increase salary by 10% for each employee
increased_salary_rdd = rdd.map(lambda x: (x[0], x[1], x[2], x[3], x[4],x[5], x[5] * 1.10, x[6], x[7]))

# Collect and show the updated RDD
increased_salary_rdd.collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ## FLATMAP
# MAGIC

# COMMAND ----------

# Use flatMap to split each tuple into individual fields
flattened_rdd = rdd.flatMap(lambda x: x)

# Collect and show the result
flattened_rdd.collect()


# COMMAND ----------

# Use flatMap to extract employee names and job titles
names_and_titles_rdd = rdd.flatMap(lambda x: [x[1], x[2]])

# Collect and show the result
names_and_titles_rdd.collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ## FILTER

# COMMAND ----------

# Filter employees where deptno is 10
# select * from emp where deptno=10
dept10_rdd = rdd.filter(lambda x: x[7] == 10)

# Collect and show the filtered result
dept10_rdd.collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ## REDUCEBYKEY
# MAGIC
# MAGIC To use the reduceByKey transformation in PySpark, the RDD needs to be in a key-value pair format, where the "key" is used for aggregation, and the "value" is what you want to reduce (combine).
# MAGIC
# MAGIC (10,2000)
# MAGIC (20,3000)
# MAGIC (20,4000)
# MAGIC 10,2000
# MAGIC 20,7000

# COMMAND ----------

# MAGIC %sql
# MAGIC select deptno,sum(sal) as Total from emp group by deptno
# MAGIC

# COMMAND ----------

# Create a key-value RDD where the key is deptno and the value is salary
dept_salary_rdd = rdd.map(lambda x: (x[7], x[5]))

# Use reduceByKey to sum the salaries for each department
salary_by_dept_rdd = dept_salary_rdd.reduceByKey(lambda a, b: a + b)

# Collect and show the result
salary_by_dept_rdd.collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ## GROUPBYKEY

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Create a key-value RDD where the key is deptno and the value is salary
dept_salary_rdd = rdd.map(lambda x: (x[7], x[5]))

# Group salaries by deptno
grouped_salaries_rdd = dept_salary_rdd.groupByKey()

#grouped_salaries_rdd.collect()



# Sum the salaries for each department
dept_total_salary_rdd = grouped_salaries_rdd.mapValues(lambda salaries: sum(salaries))

# Collect and show the result
dept_total_salary_rdd.collect()


# COMMAND ----------

# MAGIC %md
# MAGIC ## JOINS

# COMMAND ----------

departments = [
    (10, "ACCOUNTING"),
    (20, "RESEARCH"),
    (30, "SALES"),
    (40, "OPERATIONS")
]

# Convert to RDD
departments_rdd = spark.sparkContext.parallelize(departments)


# COMMAND ----------

# Convert employee RDD to key-value pairs (key is deptno, value is the whole employee record)
employee_rdd = rdd.map(lambda x: (x[7], x))  # Key is deptno, value is employee tuple

# Perform the inner join on deptno
joined_rdd = employee_rdd.join(departments_rdd)

# Collect and show the result
joined_rdd.collect()


# COMMAND ----------

# Extract only the employee name and department name
employee_dept_rdd = joined_rdd.map(lambda x: (x[1][0][1], x[1][1]))  # (Employee Name, Department Name)

# Collect and show the result
employee_dept_rdd.collect()


# COMMAND ----------

employee_dept_rdd.saveAsTextFile("/FileStore/tables/emp_dname_output")

# COMMAND ----------

employee_dept_rdd.getNumPartitions()

# COMMAND ----------

#dbutils.fs.rm("/FileStore/tables/emp_dname_output",True)

# COMMAND ----------

employee_dept_rdd.take(5)

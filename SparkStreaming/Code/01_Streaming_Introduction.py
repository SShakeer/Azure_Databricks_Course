# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Real time analytics with Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Spark Streaming
# MAGIC * **Spark Streaming is an extension of the core Spark API that enables scalable and fault-tolerant processing of real-time data streams. It operates by breaking down continuous streams of data into micro-batches, which are then processed by Spark's engine to generate results in batches. This approach allows for low-latency analytics and near real-time processing**
# MAGIC
# MAGIC * **Spark Streaming uses DStreams to represent continuous streams of data. DStreams are internally represented as a sequence of Resilient Distributed Datasets (RDDs), which are Spark's core abstraction for distributed data**
# MAGIC
# MAGIC * **The incoming data stream is divided into DStreams, which are represented as a continuous series of RDDs**
# MAGIC
# MAGIC   **Limitations:**
# MAGIC
# MAGIC * **It is RDD based and no optimization**

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2. Spark Structured Streaming
# MAGIC * **Spark Structured Streaming is a stream processing engine built on Spark SQL that processes data incrementally and updates the final results as more streaming data arrives. It brought a lot of ideas from other structured APIs in Spark (Dataframe and Dataset) and offered query optimizations similar to SparkSQL.**
# MAGIC
# MAGIC * **It Supports only Python and Scala Languages**
# MAGIC
# MAGIC * **Easy programming as Spark SQL**
# MAGIC
# MAGIC * **Based on Dataframes/Datasets (Structured)**
# MAGIC
# MAGIC   **Advantages:**
# MAGIC
# MAGIC * **It is Dataframe based and automatic optimization same as Spark SQL**
# MAGIC * **Allow apps building end to end using batch and streaming SQL APIs**
# MAGIC * **Low latency**
# MAGIC * **Realtime streaming pipelines (1ms to 100ms)**
# MAGIC * **Batch pipelines with inc load sources**

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## 1.3. DLT
# MAGIC * **DLT is a framework for creating batch and streaming data pipelines in SQL and Python. Common use cases for DLT include data ingestion from sources such as cloud storage (such as Amazon S3, Azure ADLS Gen2, and Google Cloud Storage) and message buses (such as Apache Kafka, Amazon Kinesis, Google Pub/Sub, Azure EventHub, and Apache Pulsar), and incremental batch and streaming transformations.**

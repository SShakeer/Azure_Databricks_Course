# Databricks notebook source
# MAGIC %md
# MAGIC #### 1.What is Data Stream?
# MAGIC
# MAGIC *  Any data source that grows over time
# MAGIC *  New files landing in cloud storage
# MAGIC *  Updates to a database captured in a CDC feed
# MAGIC *  Events queued in a pub/sub messaging feed

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.Trigger Intervals?
# MAGIC
# MAGIC

# COMMAND ----------

streamDF.writeStream
.trigger(processingTime="2 minutes")
.outputMode("append")
.option("checkpointLocation", "/path")
.table(”Output_Table")

# COMMAND ----------

# MAGIC %md
# MAGIC * Default trigger mode  processingTime="500ms"
# MAGIC * Fixed interval:
# MAGIC   Process data in micro-batches at the user-specified intervals
# MAGIC
# MAGIC   Example: .trigger(processingTime=”5 minutes")
# MAGIC * Triggered batch:
# MAGIC   
# MAGIC   Process all available data in a single batch, then stop
# MAGIC
# MAGIC   Example:.trigger(once=True)
# MAGIC
# MAGIC * Triggered micro-batches:
# MAGIC
# MAGIC   Process all available data in multiple micro-batches, then stop
# MAGIC
# MAGIC   Example: .trigger(availableNow=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.Output Modes

# COMMAND ----------

streamDF.writeStream
.trigger(processingTime="2 minutes")
.outputMode("append")
.option("checkpointLocation", "/path")
.table("Output_Table")

# COMMAND ----------

# MAGIC %md
# MAGIC * append: 
# MAGIC   Only newly appended rows are incrementally appended to the target table with each batch
# MAGIC
# MAGIC   Example: .outputMode("append")
# MAGIC
# MAGIC * complete: 
# MAGIC
# MAGIC   The target table is overwritten with each batch
# MAGIC
# MAGIC   Example: .outputMode("complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ####4.Checkpointing
# MAGIC
# MAGIC * Store stream state
# MAGIC * Track the progress of your stream processing

# COMMAND ----------

streamDF.writeStream
.trigger(processingTime="2 minutes")
.outputMode("append")
.option("checkpointLocation", "/path")
.table(”Output_Table")

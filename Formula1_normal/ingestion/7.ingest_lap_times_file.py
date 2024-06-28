# Databricks notebook source
# MAGIC %md
# MAGIC ## ingest lap_times_file
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - read the csv file using sp[ark dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),

                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", StringType(), True),

                                      StructField("time", StringType(), True),

                                      StructField("milliseconds", IntegerType(), True)

                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/formula1dlil/raw/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=lap_times_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##step3 - write the output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")
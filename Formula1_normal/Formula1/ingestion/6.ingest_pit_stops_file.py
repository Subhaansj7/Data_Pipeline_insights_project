# Databricks notebook source
# MAGIC %md
# MAGIC ## ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - read the json file using sp[ark dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),

                                      StructField("driverId", IntegerType(), True),

                                      StructField("stop", StringType(), True),

                                      StructField("lap", IntegerType(), True),

                                      StructField("time", StringType(), True),

                                      StructField("duration", StringType(), True),

                                      StructField("milliseconds", IntegerType(), True)

                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("/mnt/formula1dlil/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=pit_stops_df.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##step3 - write the output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
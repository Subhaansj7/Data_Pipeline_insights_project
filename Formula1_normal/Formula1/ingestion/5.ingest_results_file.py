# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest results.json file 

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Read the JSON file using spark dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),
                               StructField("raceId",IntegerType(),True),
                               StructField("driverId",IntegerType(),True),
                               StructField("constructorId",IntegerType(),True),
                               StructField("number",IntegerType(),True),
                               StructField("grid",IntegerType(),True),
                               StructField("position",IntegerType(),True),
                               StructField("postionText",StringType(),True),
                               StructField("postionOrder",IntegerType(),True),
                               StructField("points",IntegerType(),True),
                               StructField("laps",IntegerType(),True),
                               StructField("time",StringType(),True),
                               StructField("milliseconds",IntegerType(),True),
                               StructField("fastestLap",IntegerType(),True),
                               StructField("rank",IntegerType(),True),
                               StructField("fastestLapTime",StringType(),True),
                               StructField("fastestLapSpeed",FloatType(),True),
                               StructField("statusId",StringType(),True)

])

# COMMAND ----------

results_df=spark.read\
.schema(results_schema)\
.json("/mnt/formula1dlil/raw/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step2 - Rename the column and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

results_with_columns_df=results_df.withColumnRenamed("resultId","result_id")\
                                  .withColumnRenamed("raceId","race_id")\
                                  .withColumnRenamed("driverId","driver_id")\
                                  .withColumnRenamed("constructorId","constructor_id")\
                                  .withColumnRenamed("positonText","position_text")\
                                  .withColumnRenamed("postionOrder","positon_order")\
                                  .withColumnRenamed("fastestLap","fastest_lap")\
                                  .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                  .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                  .withColumn("ingestion_date",current_timestamp())\
                                  .withColumn("data_source",lit(v_data_source)) 
                                 

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df=results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - write to output the processed container in parquet format

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")
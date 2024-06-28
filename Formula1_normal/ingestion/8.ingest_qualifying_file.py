# Databricks notebook source
# MAGIC %md
# MAGIC ## ingest qualifying.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - read the json file using sp[ark dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),

                                      StructField("driverId", IntegerType(), True),

                                      StructField("constructorId", IntegerType(), True),

                                      StructField("number", IntegerType(), True),

                                      StructField("position", IntegerType(), True),

                                      StructField("q1", StringType(), True),

                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)

                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/formula1dlil/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ###step 2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("constructorId","constructor_id")\
.withColumn("ingestion_date",current_timestamp())\
.withColumn("data_source",lit(v_data_source)) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##step3 - write the output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")
# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits.csv file#

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step-1 Read the csv file in spark dataframe reader###
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType , StructField ,IntegerType,StringType ,DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitID",IntegerType(),False),
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lnh",DoubleType(),True),
                                     StructField("alt",IntegerType(),True),
                                     StructField("url",StringType(),True)

])

# COMMAND ----------

circuits_df= spark \
.read.option("header",True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select only columns required##
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df=circuits_df.select(col("circuitID"),col("circuitRef"),col("name"),col("location"),col("lat"),col("lnh"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Step3- Rename the column as required##

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitID","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lnh","longitude") \
.withColumnRenamed("alt","altitude") \
.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##step 4 -Add ingestion date to dataframe##

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##step-5 - write data to datalake as parquet##

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
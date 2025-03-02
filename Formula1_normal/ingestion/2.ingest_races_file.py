# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###step1- Read the csv file using spark dataframe reader api

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df=spark.read \
.option("header",True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###step2- add ingestion date and  race_timestamp to the data frame###
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , to_timestamp, concat,col,lit

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df).withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-mm-dd HH:mm:ss'))


# COMMAND ----------

# MAGIC %md
# MAGIC ###step 3 - Select only columns and Rename column as required###

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'))


# COMMAND ----------

races_selected_df=races_selected_df.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ###step4 - write the output to processed container in parquet format

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
races_selected_df.write.mode('overwrite').partitionBy('race_year').format("parquet").saveAsTable("f1_processed.race")

# COMMAND ----------

dbutils.notebook.exit("Success")
# Databricks notebook source
# MAGIC %md
# MAGIC ###Access data lake cluster scoped configuration ###
# MAGIC #1.Set the spark config fs.azure.acount.key in cluster#
# MAGIC #2.List file in demo container#
# MAGIC #3.Read data form circuits.csv#

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlil.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlil.dfs.core.windows.net/circuits.csv"))
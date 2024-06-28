# Databricks notebook source
# MAGIC %md
# MAGIC ###Access data lake using access keys ###
# MAGIC #1.Set the spark config fs.azure.acount.key#
# MAGIC #2.List file in demo container#
# MAGIC #3.Read data form circuits.csv#

# COMMAND ----------

formula1_account_key= dbutils.secrets.get(scope='formula1-scope', key='formula1dlil-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlil.dfs.core.windows.net",
    formula1_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlil.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlil.dfs.core.windows.net/circuits.csv"))
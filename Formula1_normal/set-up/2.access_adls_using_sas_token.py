# Databricks notebook source
# MAGIC %md
# MAGIC #Access data lake using Sas token#
# MAGIC ###1.set the spark config for SAS token###
# MAGIC ###2.List the file in demo container###
# MAGIC ###3.Read data from circuits.csv###
# MAGIC

# COMMAND ----------

formula1dlil_demo_sas_token = dbutils.secrets.get(scope='formula1-scope',key='formula1dlil_demo_sas_token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlil.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlil.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlil.dfs.core.windows.net", formula1dlil_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlil.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlil.dfs.core.windows.net/circuits.csv"))
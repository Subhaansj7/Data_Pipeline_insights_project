# Databricks notebook source
# MAGIC %md
# MAGIC #Explore the capabalities of the dbutils.secret utility
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope', key='formula1dlil-account-key')
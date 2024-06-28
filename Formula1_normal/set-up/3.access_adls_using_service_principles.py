# Databricks notebook source
# MAGIC %md
# MAGIC ###Access data lake using service principal ###
# MAGIC #1.Register azure ad application/ service portal#
# MAGIC #2.generate secret/password for the application#
# MAGIC #3.set saprk.config wiath app/client id , directory/tenant id & secret#
# MAGIC #4.assign role "Storage blob data contributor" to data lake#

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key='formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlil.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlil.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlil.dfs.core.windows.net",client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlil.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlil.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlil.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlil.dfs.core.windows.net/circuits.csv"))
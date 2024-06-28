# Databricks notebook source
# MAGIC %md
# MAGIC #Mount Azure DataLake using Service Principal #
# MAGIC ##1.Get client_id, tenant_id and client_secret from key vault#
# MAGIC ##2.set saprk.config wiath app/client id , directory/tenant id & secret##
# MAGIC ##3.call file system utility mount to mount the storage##
# MAGIC ##4.Explore other file system utilities related to mount(list all mount,unmount)

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

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlil.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlil/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlil/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlil/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())
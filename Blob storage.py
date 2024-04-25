# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help('mount')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.pp09.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.pp09.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.pp09.dfs.core.windows.net", "104b246b-7706-4160-ac0c-23acf8166a5e")
spark.conf.set("fs.azure.account.oauth2.client.secret.pp09.dfs.core.windows.net", "SNc8Q~rYdJpsmo6zKNmJJjAeuAR97GQG9FtWeci0" )
spark.conf.set("fs.azure.account.oauth2.client.endpoint.pp09.dfs.core.windows.net", f"https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://pplog@blobpp09.blob.core.windows.net",
  mount_point = "/mnt/blobpp09",
  extra_configs = {"fs.azure.account.key.blobpp09.blob.core.windows.net": 
            "k84BeZa+nfgKBZnBQ+wt3/hIzoUhaPrCaaQsel81tFOuOh47qa1dOP0Ms8QsbVrQ5ZvN3IXsD57n+ASt15UpnA=="}
  )

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.pp09.blob.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.pp09.blob.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.pp09.blob.core.windows.net", "104b246b-7706-4160-ac0c-23acf8166a5e")
spark.conf.set("fs.azure.account.oauth2.client.secret.pp09.blob.core.windows.net", "SNc8Q~rYdJpsmo6zKNmJJjAeuAR97GQG9FtWeci0" )
spark.conf.set("fs.azure.account.oauth2.client.endpoint.pp09.blob.core.windows.net", f"https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

# COMMAND ----------

# MAGIC %fs ls wasbs://pplog@blobpp09.dfs.core.windows.net/

# COMMAND ----------

storage_account_name = "blobpp09"
storage_account_key = "k84BeZa+nfgKBZnBQ+wt3/hIzoUhaPrCaaQsel81tFOuOh47qa1dOP0Ms8QsbVrQ5ZvN3IXsD57n+ASt15UpnA=="
container_name = "pplog"

spark.conf.set(
  "fs.azure.account.key.blobpp09.blob.core.windows.net",
  storage_account_key
)

dbutils.fs.ls("wasbs://pplog@blobpp09.blob.core.windows.net/")

# COMMAND ----------

# MAGIC %fs ls wasbs://pplog@blobpp09.blob.core.windows.net/

# COMMAND ----------

dbutils.fs.ls("wasbs://pplog@blobpp09.blob.core.windows.net/")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

display(spark.read.csv("dbfs:/mnt/blobpp09/sample2.xls", header= True))

# COMMAND ----------

curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
apt-get update ACCEPT_EULA=Y apt-get install -y msodbcsql18
# optional: for bcp and sqlcmd
ACCEPT_EULA=Y apt-get install -y mssql-tools18
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrcsource ~/.bashrc

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
# MAGIC
# MAGIC curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
# MAGIC
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install -y msodbcsql17
# MAGIC # optional: for bcp and sqlcmd
# MAGIC ACCEPT_EULA=Y apt-get install -y mssql-tools
# MAGIC echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
# MAGIC source ~/.bashrc
# MAGIC # optional: for unixODBC development headers
# MAGIC apt-get install -y unixodbc-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
# MAGIC
# MAGIC curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
# MAGIC
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
# MAGIC # optional: for bcp and sqlcmd
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
# MAGIC echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
# MAGIC source ~/.bashrc
# MAGIC # optional: for unixODBC development headers
# MAGIC sudo apt-get install -y unixodbc-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC apt list --installed | grep

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
# MAGIC
# MAGIC curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
# MAGIC
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
# MAGIC # optional: for bcp and sqlcmd
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18
# MAGIC echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
# MAGIC source ~/.bashrc
# MAGIC # optional: for unixODBC development headers
# MAGIC sudo apt-get install -y unixodbc-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
# MAGIC
# MAGIC curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | tee /etc/apt/sources.list.d/mssql-release.list
# MAGIC
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install -y msodbcsql18
# MAGIC # optional: for bcp and sqlcmd
# MAGIC ACCEPT_EULA=Y apt-get install -y mssql-tools18
# MAGIC echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
# MAGIC source ~/.bashrc
# MAGIC # optional: for unixODBC development headers
# MAGIC apt-get install -y unixodbc-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC apt list --installed | grep mssql-tools18

# COMMAND ----------

import pandas as pd
dates_pd = dbutils.jobs.taskValues.get(taskKey = "initDate", key = "dates_json", debugValue = 1)
display(dates_pd)

df = pd.DataFrame.from_dict(dates_pd)
display (df)

# COMMAND ----------

# MAGIC %pip install

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------



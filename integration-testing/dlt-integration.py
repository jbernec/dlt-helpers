# Databricks notebook source
# Permission is based on File or folder based ACL assignments to the Data Lake filesystem (container) . RBAC assignments to the top level Azure Data Lake resource is not required.
# https://docs.databricks.com/storage/azure-storage.html
spark.conf.set("fs.azure.account.auth.type.adls04.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adls04.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adls04.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get("myscope", key="tenantid")))

# COMMAND ----------

spark.sql(
    """
    create schema if not exists dlt_catalog.dlt_schema managed location 'abfss://unity-catalog@adls04.dfs.core.windows.net/dltcatalog/dltdata'
"""
)

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .load("/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/")
)

# COMMAND ----------

df.display()

# COMMAND ----------

from helper_functions import get_rules_as_list_of_dict, append_at_symbol
from pyspark.sql.functions import expr, col

df_rules = spark.createDataFrame(get_rules_as_list_of_dict())

# COMMAND ----------

df_rules.display()

# COMMAND ----------

def get_rules(tag):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  for row in df_rules.filter(col("tag") == tag).collect():
    rules[row['name']] = row['constraint']
  return rules

# COMMAND ----------

get_rules("character_validity")

# COMMAND ----------

#from pyspark.sql.functions import concat, lit, col

# Insert "@" at the end of the website column

df_concat = append_at_symbol(df=df, column_name="website")

# Display the updated DataFrame
display(df_concat)

# COMMAND ----------

df_concat.write.format("parquet").mode("overwrite").save("/tmp/delta/concat")

# COMMAND ----------

spark.read.format("parquet").load("/tmp/delta/concat").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM dlt_catalog.dlt_schema.raw_farmers_market;

# Databricks notebook source
# Permission is based on File or folder based ACL assignments to the Data Lake filesystem (container) . RBAC assignments to the top level Azure Data Lake resource is not required.
# https://docs.databricks.com/storage/azure-storage.html
spark.conf.set("fs.azure.account.auth.type.adls04.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adls04.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adls04.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get("myscope", key="tenantid")))

# COMMAND ----------


schema_location = "abfss://unity-catalog@adls04.dfs.core.windows.net/csvdata/_schematracking"
source_path = "/tmp/delta/concat"

options = {
    "cloudFiles.format": "parquet",
    "cloudFiles.inferColumnTypes": False,
    "cloudFiles.inferSchema": False,
    "cloudFiles.schemaEvolutionMode":"rescue",
    "cloudFiles.schemaLocation": schema_location,
}

# COMMAND ----------

import dlt
from helper_functions import get_rules_as_list_of_dict, remove_at_symbol, get_rules
from pyspark.sql.functions import expr, col

# UC enabled DLT bronze ingestion with schema inference
@dlt.table(
    table_properties={"quality": "bronze"},
    comment="This table contains raw farmers market data with '@' removed from the website column",
    name="raw_farmers_market")
@dlt.expect_all(get_rules('character_validity'))
def get_data():
    df = (
        spark.readStream.format("cloudFiles")
        .options(**options)
        .option("cloudFiles.schemaLocation", schema_location)  # Specify the schema location
        .load(source_path)
    )

    # Apply the remove_at_symbol function to the 'website' column
    df = remove_at_symbol(df, "website")
    
    return df

# Databricks notebook source
# Permission is based on File or folder based ACL assignments to the Data Lake filesystem (container) . RBAC assignments to the top level Azure Data Lake resource is not required.
# https://docs.databricks.com/storage/azure-storage.html
spark.conf.set("fs.azure.account.auth.type.adls04.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adls04.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adls04.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get("myscope", key="tenantid")))

# COMMAND ----------

# https://realpython.com/python-logging/
# https://opentelemetry.io/docs/instrumentation/python/
# https://betterstack.com/community/guides/logging/how-to-start-logging-with-python/

from datetime import datetime
import os
import json
import logging
from pyspark.sql.functions import col
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.session import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql import DataFrame
from azure.core.exceptions import HttpResponseError

# COMMAND ----------

# Set logger

logger = logging.getLogger(__name__)
# logger.addHandler(handler)
logger.setLevel(logging.INFO)

# COMMAND ----------

# Create a catalog, schema and table within the catalog and specify the location

spark.sql('SHOW CATALOGS').display()

spark.sql(
    """
    create catalog if not exists ado_lab_catalog MANAGED LOCATION 'abfss://unity-catalog@adls04.dfs.core.windows.net/adocatalog/'
          """
).display()

spark.sql(
    """
    create schema if not exists ado_lab_catalog.ado_schema managed location 'abfss://unity-catalog@adls04.dfs.core.windows.net/adocatalog/adodata'
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES, CREATE TABLE ON EXTERNAL LOCATION `metastore_root_location` TO `account users`;

# COMMAND ----------



# COMMAND ----------

catalog_name = "ado_lab_catalog"
schema_name = "ado_schema"
table_name = "nyctaxi_data"
checkpoint_path = "abfss://unity-catalog@adls04.dfs.core.windows.net/adocatalog/_checkpoint"
schema_location = "abfss://unity-catalog@adls04.dfs.core.windows.net/adocatalog/_schematracking"

spark.sql(f"use catalog {catalog_name}")

options = {
    "cloudFiles.format": "json",
    "cloudFiles.inferColumnTypes": True,
    "cloudFiles.inferSchema": True,
    "cloudFiles.schemaLocation": schema_location,
}

# COMMAND ----------

def start_streaming_query():
    while True:
        try:
            q = (
                spark.readStream.format("cloudFiles")
                .options(**options)
                .load("dbfs:/databricks-datasets/nyctaxi/sample/json/")
                .writeStream.format("delta")
                .trigger(availableNow=True)
                .option("mergeSchema", "true")
                .option("checkpointLocation", checkpoint_path)
                .queryName("nytaxi_query")
                .toTable(f"{catalog_name}.{schema_name}.{table_name}")
            )
            q.awaitTermination()
            return q
            properties = {
            "notebook_name": "observability-pilot",
            "notebook_cell_execution": "nytaxi data streaming query",
            }
            logger.info("streaming query execution was successful", exc_info=True, extra=properties)
            print("check the azure monitor Apptraces logs")
        except BaseException as e:
            properties = {
            "notebook_name": "observability-pilot",
            "notebook_cell_execution": "nytaxi data streaming query",
            }
            logger.exception("streaming query execution encountered an exception", exc_info=True, extra=properties)
            print("check the azure monitor exception logs")
            # Adding a new column will trigger an UnknownFieldException. In this case we just restart the stream:
            if not ("UnknownFieldException" in str(e.stackTrace)):
                raise e



# COMMAND ----------

start_streaming_query()

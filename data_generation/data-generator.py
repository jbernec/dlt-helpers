# Databricks notebook source
# MAGIC %md
# MAGIC https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html
# MAGIC
# MAGIC Creating simple synthetic data sets
# MAGIC You can use the data generator with, or without the use of a pre-existing schema. The basic mechanism is as follows:
# MAGIC
# MAGIC Define a DataGenerator instance
# MAGIC
# MAGIC Optionally add a schema
# MAGIC
# MAGIC If using a schema, add specifications for how the columns are to be generated with the withColumnSpec method. This does not add new columns, but specifies how columns imported from the schema should be generated
# MAGIC
# MAGIC Add new columns with the withColumn method
# MAGIC
# MAGIC

# COMMAND ----------

# Permission is based on File or folder based ACL assignments to the Data Lake filesystem (container) . RBAC assignments to the top level Azure Data Lake resource is not required.
# https://docs.databricks.com/storage/azure-storage.html
spark.conf.set("fs.azure.account.auth.type.adls04.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adls04.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret.adls04.dfs.core.windows.net", dbutils.secrets.get("myscope", key="clientsecret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adls04.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(dbutils.secrets.get("myscope", key="tenantid")))

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import FloatType, IntegerType, StringType

source_path = "dbfs:/databricks-datasets/nyctaxi/sample/json/"

def read_df():
    return (
        spark.read.format("json")
        .option("multiline", "true")
        .load(source_path)
    )

# COMMAND ----------

table_df = read_df()
table_df.display()

# COMMAND ----------

table_df.printSchema()

# COMMAND ----------

# Creating data set with pre-existing schema

table_schema = read_df().schema
dataspec = (dg.DataGenerator(spark, rows=10, partitions=1)
            .withSchema(table_schema))

# COMMAND ----------

df = dataspec.build()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC https://databrickslabs.github.io/dbldatagen/public_docs/generating_from_existing_data.html#generating-synthetic-data-from-existing-data-or-schema-experimental
# MAGIC
# MAGIC Generating Synthetic Data from Existing Data or Schema (Experimental)

# COMMAND ----------

import dbldatagen as dg

dfSource = read_df()

analyzer = dg.DataAnalyzer(sparkSession=spark, df=dfSource)

display(analyzer.summarizeToDF())

# COMMAND ----------

# MAGIC %md
# MAGIC Use the methods scriptDataGeneratorFromSchema and the scriptDataGeneratorFromData to generate code from an existing schema or Spark dataframe respectively.
# MAGIC
# MAGIC For example, the following code will generate code to synthesize data from an existing schema.
# MAGIC
# MAGIC The generated code is intended to be stub code and will need to be modified to match your desired data layout.

# COMMAND ----------

import dbldatagen as dg

code =  dg.DataAnalyzer.scriptDataGeneratorFromSchema(dfSource.schema)

# COMMAND ----------

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=10,
                     random=True,
                     )
    .withColumn('DOLocationID', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('PULocationID', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('RatecodeID', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('VendorID', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('congestion_surcharge', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('extra', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('fare_amount', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('improvement_surcharge', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('mta_tax', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('passenger_count', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('payment_type', 'bigint', minValue=0, maxValue=1000000)
    .withColumn('store_and_fwd_flag', 'string', values=["Y", "N"])
    .withColumn('tip_amount', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('tolls_amount', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('total_amount', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('tpep_dropoff_datetime', 'string', template=r'\\w')
    .withColumn('tpep_pickup_datetime', 'string', template=r'\\w')
    .withColumn('trip_distance', 'double', minValue=0.0, maxValue=1000000.0, step=0.1)
    .withColumn('pep_pickup_date_txt', 'date', expr='current_date()')
    )

df_generated = generation_spec.build()
df_generated.display()

# COMMAND ----------

# MAGIC %md
# MAGIC For example, the following code will generate synthetic data generation code from a source dataframe.

# COMMAND ----------

import dbldatagen as dg

df_source = read_df()

analyzer = dg.DataAnalyzer(sparkSession=spark, df=df_source)

generatedCode = analyzer.scriptDataGeneratorFromData()

# COMMAND ----------

# Code snippet generated with Databricks Labs Data Generator (`dbldatagen`) DataAnalyzer class
# Install with `pip install dbldatagen` or in notebook with `%pip install dbldatagen`
# See the following resources for more details:
#
#   Getting Started - [https://databrickslabs.github.io/dbldatagen/public_docs/APIDOCS.html]
#   Github project - [https://github.com/databrickslabs/dbldatagen]
#
import dbldatagen as dg
import pyspark.sql.types

# Column definitions are stubs only - modify to generate correct data  
#
generation_spec2 = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=100000,
                     random=True,
                     )
    .withColumn('DOLocationID', 'bigint', minValue=1, maxValue=265)
    .withColumn('PULocationID', 'bigint', minValue=1, maxValue=265)
    .withColumn('RatecodeID', 'bigint', minValue=1, maxValue=99, percentNulls=0.01)
    .withColumn('VendorID', 'bigint', minValue=1, maxValue=2, percentNulls=0.01)
    .withColumn('congestion_surcharge', 'double', minValue=-2.5, maxValue=3.0, step=0.1)
    .withColumn('extra', 'double', minValue=-4.5, maxValue=90.06, step=0.1)
    .withColumn('fare_amount', 'double', minValue=-1472.0, maxValue=398468.38, step=0.1)
    .withColumn('improvement_surcharge', 'double', minValue=-0.3, maxValue=0.3, step=0.1)
    .withColumn('mta_tax', 'double', minValue=-0.5, maxValue=3.3, step=0.1)
    .withColumn('passenger_count', 'bigint', minValue=0, maxValue=9, percentNulls=0.01)
    .withColumn('payment_type', 'bigint', minValue=1, maxValue=5, percentNulls=0.01)
    .withColumn('store_and_fwd_flag', 'string', values=["Y", "N"], percentNulls=0.01)
    .withColumn('tip_amount', 'double', minValue=-200.8, maxValue=404.44, step=0.1)
    .withColumn('tolls_amount', 'double', minValue=-40.5, maxValue=612.66, step=0.1)
    .withColumn('total_amount', 'double', minValue=-1472.8, maxValue=398471.2, step=0.1)
    .withColumn('tpep_dropoff_datetime', 'string', template=r'\\w')
    .withColumn('tpep_pickup_datetime', 'string', template=r'\\w')
    .withColumn('trip_distance', 'double', minValue=-37264.53, maxValue=19130.18, step=0.1)
    .withColumn('pep_pickup_date_txt', 'date', expr='current_date()')
    )

df_generated2 = generation_spec2.build()
df_generated2.display()

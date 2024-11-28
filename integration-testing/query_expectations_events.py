# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG dlt_catalog;
# MAGIC USE SCHEMA dlt_schema;
# MAGIC CREATE or Replace VIEW event_log_raw AS SELECT * FROM event_log(TABLE(dlt_catalog.dlt_schema.raw_farmers_market));

# COMMAND ----------

df_events = spark.sql("""
          SELECT * FROM event_log(TABLE(dlt_catalog.dlt_schema.raw_farmers_market))
          """)

# COMMAND ----------

df_events.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from event_log_raw;
# MAGIC
# MAGIC SELECT id, `timestamp`, origin.flow_name, details:flow_progress.metrics.num_output_rows, details:flow_progress.status, details:flow_progress.data_quality.dropped_records, details:flow_progress.data_quality.expectations, * FROM event_log_raw WHERE event_type = 'flow_progress' ORDER BY timestamp DESC;

# COMMAND ----------

df_events.selectExpr(
    "id",
    "timestamp",
    "origin.flow_name",
    "details:flow_progress.metrics.num_output_rows",
    "details:flow_progress.status",
    "details:flow_progress.data_quality.dropped_records",
    "details:flow_progress.data_quality.expectations",
    "*",
).filter("event_type = 'flow_progress' and details:flow_progress.status = 'COMPLETED'").display()

# COMMAND ----------

df_expectations = df_events.selectExpr("id", "details:flow_progress.data_quality.expectations").filter("event_type = 'flow_progress' and details:flow_progress.status = 'RUNNING'")

df_expectations.display()

# COMMAND ----------

from pyspark.sql.types import (
    ArrayType,
    StructType,
    StructField,
    StringType,
    IntegerType,
)
from pyspark.sql.functions import explode, from_json

schema = ArrayType(
    StructType(
        [
            StructField("name", StringType()),
            StructField("dataset", StringType()),
            StructField("passed_records", IntegerType()),
            StructField("failed_records", IntegerType()),
        ]
    )
)

df_expectations = df_expectations.withColumn("expectations", explode(from_json(df_expectations.expectations, schema)))
df_expectations = df_expectations.selectExpr("id", "expectations.name",  "expectations.dataset", "expectations.passed_records", "expectations.failed_records")

df_expectations.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explorations

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW latest_update AS SELECT origin.update_id AS id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1;
""").display()

# COMMAND ----------

spark.sql("""
          select * from latest_update;
          """).display()

# COMMAND ----------

df = spark.sql("""
SELECT
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (
    SELECT
      explode(
        from_json(
          details :flow_progress.data_quality.expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      event_log_raw,
      latest_update
    WHERE
      event_type = 'flow_progress'
      AND origin.update_id = latest_update.id
  )
GROUP BY
  row_expectations.dataset,
  row_expectations.name
  """)

# # Write the DataFrame to a Unity Catalog table
# df.write.format("delta").saveAsTable("dlt_catalog.dlt_schema.quality_metrics")

display(df)

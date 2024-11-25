# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM event_log("34b561ea-5d0e-47e5-9d74-016df880ce65")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM event_log(TABLE(dlt_catalog.dlt_schema.raw_farmers_market))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW event_log_raw AS SELECT * FROM event_log("34b561ea-5d0e-47e5-9d74-016df880ce65")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW latest_update AS SELECT origin.update_id AS id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       event_log_raw,
# MAGIC       latest_update
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress'
# MAGIC       AND origin.update_id = latest_update.id
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name
# MAGIC

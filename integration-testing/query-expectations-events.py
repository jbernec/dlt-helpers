# Databricks notebook source
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

# Write the DataFrame to a Unity Catalog table
df.write.format("delta").saveAsTable("dlt_catalog.dlt_schema.quality_metrics")

from pyspark.sql import SparkSession

def get_rules_as_list_of_dict():
  return [
    {
      "name": "InvoiceNo",
      "constraint": "InvoiceNo IS NOT NULL",
      "tag": "validity"
    },
    {
      "name": "CustomerID",
      "constraint": "CustomerID IS NOT NULL",
      "tag": "validity"
    },
    {
      "name": "StockCode",
      "constraint": "StockCode IS NOT NULL",
      "tag": "validity"
    },
    {
      "name": "fresh_data",
      "constraint": "to_date(updateTime,'M/d/yyyy h:m:s a') > '2010-01-01'",
      "tag": "maintained"
    },
    {
      "name": "StockCode",
      "constraint": "NOT(StockCode IS NULL AND InvoiceNo IS NULL AND CustomerID IS NULL)",
      "tag": "maintained"
    },
    {
      "name": "invalid_character",
      "constraint": "StockCode RLIKE '[^a-zA-Z0-9_ ]'",
      "tag": "character_validity"
    }
  ]

def get_rules(tag):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  spark = SparkSession.builder \
          .appName("Pytest-PySpark-Testing") \
          .getOrCreate()
  df_rules = spark.createDataFrame(get_rules_as_list_of_dict())
  for row in df_rules.filter(col("tag") == tag).collect():
    rules[row['name']] = row['constraint']
  return rules

from pyspark.sql.functions import regexp_replace, col

# Define the function to remove "@" from the column value
def remove_at_symbol(df, column_name):
  pass
  return df.withColumn(column_name, regexp_replace(col(column_name), "@", ""))
  

from pyspark.sql.functions import concat, lit, col

# Define the function to append "@" to the end of the column value
def append_at_symbol(df, column_name):
  pass
  return df.withColumn(column_name, concat(col(column_name), lit("@")))

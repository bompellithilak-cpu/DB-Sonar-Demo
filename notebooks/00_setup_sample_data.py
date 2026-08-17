# Databricks notebook source
# MAGIC %md
# MAGIC # Seed the bronze table
# MAGIC
# MAGIC Creates `customers_bronze` with 10 rows: 6 clean, 4 deliberately dirty
# MAGIC (null id, bad email, impossible age, unknown segment) so the pipeline
# MAGIC has something to quarantine.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")  # noqa: F821
dbutils.widgets.text("schema", "demo")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")  # noqa: F821
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")  # noqa: F821

# COMMAND ----------

rows = [
    ("C001", "asha@example.com", 34, "RETAIL"),
    ("C002", "ben@example.com", 51, "wholesale"),
    ("C003", "chandra@example.com", 28, "Online"),
    ("C004", "deepa@example.com", 45, "RETAIL"),
    ("C005", "eshan@example.com", 63, "ONLINE"),
    ("C006", "farida@example.com", 22, "WHOLESALE"),
    # --- deliberately dirty ---
    (None, "ghost@example.com", 30, "RETAIL"),      # null customer_id
    ("C008", "not-an-email", 40, "RETAIL"),          # bad email
    ("C009", "irfan@example.com", 5, "RETAIL"),      # age below 18
    ("C010", "jaya@example.com", 33, "B2B"),         # unknown segment
]

df = spark.createDataFrame(rows, "customer_id string, email string, age int, segment string")  # noqa: F821

df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.customers_bronze")

print(f"seeded {df.count()} rows into {catalog}.{schema}.customers_bronze")

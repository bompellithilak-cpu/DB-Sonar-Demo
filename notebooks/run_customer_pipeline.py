# Databricks notebook source
# MAGIC %md
# MAGIC # Customer pipeline (bronze -> silver + quarantine)
# MAGIC
# MAGIC This notebook is deliberately **thin**. All logic lives in the
# MAGIC `customer_pipeline` wheel so it can be unit-tested and scanned properly.
# MAGIC
# MAGIC What SonarQube can and cannot see here:
# MAGIC  - lines starting with `# MAGIC` are comments to Sonar, so `%md` and
# MAGIC    `%sql` cells are invisible to it
# MAGIC  - `spark` and `dbutils` are injected by the Databricks runtime, so a
# MAGIC    scanner running outside Databricks would call them undefined names

# COMMAND ----------

dbutils.widgets.text("catalog", "main")  # noqa: F821
dbutils.widgets.text("schema", "demo")  # noqa: F821

catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821

# COMMAND ----------

from customer_pipeline import transforms

transforms.run(spark, catalog=catalog, schema=schema)  # noqa: F821

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sonar does not analyse this cell at all. SQL needs SQLFluff.
# MAGIC SELECT 'silver' AS layer, count(*) AS rows FROM identifier(:catalog || '.' || :schema || '.customers_silver')
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine', count(*) FROM identifier(:catalog || '.' || :schema || '.customers_quarantine');

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result: **6 rows in silver, 4 in quarantine.**
# MAGIC Each quarantined row keeps the reason, so the failure is auditable.

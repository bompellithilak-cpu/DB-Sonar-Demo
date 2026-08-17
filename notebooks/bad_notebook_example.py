# Databricks notebook source
# MAGIC %md
# MAGIC # The anti-pattern: everything inline in the notebook
# MAGIC
# MAGIC This is what most Databricks repos look like. Sonar will still scan it
# MAGIC (a source-format notebook is valid Python) and will flag the secret and
# MAGIC the smells - but it can never report **coverage** for this file, because
# MAGIC there is no way to unit-test a notebook cell. That is the whole argument
# MAGIC for moving logic into a package.

# COMMAND ----------

# Sonar S2068: hardcoded credential sitting in a notebook - very common in real life
STORAGE_KEY = "8f2b1c94a7de40f5b6c3128e9a0d7f41"

spark.conf.set(  # noqa: F821
    "fs.azure.account.key.demostorage.dfs.core.windows.net", STORAGE_KEY
)

# COMMAND ----------

df = spark.read.csv("/mnt/raw/customers", header=True, inferSchema=True)  # noqa: F821

unused_count = df.count()  # Sonar S1481: assigned and never used

if df.count() == None:  # Sonar S5727: comparison to None with ==
    print("empty")

df.write.mode("overwrite").saveAsTable("main.demo.customers_silver")

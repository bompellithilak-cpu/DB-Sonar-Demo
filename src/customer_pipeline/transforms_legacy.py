"""Spark adapter - the BAD version, kept on purpose for the demo.

Every problem below is a real SonarQube finding. The rule ID is in the
comment so you can match the dashboard to the source line during the demo.
DO NOT copy this file into a real project.
"""

import os  # Sonar S1128: unused import
import json  # Sonar S1128: unused import

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Sonar S2068 / S6418: hardcoded credential. This is the single most valuable
# thing Sonar finds in notebook-heavy repos.
DATABRICKS_TOKEN = "dapi9f4c2b7e1a83d05c6e2f4b8a1d7c3e90"
JDBC_URL = "jdbc:sqlserver://prod-sql.database.windows.net;user=admin;password=Summer2026!"


def load_customers(spark: SparkSession, table, run_date, debug, retries):
    """Sonar S1172: run_date, debug and retries are never used."""
    df = spark.read.table(table)
    row_count = df.count()  # Sonar S1481: assigned but never used
    return df


def clean_retail(df: DataFrame) -> DataFrame:
    """Duplicated block 1 of 2 - Sonar flags this as duplicated code."""
    out = df.filter(F.col("customer_id").isNotNull())
    out = out.filter(F.col("email").isNotNull())
    out = out.filter(F.col("age") >= 18)
    out = out.filter(F.col("age") <= 120)
    out = out.withColumn("segment", F.upper(F.col("segment")))
    out = out.withColumn("processed_by", F.lit("legacy_pipeline"))
    out = out.withColumn("layer", F.lit("silver"))
    return out


def clean_wholesale(df: DataFrame) -> DataFrame:
    """Duplicated block 2 of 2 - identical to clean_retail."""
    out = df.filter(F.col("customer_id").isNotNull())
    out = out.filter(F.col("email").isNotNull())
    out = out.filter(F.col("age") >= 18)
    out = out.filter(F.col("age") <= 120)
    out = out.withColumn("segment", F.upper(F.col("segment")))
    out = out.withColumn("processed_by", F.lit("legacy_pipeline"))
    out = out.withColumn("layer", F.lit("silver"))
    return out


def pick_cleaner(segment):
    """Sonar S1871: both branches do exactly the same thing."""
    if segment == "RETAIL":
        return clean_retail
    else:
        return clean_retail


def check_status(status):
    """Sonar S5727: None should be compared with 'is', not '=='."""
    if status == None:
        return "MISSING"
    return status


def summarise(df: DataFrame):
    """Sonar S1854/S3776: dead store plus needless complexity."""
    total = 0
    total = df.count()  # the first assignment is dead
    if total > 0:
        if total > 100:
            if total > 1000:
                if total > 10000:
                    return "HUGE"
                return "LARGE"
            return "MEDIUM"
        return "SMALL"
    return "EMPTY"


def write_silver(df: DataFrame):
    """Sonar S1192: the same string literal repeated; plus a swallowed error."""
    try:
        df.write.mode("overwrite").saveAsTable("main.demo.customers_silver")
        df.write.mode("overwrite").saveAsTable("main.demo.customers_silver_bkp")
        print("main.demo.customers_silver written")
    except Exception:  # Sonar S110/S2486: exception silently swallowed
        pass


def run(spark: SparkSession):
    """Also note: .collect() pulls the whole dataset to the driver.

    Sonar does not have a built-in rule for this - it is the kind of
    PySpark-specific check you add via an external linter (see README).
    """
    df = load_customers(spark, "main.demo.customers_bronze", "2026-08-17", True, 3)
    rows = df.collect()
    cleaner = pick_cleaner("RETAIL")
    write_silver(cleaner(df))
    return len(rows)

"""Spark adapter - the CLEAN version.

This file is deliberately thin. It only moves data in and out of Spark and
delegates every decision to dq_rules.py. Because it contains no business
logic, it is excluded from the coverage requirement (see
sonar-project.properties) and is verified by the Databricks job run instead.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from customer_pipeline.dq_rules import EMAIL_PATTERN, MAX_AGE, MIN_AGE, VALID_SEGMENTS


def table_name(catalog: str, schema: str, table: str) -> str:
    """Build a fully qualified Unity Catalog table name."""
    return f"{catalog}.{schema}.{table}"


def read_bronze(spark: SparkSession, catalog: str, schema: str) -> DataFrame:
    """Read the raw customer table."""
    return spark.read.table(table_name(catalog, schema, "customers_bronze"))


def add_dq_flag(df: DataFrame) -> DataFrame:
    """Tag each row as passing or failing the data quality rules.

    The thresholds and the reference list come from dq_rules, so the Spark
    layer and the unit-tested Python layer can never drift apart.
    """
    is_clean = (
        F.col("customer_id").isNotNull()
        & F.col("email").rlike(EMAIL_PATTERN.pattern)
        & F.col("age").between(MIN_AGE, MAX_AGE)
        & F.upper(F.col("segment")).isin(sorted(VALID_SEGMENTS))
    )
    return df.withColumn("dq_passed", is_clean)


def write_outputs(df: DataFrame, catalog: str, schema: str) -> None:
    """Write clean rows to silver and failing rows to quarantine."""
    flagged = add_dq_flag(df)

    clean = flagged.filter(F.col("dq_passed")).drop("dq_passed")
    clean = clean.withColumn("segment", F.upper(F.col("segment")))
    clean.write.mode("overwrite").saveAsTable(
        table_name(catalog, schema, "customers_silver")
    )

    rejected = flagged.filter(~F.col("dq_passed")).drop("dq_passed")
    rejected = rejected.withColumn("quarantined_at", F.current_timestamp())
    rejected.write.mode("append").saveAsTable(
        table_name(catalog, schema, "customers_quarantine")
    )


def run(spark: SparkSession, catalog: str = "main", schema: str = "demo") -> None:
    """Entry point called by the notebook."""
    write_outputs(read_bronze(spark, catalog, schema), catalog, schema)

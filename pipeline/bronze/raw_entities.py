# Databricks notebook source
# Bronze layer — raw entity data landing table
# Minimal transformation: schema enforcement + metadata columns only.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, TimestampType, StructType, StructField

# COMMAND ----------
@dlt.table(
    name="bronze_entities",
    comment="Raw entity records as landed from the source API. No transformation applied.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect("valid_ingestion_timestamp", "_ingested_at IS NOT NULL")
def bronze_entities():
    """
    Read raw entity records from the Delta table written by the ingestion job.
    In production, replace with Auto Loader (cloud_files) if reading from file storage.
    """
    catalog = spark.conf.get("pipelines.catalog", "main")
    schema = spark.conf.get("pipelines.schema", "pipeline_dev")

    return (
        spark.readStream
        .format("delta")
        .table(f"{catalog}.{schema}_raw.entities_raw")
        .withColumn("_pipeline_version", F.current_timestamp())
    )

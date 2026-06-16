"""
Silver Transform
================
Transforms the valid rows from the DQ engine into the cleaned Silver layer.
Applies configurable normalization: title-casing, trimming, type casting, imputation.
Only receives pre-validated rows — no additional filtering needed.
"""
import sys
import os
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, to_date, regexp_replace, trim, initcap, when, lit
)
from pipeline.config import BRONZE_PATH, SILVER_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, read_delta, write_delta


def _load_config():
    paths = [
        "/opt/airflow/pipeline/dq_config.yaml",
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "pipeline", "dq_config.yaml"),
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p) as f:
                return yaml.safe_load(f)
    return {}


def transform_silver(spark=None, valid_df: DataFrame = None, row_count: int = None):
    """
    Transform valid rows into Silver layer.
    If valid_df is provided (from DQ engine), use it directly.
    Otherwise fall back to reading Bronze and filtering internally.
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("SilverTransform")
        own_spark = True

    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

    try:
        # Get source data
        if valid_df is not None:
            bronze_df = valid_df
            print("[Silver] Using pre-validated DataFrame from DQ engine.")
        else:
            if not os.path.exists(BRONZE_PATH):
                print("[Silver] Bronze path does not exist. Skipping.")
                return 0, None
            bronze_df = read_delta(spark, BRONZE_PATH)

        config = _load_config()
        cleaning = config.get("cleaning", {})
        age_cfg  = config.get("columns", {}).get("age", {})

        # ---- Normalize ID ----
        cleaned_id  = regexp_replace(trim(col("id")),  r"\.0+$", "").cast("int")

        # ---- Normalize Age ----
        cleaned_age = regexp_replace(trim(col("age")), r"\.0+$", "").cast("int")
        null_impute = age_cfg.get("null_impute_value", 45)
        final_age   = when(cleaned_age.isNull(), lit(null_impute)).otherwise(cleaned_age)

        # ---- Normalize Name ----
        name_col = trim(col("name"))
        if cleaning.get("name", {}).get("title_case", True):
            name_col = initcap(name_col)

        # Build transformed DataFrame
        transformed_df = bronze_df \
            .withColumn("id",   cleaned_id) \
            .withColumn("age",  final_age) \
            .withColumn("name", name_col)

        # Deduplicate on ID (safety net for any edge cases the DQ engine may miss)
        if valid_df is not None:
            final_df = transformed_df
        else:
            final_df = transformed_df.dropDuplicates(["id"])

        # Add processed_date partition column
        final_df = final_df.withColumn("processed_date", to_date("ingestion_time"))

        if row_count is None:
            row_count = final_df.count()
        if row_count > 0:
            write_delta(final_df, SILVER_PATH, mode="overwrite", partition_by="processed_date")
            print(f"[Silver] Transformation Success: {row_count} rows written.")
        else:
            print("[Silver] No valid rows to write to Silver.")

        return row_count, (final_df if row_count > 0 else None)

    except Exception as e:
        print(f"[Silver] Transformation Failed: {str(e)}")
        if own_spark:
            sys.exit(1)
        raise e
    finally:
        if own_spark:
            spark.stop()


if __name__ == "__main__":
    transform_silver()

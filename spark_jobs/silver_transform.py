"""
Silver Transform
================
Transforms the valid rows from the DQ engine into the cleaned Silver layer.
Applies schema-specific casting and dynamic type-inference for Generic mode.
"""
import sys
import os
import yaml
import pandas as pd
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


def transform_silver(spark=None, valid_df: DataFrame = None, row_count: int = None, dataset_type: str = "GENERIC"):
    """
    Transform valid rows into Silver layer.
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
            print(f"[Silver] Using pre-validated DataFrame from DQ engine. Mode: {dataset_type}")
        else:
            if not os.path.exists(BRONZE_PATH):
                print("[Silver] Bronze path does not exist. Skipping.")
                return 0, None
            bronze_df = read_delta(spark, BRONZE_PATH)

        config = _load_config()

        # Build transformed DataFrame based on dataset type
        if dataset_type == "CUSTOMER":
            cleaning = config.get("cleaning", {})
            age_cfg  = config.get("columns", {}).get("age", {})
            transformed_df = bronze_df
            
            if "id" in bronze_df.columns:
                cleaned_id  = regexp_replace(trim(col("id")),  r"\.0+$", "").cast("int")
                transformed_df = transformed_df.withColumn("id", cleaned_id)
                
            if "age" in bronze_df.columns:
                cleaned_age = regexp_replace(trim(col("age")), r"\.0+$", "").cast("int")
                null_impute = age_cfg.get("null_impute_value", 45)
                final_age   = when(cleaned_age.isNull(), lit(null_impute)).otherwise(cleaned_age)
                transformed_df = transformed_df.withColumn("age", final_age)
                
            if "name" in bronze_df.columns:
                name_col = trim(col("name"))
                if cleaning.get("name", {}).get("title_case", True):
                    name_col = initcap(name_col)
                transformed_df = transformed_df.withColumn("name", name_col)
                
            if "id" in transformed_df.columns:
                final_df = transformed_df.dropDuplicates(["id"])
            else:
                final_df = transformed_df

        elif dataset_type == "ORDERS":
            transformed_df = bronze_df
            if "order_id" in bronze_df.columns:
                transformed_df = transformed_df.withColumn("order_id", trim(col("order_id")))
            if "quantity" in bronze_df.columns:
                transformed_df = transformed_df.withColumn("quantity", regexp_replace(trim(col("quantity")), r"\.0+$", "").cast("int"))
            if "unit_price" in bronze_df.columns:
                transformed_df = transformed_df.withColumn("unit_price", trim(col("unit_price")).cast("double"))
            if "product_name" in bronze_df.columns:
                transformed_df = transformed_df.withColumn("product_name", regexp_replace(trim(col("product_name")), r"\s+", " "))
                
            if "order_id" in transformed_df.columns:
                final_df = transformed_df.dropDuplicates(["order_id"])
            else:
                final_df = transformed_df

        else:  # GENERIC mode
            # 1. Take a sample for fast type inference using Pandas
            sample_pdf = bronze_df.limit(1000).toPandas()
            col_types = {}
            for c in sample_pdf.columns:
                if c in ["ingestion_time", "source_file", "processed_date"]:
                    continue
                series = sample_pdf[c].dropna()
                if series.dtype == object:
                    series = series.str.strip()
                    series = series[series != ""]
                if series.empty:
                    col_types[c] = "string"
                    continue
                
                # Check Integer
                try:
                    num_series = pd.to_numeric(series)
                    if (num_series % 1 == 0).all():
                        col_types[c] = "int"
                    else:
                        col_types[c] = "double"
                    continue
                except Exception:
                    pass
                    
                # Check Date
                try:
                    if series.astype(str).str.len().mean() >= 8:
                        pd.to_datetime(series)
                        col_types[c] = "date"
                        continue
                except Exception:
                    pass
                    
                col_types[c] = "string"

            # 2. Apply transformations to Bronze DataFrame
            transformed_df = bronze_df
            for c, t in col_types.items():
                if t == "int":
                    transformed_df = transformed_df.withColumn(c, regexp_replace(trim(col(c)), r"\.0+$", "").cast("int"))
                elif t == "double":
                    transformed_df = transformed_df.withColumn(c, trim(col(c)).cast("double"))
                elif t == "date":
                    transformed_df = transformed_df.withColumn(c, to_date(col(c)))
                else:
                    transformed_df = transformed_df.withColumn(c, regexp_replace(trim(col(c)), r"\s+", " "))
            
            final_df = transformed_df

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

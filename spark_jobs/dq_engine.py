"""
Enterprise Data Quality Engine
================================
Row-level validation. Returns two DataFrames: valid_df and invalid_df.
Every rejected row is tagged with the reason and rule violated.
Supports CUSTOMER, ORDERS, and GENERIC dataset modes.
"""
import os
import yaml
import json
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, trim, regexp_replace, when, lit, array, array_remove,
    concat_ws, current_timestamp
)


def _load_config():
    search_paths = [
        os.environ.get("DQ_CONFIG_FILE", ""),
        "/opt/airflow/pipeline/dq_config.yaml",
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "pipeline", "dq_config.yaml"),
    ]
    for path in search_paths:
        if path and os.path.exists(path):
            with open(path, "r") as f:
                return yaml.safe_load(f)
    return {}


def run_dq_engine(df: DataFrame, run_id: str = "unknown", source_file: str = "unknown", dataset_type: str = "GENERIC"):
    """
    Main DQ engine entry point.
    """
    if dataset_type == "CUSTOMER":
        return _run_customer_dq(df, run_id, source_file)
    elif dataset_type == "ORDERS":
        return _run_orders_dq(df, run_id, source_file)
    else:
        return _run_generic_dq(df, run_id, source_file)


def _run_customer_dq(df: DataFrame, run_id: str, source_file: str):
    cols = df.columns
    
    # ---- Normalize numeric string fields & Per-row violation flags ----
    if "id" in cols:
        cleaned_id  = regexp_replace(trim(col("id")),  r"\.0+$", "")
        parsed_id   = cleaned_id.cast("int")
        is_null_id      = col("id").isNull()  | (trim(col("id")) == "")
        is_malformed_id = (~is_null_id) & parsed_id.isNull()
    else:
        cleaned_id  = lit(None).cast("string")
        parsed_id   = lit(None).cast("int")
        is_null_id      = lit(False)
        is_malformed_id = lit(False)

    if "age" in cols:
        cleaned_age = regexp_replace(trim(col("age")), r"\.0+$", "")
        parsed_age  = cleaned_age.cast("int")
        is_null_age     = col("age").isNull() | (trim(col("age")) == "")
        is_invalid_age  = (~is_null_age) & (
            parsed_age.isNull() | (parsed_age < 0) | (parsed_age > 120)
        )
    else:
        cleaned_age = lit(None).cast("string")
        parsed_age  = lit(None).cast("int")
        is_null_age     = lit(False)
        is_invalid_age  = lit(False)

    if "name" in cols:
        is_null_name    = col("name").isNull() | (trim(col("name")) == "")
        is_suspicious_name = (~is_null_name) & (
            regexp_replace(trim(col("name")), r"[^0-9]", "").cast("long").isNotNull() &
            (regexp_replace(trim(col("name")), r"[^0-9]", "") == trim(col("name")))
        )
    else:
        is_null_name = lit(False)
        is_suspicious_name = lit(False)

    # ---- Tag each row with all violations ----
    tagged_df = df \
        .withColumn("__norm_id",  cleaned_id) \
        .withColumn("__norm_age", cleaned_age) \
        .withColumn("__flag_null_id",      when(is_null_id,          lit("null_id")).otherwise(lit(None))) \
        .withColumn("__flag_malformed_id", when(is_malformed_id,     lit("malformed_id")).otherwise(lit(None))) \
        .withColumn("__flag_null_age",     when(is_null_age,         lit("null_age")).otherwise(lit(None))) \
        .withColumn("__flag_invalid_age",  when(is_invalid_age,      lit("invalid_age")).otherwise(lit(None))) \
        .withColumn("__flag_null_name",    when(is_null_name,        lit("null_name")).otherwise(lit(None))) \
        .withColumn("__flag_suspicious_name", when(is_suspicious_name, lit("suspicious_name")).otherwise(lit(None)))

    flag_cols = [
        "__flag_null_id", "__flag_malformed_id",
        "__flag_null_age", "__flag_invalid_age",
        "__flag_null_name", "__flag_suspicious_name",
    ]
    tagged_df = tagged_df.withColumn(
        "__violations",
        array_remove(array(*[col(c) for c in flag_cols]), None)
    )
    
    is_null_id_col = is_null_id if "id" in cols else lit(False)
    is_malformed_id_col = is_malformed_id if "id" in cols else lit(False)
    is_invalid_age_col = is_invalid_age if "age" in cols else lit(False)
    
    tagged_df = tagged_df.withColumn(
        "__has_critical_violation",
        is_null_id_col | is_malformed_id_col | is_invalid_age_col
    )

    # ---- Duplicate detection on normalized ID (batch-level) ----
    if "id" in cols:
        windowSpec = Window.partitionBy("__norm_id")
        tagged_df = tagged_df.withColumn("__id_count", F.count("*").over(windowSpec))
        tagged_df = tagged_df.withColumn(
            "__is_duplicate",
            (~is_null_id) & (~is_malformed_id) & (col("__id_count") > 1)
        )
        tagged_df = tagged_df.withColumn(
            "__violations",
            when(
                col("__is_duplicate"),
                F.concat(col("__violations"), array(lit("duplicate_id")))
            ).otherwise(col("__violations"))
        )
        tagged_df = tagged_df.withColumn(
            "__has_critical_violation",
            col("__has_critical_violation") | col("__is_duplicate")
        )
    else:
        tagged_df = tagged_df \
            .withColumn("__is_duplicate", lit(False)) \
            .withColumn("__id_count", lit(1))

    # ---- Split valid / invalid ----
    internal_cols = [c for c in tagged_df.columns if c.startswith("__")]

    valid_df = tagged_df.filter(~col("__has_critical_violation")) \
        .drop(*internal_cols)

    invalid_df = tagged_df.filter(col("__has_critical_violation")) \
        .withColumn("quarantine_reason",  concat_ws(", ", col("__violations"))) \
        .withColumn("rule_violated",       col("quarantine_reason")) \
        .withColumn("quarantine_time",     current_timestamp()) \
        .withColumn("run_id",              lit(run_id)) \
        .withColumn("dq_source_file",      lit(source_file)) \
        .drop(*[c for c in internal_cols if c not in ("__violations",)]) \
        .drop("__violations")

    # ---- Scorecard (Consolidated Pass) ----
    tagged_df = tagged_df.cache()
    
    agg_exprs = [
        F.count("*").alias("total"),
        F.sum(F.when(col("__has_critical_violation"), 1).otherwise(0)).alias("invalid_count"),
        F.sum(F.when(is_null_id, 1).otherwise(0)).alias("null_ids_count"),
        F.sum(F.when(is_malformed_id, 1).otherwise(0)).alias("malformed_id_count"),
        F.sum(F.when(col("__is_duplicate") == True, 1).otherwise(0)).alias("dup_count"),
        F.sum(F.when(is_invalid_age, 1).otherwise(0)).alias("invalid_age_count"),
        F.sum(F.when(is_null_age, 1).otherwise(0)).alias("null_age_count"),
        F.sum(F.when(is_null_name, 1).otherwise(0)).alias("null_name_count")
    ]
    
    metrics = tagged_df.select(*agg_exprs).collect()[0]

    total = metrics["total"] or 0
    invalid_count = metrics["invalid_count"] or 0
    valid_count = total - invalid_count
    dq_score = (valid_count / total * 100) if total > 0 else 100.0

    scorecard = {
        "run_id":          run_id,
        "source_file":     source_file,
        "total_rows":      total,
        "valid_rows":      valid_count,
        "invalid_rows":    invalid_count,
        "dq_score":        round(dq_score, 2),
        "null_ids":        metrics["null_ids_count"] or 0,
        "malformed_ids":   metrics["malformed_id_count"] or 0,
        "duplicate_ids":   metrics["dup_count"] or 0,
        "null_ages":       metrics["null_age_count"] or 0,
        "invalid_ages":    metrics["invalid_age_count"] or 0,
        "null_names":      metrics["null_name_count"] or 0,
        "dataset_type":    "CUSTOMER"
    }

    print(f"[DQ Engine - CUSTOMER] Total={total} | Valid={valid_count} | Invalid={invalid_count} | Score={dq_score:.1f}%")
    return valid_df, invalid_df, scorecard


def _run_orders_dq(df: DataFrame, run_id: str, source_file: str):
    # Required columns for orders dataset: order_id, product_name, quantity, unit_price
    cols = df.columns
    
    is_null_order_id = col("order_id").isNull() | (trim(col("order_id")) == "")
    
    is_null_qty = col("quantity").isNull() | (trim(col("quantity")) == "")
    cleaned_qty = regexp_replace(trim(col("quantity")), r"\.0+$", "")
    parsed_qty = cleaned_qty.cast("int")
    is_invalid_qty = (~is_null_qty) & (parsed_qty.isNull() | (parsed_qty < 1))
    
    is_null_price = col("unit_price").isNull() | (trim(col("unit_price")) == "")
    parsed_price = trim(col("unit_price")).cast("double")
    is_invalid_price = (~is_null_price) & (parsed_price.isNull() | (parsed_price < 0.0))

    tagged_df = df \
        .withColumn("__flag_null_order_id", when(is_null_order_id, lit("null_order_id")).otherwise(lit(None))) \
        .withColumn("__flag_null_qty",      when(is_null_qty,      lit("null_qty")).otherwise(lit(None))) \
        .withColumn("__flag_invalid_qty",   when(is_invalid_qty,   lit("invalid_qty")).otherwise(lit(None))) \
        .withColumn("__flag_null_price",    when(is_null_price,    lit("null_price")).otherwise(lit(None))) \
        .withColumn("__flag_invalid_price",  when(is_invalid_price,  lit("invalid_price")).otherwise(lit(None)))

    flag_cols = ["__flag_null_order_id", "__flag_null_qty", "__flag_invalid_qty", "__flag_null_price", "__flag_invalid_price"]
    tagged_df = tagged_df.withColumn("__violations", array_remove(array(*[col(c) for c in flag_cols]), None))
    
    tagged_df = tagged_df.withColumn(
        "__has_critical_violation",
        is_null_order_id | is_invalid_qty | is_invalid_price
    )

    # Duplicate order_id detection
    windowSpec = Window.partitionBy("order_id")
    tagged_df = tagged_df.withColumn("__order_id_count", F.count("*").over(windowSpec))
    tagged_df = tagged_df.withColumn(
        "__is_duplicate",
        (~is_null_order_id) & (col("__order_id_count") > 1)
    )
    tagged_df = tagged_df.withColumn(
        "__violations",
        when(col("__is_duplicate"), F.concat(col("__violations"), array(lit("duplicate_order_id")))).otherwise(col("__violations"))
    )
    tagged_df = tagged_df.withColumn(
        "__has_critical_violation",
        col("__has_critical_violation") | col("__is_duplicate")
    )

    internal_cols = [c for c in tagged_df.columns if c.startswith("__")]
    valid_df = tagged_df.filter(~col("__has_critical_violation")).drop(*internal_cols)
    
    invalid_df = tagged_df.filter(col("__has_critical_violation")) \
        .withColumn("quarantine_reason",  concat_ws(", ", col("__violations"))) \
        .withColumn("rule_violated",       col("quarantine_reason")) \
        .withColumn("quarantine_time",     current_timestamp()) \
        .withColumn("run_id",              lit(run_id)) \
        .withColumn("dq_source_file",      lit(source_file)) \
        .drop(*[c for c in internal_cols if c not in ("__violations",)]) \
        .drop("__violations")

    tagged_df = tagged_df.cache()
    agg_exprs = [
        F.count("*").alias("total"),
        F.sum(F.when(col("__has_critical_violation"), 1).otherwise(0)).alias("invalid_count"),
        F.sum(F.when(is_null_order_id, 1).otherwise(0)).alias("null_order_id_count"),
        F.sum(F.when(is_invalid_qty, 1).otherwise(0)).alias("invalid_qty_count"),
        F.sum(F.when(is_invalid_price, 1).otherwise(0)).alias("invalid_price_count"),
        F.sum(F.when(col("__is_duplicate") == True, 1).otherwise(0)).alias("dup_count")
    ]
    metrics = tagged_df.select(*agg_exprs).collect()[0]
    total = metrics["total"] or 0
    invalid_count = metrics["invalid_count"] or 0
    valid_count = total - invalid_count
    dq_score = (valid_count / total * 100) if total > 0 else 100.0

    scorecard = {
        "run_id":          run_id,
        "source_file":     source_file,
        "total_rows":      total,
        "valid_rows":      valid_count,
        "invalid_rows":    invalid_count,
        "dq_score":        round(dq_score, 2),
        "null_order_ids":  metrics["null_order_id_count"] or 0,
        "invalid_qty":     metrics["invalid_qty_count"] or 0,
        "invalid_price":   metrics["invalid_price_count"] or 0,
        "duplicate_ids":   metrics["dup_count"] or 0,
        "dataset_type":    "ORDERS"
    }
    
    print(f"[DQ Engine - ORDERS] Total={total} | Valid={valid_count} | Invalid={invalid_count} | Score={dq_score:.1f}%")
    return valid_df, invalid_df, scorecard


def _run_generic_dq(df: DataFrame, run_id: str, source_file: str):
    # Generic Ingestion: No row quarantine. Calculate metadata dynamically.
    col_count = len(df.columns)
    
    # single-pass null, distinct, and total count aggregation
    agg_exprs = [F.count("*").alias("total_rows")]
    for c in df.columns:
        if c in ["ingestion_time", "source_file"]:
            continue
        agg_exprs.append(F.sum(F.when(col(c).isNull() | (trim(col(c)) == ""), 1).otherwise(0)).alias(f"{c}_nulls"))
        agg_exprs.append(F.countDistinct(c).alias(f"{c}_distinct"))

    metrics_row = df.select(*agg_exprs).collect()[0] if agg_exprs else {}
    total_rows = int(metrics_row.get("total_rows", 0) or 0)
    
    if total_rows == 0:
        scorecard = {
            "run_id":          run_id,
            "source_file":     source_file,
            "total_rows":      0,
            "valid_rows":      0,
            "invalid_rows":    0,
            "dq_score":        100.0,
            "dataset_type":    "GENERIC",
            "duplicate_rate":  0.0,
            "total_columns":   col_count,
            "completeness_score": 100.0,
            "column_metrics":  "{}"
        }
        empty_schema = df.schema
        spark = df.sparkSession
        empty_df = spark.createDataFrame([], empty_schema)
        return df, empty_df, scorecard

    # Only count distinct rows once to get duplicate_rate
    distinct_rows = df.drop("ingestion_time", "source_file").distinct().count()
    duplicate_rows = total_rows - distinct_rows
    dup_rate = (duplicate_rows / total_rows * 100.0)
    
    total_non_nulls = 0
    dtypes = {name: dtype for name, dtype in df.dtypes}
    
    column_report = {}
    for c in df.columns:
        if c in ["ingestion_time", "source_file"]:
            continue
        nulls = metrics_row.get(f"{c}_nulls", 0) or 0
        distinct = metrics_row.get(f"{c}_distinct", 0) or 0
        non_nulls = max(0, total_rows - nulls)
        total_non_nulls += non_nulls
        
        column_report[c] = {
            "null_count": int(nulls),
            "null_percentage": round(nulls / total_rows * 100.0, 2),
            "distinct_count": int(distinct),
            "datatype": dtypes.get(c, "string")
        }

    total_cells = total_rows * len(column_report)
    completeness = (total_non_nulls / total_cells * 100.0) if total_cells > 0 else 100.0

    scorecard = {
        "run_id":          run_id,
        "source_file":     source_file,
        "total_rows":      total_rows,
        "valid_rows":      total_rows,
        "invalid_rows":    0,
        "dq_score":        round(completeness, 2),
        "dataset_type":    "GENERIC",
        "duplicate_rate":  round(dup_rate, 2),
        "total_columns":   col_count,
        "completeness_score": round(completeness, 2),
        "column_metrics":  column_report
    }

    # Empty DataFrame placeholder for invalid rows
    empty_df = df.sparkSession.createDataFrame([], df.schema)

    print(f"[DQ Engine - GENERIC] Total={total_rows} | Cols={col_count} | Completeness={completeness:.1f}% | Dups={dup_rate:.1f}%")
    return df, empty_df, scorecard


def write_dq_scorecard(scorecard: dict, spark: SparkSession):
    """Persist the DQ scorecard to the dq_run_report Delta table."""
    try:
        from pipeline.config import DQ_REPORT_PATH
        from pipeline.delta_utils import write_delta

        row = {}
        for k, v in scorecard.items():
            if isinstance(v, dict):
                row[k] = json.dumps(v)
            elif isinstance(v, (int, float)):
                row[k] = float(v)
            else:
                row[k] = str(v)
                
        df = spark.createDataFrame([row])
        df = df.withColumn("report_time", current_timestamp())
        # Append with automatic schema evolution
        write_delta(df, DQ_REPORT_PATH, mode="append")
        print(f"[DQ Engine] Scorecard written to {DQ_REPORT_PATH}")
    except Exception as e:
        print(f"[DQ Engine] Warning: Could not write scorecard: {e}")


def exceeds_threshold(scorecard: dict, config: dict) -> tuple:
    """
    Returns (should_fail: bool, reason: str).
    Checks thresholds. Disabled for Generic datasets.
    """
    dataset_type = scorecard.get("dataset_type", "GENERIC")
    if dataset_type == "GENERIC":
        return False, ""  # Never fail generic runs on threshold mismatch
        
    thresholds = config.get("thresholds", {})
    total = scorecard.get("total_rows", 1) or 1
    
    if dataset_type == "CUSTOMER":
        null_id_pct = scorecard.get("null_ids", 0) / total
        max_null_pct = thresholds.get("max_null_id_pct", 0.5)
        if null_id_pct > max_null_pct:
            return True, f"Null ID rate {null_id_pct:.1%} exceeds threshold {max_null_pct:.1%}"
            
    min_rows = thresholds.get("min_row_count", 1)
    if total < min_rows:
        return True, f"Row count {total} is below minimum threshold {min_rows}"
    return False, ""

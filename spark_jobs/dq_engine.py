"""
Enterprise Data Quality Engine
================================
Row-level validation. Returns two DataFrames: valid_df and invalid_df.
Every rejected row is tagged with the reason and rule violated.
The pipeline NEVER silently discards data.
"""
import os
import yaml
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


def run_dq_engine(df: DataFrame, run_id: str = "unknown", source_file: str = "unknown"):
    """
    Main DQ engine entry point.

    Returns:
        valid_df     — rows that pass all critical rules
        invalid_df   — rows that violated one or more rules (with tags)
        scorecard    — dict with DQ summary metrics
    """
    config = _load_config()
    col_cfg = config.get("columns", {})
    thresholds = config.get("thresholds", {})

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

    # Collect all flags into a single array
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

    # ---- Scorecard (Consolidated Single Aggregation Pass) ----
    tagged_df = tagged_df.cache()
    
    agg_exprs = [
        F.count("*").alias("total"),
        F.sum(F.when(col("__has_critical_violation"), 1).otherwise(0)).alias("invalid_count")
    ]
    
    if "id" in cols:
        agg_exprs.append(F.sum(F.when(is_null_id, 1).otherwise(0)).alias("null_ids_count"))
        agg_exprs.append(F.sum(F.when(is_malformed_id, 1).otherwise(0)).alias("malformed_id_count"))
        agg_exprs.append(F.sum(F.when(col("__is_duplicate") == True, 1).otherwise(0)).alias("dup_count"))
    else:
        agg_exprs.append(F.lit(0).alias("null_ids_count"))
        agg_exprs.append(F.lit(0).alias("malformed_id_count"))
        agg_exprs.append(F.lit(0).alias("dup_count"))
        
    if "age" in cols:
        agg_exprs.append(F.sum(F.when(is_invalid_age, 1).otherwise(0)).alias("invalid_age_count"))
        agg_exprs.append(F.sum(F.when(is_null_age, 1).otherwise(0)).alias("null_age_count"))
    else:
        agg_exprs.append(F.lit(0).alias("invalid_age_count"))
        agg_exprs.append(F.lit(0).alias("null_age_count"))
        
    if "name" in cols:
        agg_exprs.append(F.sum(F.when(is_null_name, 1).otherwise(0)).alias("null_name_count"))
    else:
        agg_exprs.append(F.lit(0).alias("null_name_count"))
        
    metrics = tagged_df.select(*agg_exprs).collect()[0]

    total = metrics["total"] or 0
    invalid_count = metrics["invalid_count"] or 0
    valid_count = total - invalid_count
    dq_score = (valid_count / total * 100) if total > 0 else 100.0

    null_ids_count = metrics["null_ids_count"] or 0
    malformed_id_count = metrics["malformed_id_count"] or 0
    invalid_age_count = metrics["invalid_age_count"] or 0
    null_age_count = metrics["null_age_count"] or 0
    null_name_count = metrics["null_name_count"] or 0
    dup_count = metrics["dup_count"] or 0

    scorecard = {
        "run_id":          run_id,
        "source_file":     source_file,
        "total_rows":      total,
        "valid_rows":      valid_count,
        "invalid_rows":    invalid_count,
        "dq_score":        round(dq_score, 2),
        "null_ids":        null_ids_count,
        "malformed_ids":   malformed_id_count,
        "duplicate_ids":   dup_count,
        "null_ages":       null_age_count,
        "invalid_ages":    invalid_age_count,
        "null_names":      null_name_count,
    }

    print(f"[DQ Engine] Total={total} | Valid={valid_count} | Invalid={invalid_count} | Score={dq_score:.1f}%")
    return valid_df, invalid_df, scorecard


def write_dq_scorecard(scorecard: dict, spark: SparkSession):
    """Persist the DQ scorecard to the dq_run_report Delta table."""
    try:
        from pipeline.config import DQ_REPORT_PATH
        from pipeline.delta_utils import write_delta

        row = {k: (float(v) if isinstance(v, (int, float)) else str(v))
               for k, v in scorecard.items()}
        df = spark.createDataFrame([row])
        df = df.withColumn("report_time", current_timestamp())
        write_delta(df, DQ_REPORT_PATH, mode="append")
        print(f"[DQ Engine] Scorecard written to {DQ_REPORT_PATH}")
    except Exception as e:
        print(f"[DQ Engine] Warning: Could not write scorecard: {e}")


def exceeds_threshold(scorecard: dict, config: dict) -> tuple:
    """
    Returns (should_fail: bool, reason: str).
    Checks configured thresholds against scorecard values.
    """
    thresholds = config.get("thresholds", {})
    total = scorecard.get("total_rows", 1) or 1
    null_id_pct = scorecard.get("null_ids", 0) / total
    max_null_pct = thresholds.get("max_null_id_pct", 0.5)
    if null_id_pct > max_null_pct:
        return True, f"Null ID rate {null_id_pct:.1%} exceeds threshold {max_null_pct:.1%}"
    min_rows = thresholds.get("min_row_count", 1)
    if total < min_rows:
        return True, f"Row count {total} is below minimum threshold {min_rows}"
    return False, ""

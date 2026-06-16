"""
Data Profiler
=============
Generates per-run statistical profiles for each column in the Bronze dataset.
Detects anomalies by comparing against the previous run's profile.
Results are stored in the data_profile Delta table.
"""
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when, isnan, lit, current_timestamp
from pipeline.config import PROFILE_PATH
from pipeline.delta_utils import write_delta


def profile_dataframe(df: DataFrame, run_id: str, source_file: str, spark: SparkSession):
    """
    Compute statistical profile for each column.
    Returns a list of profile records and detected anomalies.
    """
    profile_records = []
    anomalies = []
    
    total_rows = df.count()
    if total_rows == 0:
        return [], [], 0

    columns_to_profile = [c for c in df.columns if c not in ("ingestion_time", "source_file")]

    # 1. Single aggregation pass for all standard stats
    agg_exprs = []
    for c in columns_to_profile:
        # Null count
        agg_exprs.append(F.sum(F.when(col(c).isNull() | (F.trim(col(c)) == ""), 1).otherwise(0)).alias(f"null_cnt_{c}"))
        # Distinct count
        agg_exprs.append(F.countDistinct(col(c)).alias(f"distinct_cnt_{c}"))
        # Numeric stats (cast to double)
        num_col = col(c).cast("double")
        agg_exprs.append(F.min(num_col).alias(f"min_{c}"))
        agg_exprs.append(F.max(num_col).alias(f"max_{c}"))
        agg_exprs.append(F.avg(num_col).alias(f"avg_{c}"))
        agg_exprs.append(F.stddev(num_col).alias(f"stddev_{c}"))

    stats_row = df.agg(*agg_exprs).collect()[0]

    # 2. Extract stats and determine bounds for outlier pass
    bounds = {}
    outlier_exprs = []
    for c in columns_to_profile:
        mean_val = stats_row[f"avg_{c}"]
        stddev_val = stats_row[f"stddev_{c}"]
        if mean_val is not None and stddev_val is not None and stddev_val > 0:
            lower = mean_val - 3 * stddev_val
            upper = mean_val + 3 * stddev_val
            bounds[c] = (lower, upper, mean_val, stddev_val)
            num_col = col(c).cast("double")
            outlier_exprs.append(
                F.sum(F.when((num_col < lower) | (num_col > upper), 1).otherwise(0)).alias(f"outliers_{c}")
            )
        else:
            bounds[c] = None

    # 3. Single aggregation pass for all outliers
    outliers_row = None
    if outlier_exprs:
        outliers_row = df.agg(*outlier_exprs).collect()[0]

    # 4. Generate profile records
    for c in columns_to_profile:
        null_count = stats_row[f"null_cnt_{c}"] or 0
        null_pct = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0.0

        distinct_count = stats_row[f"distinct_cnt_{c}"] or 0
        dup_count = total_rows - distinct_count
        dup_pct = round(dup_count / total_rows * 100, 2) if total_rows > 0 else 0.0

        # Top 5 values
        top_values = []
        try:
            top_df = df.groupBy(col(c)).count().orderBy(F.desc("count")).limit(5)
            top_values = [str(r[c]) for r in top_df.collect()]
        except Exception:
            pass

        min_val = stats_row[f"min_{c}"]
        max_val = stats_row[f"max_{c}"]
        mean_val = round(stats_row[f"avg_{c}"], 2) if stats_row[f"avg_{c}"] is not None else None
        stddev_val = round(stats_row[f"stddev_{c}"], 2) if stats_row[f"stddev_{c}"] is not None else None

        outlier_count = 0
        if bounds[c] and outliers_row:
            outlier_count = outliers_row[f"outliers_{c}"] or 0
            if outlier_count > 0:
                lower, upper, _, _ = bounds[c]
                anomalies.append(
                    f"Column '{c}': {outlier_count} outlier(s) detected (outside [{lower:.1f}, {upper:.1f}])"
                )

        profile_records.append({
            "run_id":          run_id,
            "source_file":     source_file,
            "column_name":     c,
            "total_rows":      float(total_rows),
            "null_count":      float(null_count),
            "null_pct":        float(null_pct),
            "distinct_count":  float(distinct_count),
            "duplicate_count": float(dup_count),
            "duplicate_pct":   float(dup_pct),
            "min_value":       str(min_val) if min_val is not None else None,
            "max_value":       str(max_val) if max_val is not None else None,
            "mean_value":      str(mean_val) if mean_val is not None else None,
            "stddev_value":    str(stddev_val) if stddev_val is not None else None,
            "outlier_count":   float(outlier_count),
            "top_values":      ", ".join(top_values),
        })

    # Volume anomaly detection vs previous run
    anomalies += _detect_volume_anomalies(spark, total_rows, run_id)
    
    # Clean cache done by caller

    return profile_records, anomalies, total_rows


def write_profile(profile_records: list, spark: SparkSession):
    """Persist profile records to the data_profile Delta table."""
    if not profile_records:
        return
    try:
        profile_df = spark.createDataFrame(profile_records)
        profile_df = profile_df.withColumn("profile_time", current_timestamp())
        write_delta(profile_df, PROFILE_PATH, mode="append")
        print(f"[Profiler] {len(profile_records)} column profiles written.")
    except Exception as e:
        print(f"[Profiler] WARNING: Could not write profile: {e}")


def _detect_volume_anomalies(spark: SparkSession, current_rows: int, run_id: str):
    """Compare current row count against the last profile to detect volume anomalies."""
    anomalies = []
    try:
        if not os.path.exists(PROFILE_PATH):
            return anomalies
        prev_df = spark.read.format("delta").load(PROFILE_PATH) \
            .filter(col("column_name") == "id") \
            .filter(col("run_id") != run_id) \
            .orderBy(col("profile_time").desc()).limit(1)
        rows = prev_df.collect()
        if not rows:
            return anomalies
        prev_rows = int(rows[0]["total_rows"])
        if prev_rows > 0:
            ratio = current_rows / prev_rows
            if ratio < 0.70:
                anomalies.append(f"Volume DROP: {current_rows:,} rows vs previous {prev_rows:,} ({ratio:.0%})")
            elif ratio > 2.0:
                anomalies.append(f"Volume SPIKE: {current_rows:,} rows vs previous {prev_rows:,} ({ratio:.0%})")
    except Exception:
        pass
    return anomalies

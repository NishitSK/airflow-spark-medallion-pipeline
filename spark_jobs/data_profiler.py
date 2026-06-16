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

    numeric_cols = [f.name for f in df.schema.fields if str(f.dataType) in ("IntegerType()", "LongType()", "DoubleType()", "FloatType()")]
    string_cols  = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]

    for col_name in df.columns:
        if col_name in ("ingestion_time", "source_file"):
            continue

        null_count = df.filter(col(col_name).isNull()).count()
        null_pct   = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0.0

        distinct_count = df.select(col_name).distinct().count()
        dup_count  = total_rows - distinct_count
        dup_pct    = round(dup_count / total_rows * 100, 2) if total_rows > 0 else 0.0

        # Top 5 values
        top_values = []
        try:
            top_df = df.groupBy(col_name).count().orderBy(F.desc("count")).limit(5)
            top_values = [str(r[col_name]) for r in top_df.collect()]
        except Exception:
            pass

        # Min/Max/Mean/Stddev for numeric-like columns
        min_val = max_val = mean_val = stddev_val = None
        outlier_count = 0

        try:
            # Try casting to double for numeric stats
            num_df = df.withColumn("__num", col(col_name).cast("double")).filter(col("__num").isNotNull())
            stats = num_df.agg(
                F.min("__num").alias("min_val"),
                F.max("__num").alias("max_val"),
                F.avg("__num").alias("mean_val"),
                F.stddev("__num").alias("stddev_val")
            ).collect()[0]
            min_val    = stats["min_val"]
            max_val    = stats["max_val"]
            mean_val   = round(stats["mean_val"], 2) if stats["mean_val"] else None
            stddev_val = round(stats["stddev_val"], 2) if stats["stddev_val"] else None

            # Outlier detection: values outside mean ± 3×stddev
            if mean_val is not None and stddev_val is not None and stddev_val > 0:
                lower = mean_val - 3 * stddev_val
                upper = mean_val + 3 * stddev_val
                outlier_count = num_df.filter(
                    (col("__num") < lower) | (col("__num") > upper)
                ).count()
                if outlier_count > 0:
                    anomalies.append(
                        f"Column '{col_name}': {outlier_count} outlier(s) detected "
                        f"(outside [{lower:.1f}, {upper:.1f}])"
                    )
        except Exception:
            pass

        profile_records.append({
            "run_id":          run_id,
            "source_file":     source_file,
            "column_name":     col_name,
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

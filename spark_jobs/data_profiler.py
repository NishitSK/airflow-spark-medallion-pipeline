"""
Data Profiler
=============
Generates per-run statistical profiles for each column in the Bronze dataset.
Detects anomalies by comparing against the previous run's profile.
Results are stored in the data_profile Delta table.
"""
import os
import pandas as pd
import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp
from pipeline.config import PROFILE_PATH
from pipeline.delta_utils import write_delta


def profile_dataframe(df: DataFrame, run_id: str, source_file: str, spark: SparkSession, mappings: list = None):
    """
    Compute statistical profile for each column using vectorized Pandas operations (small datasets)
    or lazy, distributed Spark aggregations (large datasets).
    """
    import time
    start_time = time.time()
    profile_records = []
    anomalies = []
    
    # Check if dataset is large based on raw file sizes in INPUT_PATH
    large_dataset = False
    try:
        from pipeline.config import INPUT_PATH
        if os.path.exists(INPUT_PATH):
            file_sizes = [os.path.getsize(os.path.join(INPUT_PATH, f)) for f in os.listdir(INPUT_PATH) if f.endswith(('.csv', '.json'))]
            total_bytes = sum(file_sizes)
            if total_bytes > 5 * 1024 * 1024:  # > 5 MB
                large_dataset = True
    except Exception as e:
        print(f"[Profiler] Warning estimating file sizes: {e}")

    if large_dataset:
        print(f"[Profiler] Large dataset detected. Performing lazy, distributed PySpark profiling...")
        columns_to_profile = [c for c in df.columns if c not in ("ingestion_time", "source_file")]
        
        # Single pass distributed aggregation
        agg_exprs = [F.count("*").alias("total_rows")]
        for c in columns_to_profile:
            is_null_expr = F.when(col(c).isNull() | (F.trim(col(c)) == "") | (col(c) == "None") | (col(c) == "nan") | (col(c) == "NaN"), 1).otherwise(0)
            agg_exprs.append(F.sum(is_null_expr).alias(f"{c}_nulls"))
            agg_exprs.append(F.countDistinct(c).alias(f"{c}_distinct"))
            agg_exprs.append(F.min(col(c)).alias(f"{c}_min"))
            agg_exprs.append(F.max(col(c)).alias(f"{c}_max"))
            agg_exprs.append(F.mean(col(c).cast("double")).alias(f"{c}_mean"))
            agg_exprs.append(F.stddev(col(c).cast("double")).alias(f"{c}_stddev"))

        metrics_row = df.select(*agg_exprs).collect()[0]
        total_rows = int(metrics_row["total_rows"] or 0)
        
        if total_rows == 0:
            return [], [], 0

        for c in columns_to_profile:
            nulls = int(metrics_row[f"{c}_nulls"] or 0)
            distinct = int(metrics_row[f"{c}_distinct"] or 0)
            min_val = metrics_row[f"{c}_min"]
            max_val = metrics_row[f"{c}_max"]
            mean_val = metrics_row[f"{c}_mean"]
            stddev_val = metrics_row[f"{c}_stddev"]
            
            null_pct = round(nulls / total_rows * 100, 2) if total_rows > 0 else 0.0
            dup_count = total_rows - distinct
            dup_pct = round(dup_count / total_rows * 100, 2) if total_rows > 0 else 0.0
            
            profile_records.append({
                "run_id":          run_id,
                "source_file":     source_file,
                "column_name":     c,
                "total_rows":      float(total_rows),
                "null_count":      float(nulls),
                "null_pct":        float(null_pct),
                "distinct_count":  float(distinct),
                "duplicate_count": float(dup_count),
                "duplicate_pct":   float(dup_pct),
                "min_value":       str(min_val) if min_val is not None else None,
                "max_value":       str(max_val) if max_val is not None else None,
                "mean_value":      str(round(mean_val, 2)) if mean_val is not None else None,
                "stddev_value":    str(round(stddev_val, 2)) if stddev_val is not None else None,
                "outlier_count":   0.0,
                "top_values":      "N/A",
            })
            
        anomalies += _detect_volume_anomalies(spark, total_rows, run_id)
        total_dur = time.time() - start_time
        print(f"[Profiler Timing] Lazy Spark profiling took: {total_dur:.2f} seconds")
        return profile_records, anomalies, total_rows

    # 1. Try to read directly from input path using Pandas to avoid JVM serialization overhead
    pdf = None
    topandas_dur = 0.0
    if mappings is not None:
        try:
            topandas_start = time.time()
            from pipeline.config import INPUT_PATH
            csv_files = [f for f in os.listdir(INPUT_PATH) if f.endswith('.csv')] if os.path.exists(INPUT_PATH) else []
            json_files = [f for f in os.listdir(INPUT_PATH) if f.endswith('.json')] if os.path.exists(INPUT_PATH) else []
            
            pdf_list = []
            for f in csv_files:
                pdf_list.append(pd.read_csv(os.path.join(INPUT_PATH, f), dtype=str))
            for f in json_files:
                pdf_list.append(pd.read_json(os.path.join(INPUT_PATH, f), lines=True, dtype=str))
                
            if len(pdf_list) > 1:
                pdf = pd.concat(pdf_list, ignore_index=True)
            elif len(pdf_list) == 1:
                pdf = pdf_list[0]
            else:
                pdf = pd.DataFrame()
                
            if not pdf.empty:
                rename_dict = {m["from_col"]: m["to_col"] for m in mappings}
                pdf.rename(columns=rename_dict, inplace=True)
                topandas_dur = time.time() - topandas_start
                print(f"[Profiler Timing] Pandas raw read took: {topandas_dur:.2f} seconds")
        except Exception as e:
            print(f"[Profiler] Warning: Direct Pandas load failed (falling back to toPandas): {e}")
            pdf = None

    if pdf is None:
        topandas_start = time.time()
        pdf = df.toPandas()
        topandas_dur = time.time() - topandas_start
        print(f"[Profiler Timing] df.toPandas() took: {topandas_dur:.2f} seconds")
    
    calc_start = time.time()
    total_rows = len(pdf)
    if total_rows == 0:
        return [], [], 0

    columns_to_profile = [c for c in pdf.columns if c not in ("ingestion_time", "source_file")]

    for c in columns_to_profile:
        series = pdf[c]
        
        # Spark checks col(c).isNull() | (trim(col(c)) == "")
        is_null = series.isna()
        if series.dtype == object:
            trimmed = series.astype(str).str.strip()
            is_null = is_null | (trimmed == "") | (trimmed == "None") | (trimmed == "nan") | (trimmed == "NaN")
        
        null_count = int(is_null.sum())
        null_pct = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0.0

        # Unique count distinct non-null
        valid_series = series[~is_null]
        distinct_count = int(valid_series.nunique())
        
        dup_count = total_rows - distinct_count
        dup_pct = round(dup_count / total_rows * 100, 2) if total_rows > 0 else 0.0

        # Numeric stats: cast to float
        num_series = pd.to_numeric(series, errors='coerce')
        num_series_clean = num_series.dropna()

        min_val = None
        max_val = None
        mean_val = None
        stddev_val = None
        outlier_count = 0

        if not num_series_clean.empty:
            min_val = num_series_clean.min()
            max_val = num_series_clean.max()
            mean_val = num_series_clean.mean()
            stddev_val = num_series_clean.std()
            
            # Outliers: mean +/- 3*stddev
            if stddev_val is not None and stddev_val > 0:
                lower = mean_val - 3 * stddev_val
                upper = mean_val + 3 * stddev_val
                outliers_mask = (num_series_clean < lower) | (num_series_clean > upper)
                outlier_count = int(outliers_mask.sum())
                if outlier_count > 0:
                    anomalies.append(
                        f"Column '{c}': {outlier_count} outlier(s) detected (outside [{lower:.1f}, {upper:.1f}])"
                    )

        # Top 5 values by count descending
        series_with_none = series.copy()
        if is_null.any():
            series_with_none[is_null] = "None"
            
        top_series = series_with_none.value_counts(dropna=False).head(5)
        top_values = [str(k) for k in top_series.index]

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
            "mean_value":      str(round(mean_val, 2)) if mean_val is not None else None,
            "stddev_value":    str(round(stddev_val, 2)) if stddev_val is not None else None,
            "outlier_count":   float(outlier_count),
            "top_values":      ", ".join(top_values),
        })

    calc_dur = time.time() - calc_start
    print(f"[Profiler Timing] Pandas calculations took: {calc_dur:.2f} seconds")

    # Volume anomaly detection vs previous run
    vol_start = time.time()
    anomalies += _detect_volume_anomalies(spark, total_rows, run_id)
    vol_dur = time.time() - vol_start
    print(f"[Profiler Timing] Volume anomaly detection took: {vol_dur:.2f} seconds")
    
    total_dur = time.time() - start_time
    print(f"[Profiler Timing] Total profiling function took: {total_dur:.2f} seconds")
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
    
    # Try reading from pipeline_history.jsonl first (extremely fast, avoids Spark jobs)
    try:
        from pipeline.config import HISTORY_FILE
        if os.path.exists(HISTORY_FILE):
            import json
            prev_rows = None
            with open(HISTORY_FILE, "r") as f:
                for line in reversed(f.readlines()):
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    if rec.get("status") == "completed" and rec.get("run_id") != run_id:
                        prev_rows = int(rec.get("rows", 0))
                        break
            if prev_rows is not None and prev_rows > 0:
                ratio = current_rows / prev_rows
                if ratio < 0.70:
                    anomalies.append(f"Volume DROP: {current_rows:,} rows vs previous {prev_rows:,} ({ratio:.0%})")
                elif ratio > 2.0:
                    anomalies.append(f"Volume SPIKE: {current_rows:,} rows vs previous {prev_rows:,} ({ratio:.0%})")
                print(f"[Profiler Volume Check] Read history from file: prev_rows = {prev_rows}")
                return anomalies
    except Exception as e:
        print(f"[Profiler Volume Check] History file read failed: {e}")

    # Fallback to Delta Spark read
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


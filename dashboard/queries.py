import os
import json
import time
import streamlit as st
import random
from pipeline.config import BRONZE_PATH, SILVER_PATH, GOLD_PATH, TRACE_PATH, DQ_METRICS_PATH, METRICS_FILE, STATUS_FILE, INPUT_PATH, ARCHIVE_PATH, SAMPLES_PATH, HISTORY_FILE
from pipeline.delta_utils import get_spark_session
import airflow_client


@st.cache_resource
def get_spark():
    return get_spark_session("Dashboard")

@st.cache_data(ttl=10)
def get_kpis(_spark):
    try:
        runtime_val = "0.00"
        if os.path.exists(METRICS_FILE):
            with open(METRICS_FILE) as f:
                parts = f.read().strip().split(",")
                if len(parts) >= 2: runtime_val = f"{float(parts[1]):.2f}"

        total_unique = _spark.read.format("delta").load(SILVER_PATH).count() if os.path.exists(SILVER_PATH) else 0
        return runtime_val, total_unique
    except:
        return "0.00", 0

def load_layer_data(spark, path):
    if os.path.exists(path):
        return spark.read.format("delta").load(path)
    return None

def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

def get_last_uploaded_file():
    files = []
    for folder in [INPUT_PATH, ARCHIVE_PATH]:
        if os.path.exists(folder):
            try:
                for f in os.listdir(folder):
                    if f.endswith(('.csv', '.json')):
                        fp = os.path.join(folder, f)
                        files.append((f, os.path.getmtime(fp)))
            except:
                pass
    if files:
        files.sort(key=lambda x: x[1], reverse=True)
        return files[0][0]
    return None

def get_consolidated_status(api_url=None, username=None, password=None):
    """
    Determines current status of pipeline using the orchestration client and filesystem state checks.
    Returns (status, last_run_timestamp, last_file, error_message, source_method, stage, duration)
    """
    has_input_file = False
    input_file_name = None
    if os.path.exists(INPUT_PATH):
        try:
            input_files = [f for f in os.listdir(INPUT_PATH) if f.endswith(('.csv', '.json'))]
            if input_files:
                has_input_file = True
                input_files.sort(key=lambda x: os.path.getmtime(os.path.join(INPUT_PATH, x)), reverse=True)
                input_file_name = input_files[0]
        except:
            pass

    last_file = get_last_uploaded_file() or "None"

    last_success_time = "N/A"
    t_success = 0.0
    if os.path.exists(METRICS_FILE):
        try:
            with open(METRICS_FILE) as f:
                parts = f.read().strip().split(",")
                if parts:
                    t_success = float(parts[0])
                    from datetime import datetime
                    last_success_time = datetime.fromtimestamp(t_success).strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass

    # Read local status file to retrieve details if available
    local_stage = "Waiting"
    local_duration = "N/A"
    local_status = None
    local_error = None
    local_file_name = None
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r") as f:
                state_data = json.load(f)
                local_status = state_data.get("status")
                local_stage = state_data.get("stage", "Waiting")
                local_duration = state_data.get("duration") or "N/A"
                local_error = state_data.get("error")
                local_file_name = state_data.get("file_name")
        except:
            pass

    # 1. Check Pipeline API
    try:
        latest_run, error = airflow_client.get_latest_run()
        if latest_run and not error:
            state = latest_run.get("state")
            run_date_str = latest_run.get("start_time")
            # Map status to app status
            if state in ["running", "queued"]:
                # If running, query local file for active stage if possible
                active_stage = local_stage if local_status == "running" else "Bronze"
                return "Pipeline running", last_success_time, last_file, None, f"Pipeline Service ({state})", active_stage, local_duration
            elif state == "failed":
                run_id = latest_run.get("run_id")
                error_msg = f"Latest pipeline run '{run_date_str}' failed."
                failed_stage = "Failed"
                try:
                    # Attempt to fetch task logs and extract exception
                    task_id, log_text = airflow_client.get_failed_task_log(run_id)
                    if log_text:
                        exc_type, exc_msg, stage_name = airflow_client.extract_exception_from_log(log_text)
                        if exc_type and exc_msg:
                            error_msg = f"Exception: {exc_type}: {exc_msg}"
                        if stage_name:
                            failed_stage = stage_name
                            error_msg += f" | Stage: {stage_name}"
                        else:
                            error_msg += f" | Stage: {task_id or 'Failed'}"
                except Exception as log_err:
                    print(f"Error extracting task log exception: {log_err}")
                return "Pipeline failed", last_success_time, last_file, error_msg, "Pipeline Service", failed_stage, local_duration
            elif state == "success":
                return "Pipeline completed", last_success_time, last_file, None, "Pipeline Service", "Finished", local_duration
    except Exception:
        pass

    # 2. Check local status file
    if local_status:
        file_name = local_file_name or last_file
        if local_status == "running":
            return "Pipeline running", last_success_time, file_name, None, "Local Status", local_stage, "N/A"
        elif local_status == "failed":
            error_msg = local_error or "Unknown local pipeline failure."
            if not error_msg.startswith("Exception: "):
                # If there's a type: msg in local error, convert it
                if ":" in error_msg and not error_msg.startswith("File "):
                    parts = error_msg.split(":", 1)
                    error_msg = f"Exception: {parts[0].strip()}: {parts[1].strip()}"
                else:
                    error_msg = f"Exception: PipelineError: {error_msg}"
            error_msg += f" | Stage: {local_stage or 'Failed'}"
            return "Pipeline failed", last_success_time, file_name, error_msg, "Local Status", local_stage or "Failed", local_duration
        elif local_status == "completed":
            return "Pipeline completed", last_success_time, file_name, None, "Local Status", "Finished", local_duration

    # 3. Infer from filesystem
    if last_success_time != "N/A":
        return "Pipeline completed", last_success_time, last_file, None, "Filesystem Inference", "Finished", local_duration
        
    return "Waiting for file", "N/A", last_file, None, "Filesystem Inference", "Waiting", "N/A"

def generate_sample_datasets():
    """
    Generates three sample datasets (Small: 100 rows, Medium: 10,000 rows, Large: 100,000 rows)
    in SAMPLES_PATH if they do not exist.
    """
    import pandas as pd
    
    os.makedirs(SAMPLES_PATH, exist_ok=True)
    
    small_path = os.path.join(SAMPLES_PATH, "small_sample.csv")
    medium_path = os.path.join(SAMPLES_PATH, "medium_sample.csv")
    large_path = os.path.join(SAMPLES_PATH, "large_sample.csv")
    small_json_path = os.path.join(SAMPLES_PATH, "small_sample.json")
    medium_json_path = os.path.join(SAMPLES_PATH, "medium_sample.json")
    
    # Names lists to make mock data look realistic
    first_names = ["John", "Jane", "Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Henry"]
    last_names = ["Smith", "Jones", "Miller", "Davis", "Garcia", "Rodriguez", "Wilson", "Thomas", "Taylor", "Anderson"]

    # Small: 100 rows (dirty data for testing DQ rules)
    if not os.path.exists(small_path):
        ids = list(range(1001, 1101))
        names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in ids]
        ages = [random.randint(18, 75) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        # Use nullable integer to prevent cast to float due to None
        df["id"] = df["id"].astype("Int64")
        df["age"] = df["age"].astype("Int64")
        # Inject DQ violation samples
        df.loc[0, "id"] = pd.NA        # Null ID
        df.loc[1, "age"] = -5        # Negative Age
        df.loc[2, "age"] = 150       # Out-of-bounds Age
        df.loc[3, "id"] = 1005       # Duplicate ID
        df.to_csv(small_path, index=False)
        print("Generated small_sample.csv")
        
    # Small JSON (100 rows, newline-delimited JSON)
    if not os.path.exists(small_json_path):
        ids = list(range(1101, 1201))
        names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in ids]
        ages = [random.randint(18, 75) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        df["id"] = df["id"].astype("Int64")
        df["age"] = df["age"].astype("Int64")
        # Inject DQ violation samples
        df.loc[0, "id"] = pd.NA
        df.loc[1, "age"] = -3
        df.to_json(small_json_path, orient="records", lines=True)
        print("Generated small_sample.json")
        
    # Medium: 10,000 rows (semi-clean, test spark scale)
    if not os.path.exists(medium_path):
        ids = list(range(2001, 12001))
        names = [f"User_{i}" for i in ids]
        ages = [random.randint(10, 95) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        df["id"] = df["id"].astype("Int64")
        df["age"] = df["age"].astype("Int64")
        # Inject mild DQ issues
        df.loc[0, "id"] = pd.NA
        df.loc[1, "age"] = -1
        df.to_csv(medium_path, index=False)
        print("Generated medium_sample.csv")
        
    # Medium JSON (10,000 rows, newline-delimited JSON)
    if not os.path.exists(medium_json_path):
        ids = list(range(12001, 22001))
        names = [f"User_{i}" for i in ids]
        ages = [random.randint(10, 95) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        df["id"] = df["id"].astype("Int64")
        df["age"] = df["age"].astype("Int64")
        # Inject DQ violation samples
        df.loc[0, "id"] = pd.NA
        df.loc[1, "age"] = -2
        df.to_json(medium_json_path, orient="records", lines=True)
        print("Generated medium_sample.json")
        
    # Large: 100,000 rows (clean data to show PySpark/Delta performance)
    if not os.path.exists(large_path):
        ids = list(range(12001, 112001))
        names = [f"User_{i}" for i in ids]
        ages = [random.randint(18, 70) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        df["id"] = df["id"].astype("Int64")
        df["age"] = df["age"].astype("Int64")
        df.to_csv(large_path, index=False)
        print("Generated large_sample.csv")


def get_file_metadata(filepath):
    """
    Returns (size_mb, row_count, col_count) for a CSV or JSON file.
    """
    if not os.path.exists(filepath):
        return 0.0, 0, 0
    try:
        size_bytes = os.path.getsize(filepath)
        size_mb = size_bytes / (1024 * 1024)
        
        row_count = 0
        col_count = 0
        
        # Count lines
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for _ in f:
                row_count += 1
                
        # Parse preview to get column count
        import pandas as pd
        if filepath.endswith('.csv'):
            if row_count > 0:
                row_count -= 1
            # Read header
            try:
                df = pd.read_csv(filepath, nrows=1)
                col_count = len(df.columns)
            except:
                pass
        elif filepath.endswith('.json'):
            try:
                df = pd.read_json(filepath, lines=True, nrows=1)
                col_count = len(df.columns)
            except:
                pass
                
        return size_mb, row_count, col_count
    except:
        return 0.0, 0, 0

def get_file_preview(filepath):
    """
    Safely reads first 10 rows of a CSV or JSON file and returns a Pandas DataFrame.
    """
    import pandas as pd
    if not os.path.exists(filepath):
        return None
    try:
        if filepath.endswith('.csv'):
            return pd.read_csv(filepath, nrows=10)
        elif filepath.endswith('.json'):
            return pd.read_json(filepath, lines=True, nrows=10)
    except:
        pass
    return None

@st.cache_data(ttl=10)
def get_pipeline_history(_spark):
    """
    Retrieves the latest 20 pipeline run records.
    Parses HISTORY_FILE (JSONL), falling back to Silver table Delta log history if empty.
    """
    import pandas as pd
    runs = []
    
    # 1. Try reading from persistent HISTORY_FILE
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                for line in f:
                    if line.strip():
                        runs.append(json.loads(line))
        except:
            pass
            
    # 2. If no logs yet, fallback to querying Delta Lake log history
    if not runs and _spark is not None:
        try:
            from delta.tables import DeltaTable
            if os.path.exists(SILVER_PATH):
                dt_silver = DeltaTable.forPath(_spark, SILVER_PATH)
                history_df = dt_silver.history().select("timestamp", "version", "operation", "operationMetrics").toPandas()
                history_df.sort_values("version", ascending=False, inplace=True)
                
                for idx, row in history_df.iterrows():
                    metrics = row["operationMetrics"] if row["operationMetrics"] else {}
                    rows = int(metrics.get('numOutputRows', 0)) if metrics.get('numOutputRows') else 0
                    
                    runs.append({
                        "timestamp": row["timestamp"].timestamp() if pd.notnull(row["timestamp"]) else time.time(),
                        "run_id": f"delta_v{row['version']}",
                        "file_name": f"Delta Transaction (v{row['version']})",
                        "status": "completed",
                        "duration": "N/A",
                        "rows": rows,
                        "error": None
                    })
        except:
            pass
            
    runs.sort(key=lambda x: x.get("timestamp", 0.0), reverse=True)
    return runs[:20]

def get_dq_audit_details(spark):
    """
    Analyzes Bronze table to find malformed records and duplicate key samples.
    """
    from pyspark.sql.functions import col, regexp_replace, trim, count
    import pandas as pd
    
    audit_data = {
        "malformed_ids": {"count": 0, "samples": []},
        "malformed_ages": {"count": 0, "samples": []},
        "duplicate_ids": {"count": 0, "samples": []}
    }
    
    try:
        if not os.path.exists(BRONZE_PATH):
            return audit_data
            
        df = spark.read.format("delta").load(BRONZE_PATH)
        total_rows = df.count()
        if total_rows == 0:
            return audit_data
            
        # 1. Malformed IDs
        cleaned_id = regexp_replace(trim(col("id")), r"\.0+$", "")
        parsed_id = cleaned_id.cast("int")
        is_malformed_id = parsed_id.isNull() & col("id").isNotNull() & (trim(col("id")) != "")
        
        malformed_ids_df = df.filter(is_malformed_id)
        audit_data["malformed_ids"]["count"] = malformed_ids_df.count()
        if audit_data["malformed_ids"]["count"] > 0:
            samples = malformed_ids_df.select("id", "name").limit(5).collect()
            audit_data["malformed_ids"]["samples"] = [{"id": r["id"], "name": r["name"]} for r in samples]
            
        # 2. Malformed Ages
        cleaned_age = regexp_replace(trim(col("age")), r"\.0+$", "")
        parsed_age = cleaned_age.cast("int")
        is_malformed_age = parsed_age.isNull() & col("age").isNotNull() & (trim(col("age")) != "")
        
        malformed_ages_df = df.filter(is_malformed_age)
        audit_data["malformed_ages"]["count"] = malformed_ages_df.count()
        if audit_data["malformed_ages"]["count"] > 0:
            samples = malformed_ages_df.select("age", "name").limit(5).collect()
            audit_data["malformed_ages"]["samples"] = [{"age": r["age"], "name": r["name"]} for r in samples]
            
        # 3. Duplicate IDs (duplicates on normalized ID)
        normalized_df = df.withColumn("normalized_id", regexp_replace(trim(col("id")), r"\.0+$", ""))
        # Filter keys with count > 1
        dup_keys_df = normalized_df.groupBy("normalized_id").agg(count("*").alias("occurrences")).filter("occurrences > 1")
        audit_data["duplicate_ids"]["count"] = dup_keys_df.count()
        if audit_data["duplicate_ids"]["count"] > 0:
            # Join back to get names and raw ids for duplicate samples
            sample_dupes = normalized_df.join(dup_keys_df, "normalized_id").select("id", "name", "occurrences").limit(5).collect()
            audit_data["duplicate_ids"]["samples"] = [{"raw_id": r["id"], "name": r["name"], "occurrences": r["occurrences"]} for r in sample_dupes]
            
    except Exception as e:
        print(f"Error fetching DQ audit details: {str(e)}")
        
    return audit_data

@st.cache_data(ttl=10)
def get_dq_trends(_spark):
    """
    Retrieves all history validation metrics from DQ_METRICS_PATH.
    """
    from pyspark.sql.functions import col
    import pandas as pd
    import numpy as np
    
    try:
        if os.path.exists(DQ_METRICS_PATH) and _spark is not None:
            df = _spark.read.format("delta").load(DQ_METRICS_PATH).orderBy(col("validation_time").asc()).toPandas()
            if not df.empty:
                # Replace 0 total_rows with NaN to prevent divide-by-zero
                total = df['total_rows'].replace(0, np.nan)
                
                # Metrics percentage calculations
                df['Null Rate (%)'] = (df['null_ids'] / total * 100).fillna(0.0)
                df['Duplicate Rate (%)'] = (df['duplicate_ids'] / total * 100).fillna(0.0)
                df['Invalid Age Rate (%)'] = (df['invalid_ages'] / total * 100).fillna(0.0)
                
                # Quality Score = (Passed Rows / Total Rows) * 100
                # Passed Rows = Total - (nulls + invalid age + duplicates)
                failed_records = df['null_ids'] + df['invalid_ages'] + df['duplicate_ids']
                passed_records = (df['total_rows'] - failed_records).clip(lower=0)
                df['Quality Score (%)'] = (passed_records / total * 100).fillna(100.0)
                df['Failure Rate (%)'] = (failed_records / total * 100).fillna(0.0)
                
                # Formatting validation time
                df['Timestamp'] = pd.to_datetime(df['validation_time']).dt.strftime('%m-%d %H:%M:%S')
                return df
    except Exception as e:
        print(f"Error fetching DQ trends: {str(e)}")
    return pd.DataFrame()

@st.cache_data(ttl=10)
def get_incidents(_spark):
    """
    Retrieves logged incidents and execution traces from TRACE_PATH.
    """
    from pyspark.sql.functions import col
    import pandas as pd
    
    try:
        if os.path.exists(TRACE_PATH) and _spark is not None:
            df = _spark.read.format("delta").load(TRACE_PATH)
            incidents_df = df.orderBy(col("timestamp").desc()).limit(20).toPandas()
            return incidents_df
    except Exception as e:
        print(f"Error fetching incidents: {str(e)}")
    return pd.DataFrame()

@st.cache_data(ttl=10)
def get_last_run_summary(_spark):
    """
    Fetches details of the last completed or failed run for the summary card.
    Returns a dict with status, start_time, end_time, duration, rows_processed, dq_score.
    """
    import os
    import json
    import pandas as pd
    from pipeline.config import HISTORY_FILE, DQ_METRICS_PATH
    
    summary = {
        "status": "N/A",
        "start_time": "N/A",
        "end_time": "N/A",
        "duration": "N/A",
        "rows": "0",
        "dq_score": "N/A"
    }
    
    # 1. Fetch latest record from history
    last_record = None
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                lines = f.readlines()
                if lines:
                    last_record = json.loads(lines[-1].strip())
        except:
            pass
            
    if last_record:
        raw_status = last_record.get("status", "N/A")
        if raw_status == "completed":
            summary["status"] = "Success"
        elif raw_status == "failed":
            summary["status"] = "Failed"
        else:
            summary["status"] = raw_status.capitalize()
            
        duration_val = last_record.get("duration", "N/A")
        if isinstance(duration_val, (int, float)):
            summary["duration"] = f"{duration_val:.2f}s"
        else:
            try:
                summary["duration"] = f"{float(duration_val):.2f}s"
            except:
                summary["duration"] = str(duration_val) + ("s" if duration_val != "N/A" else "")
            
        rows_val = last_record.get("rows", 0)
        summary["rows"] = f"{rows_val:,}" if isinstance(rows_val, (int, float)) else str(rows_val)
        
        # Estimate start and end times
        timestamp = last_record.get("timestamp")
        if timestamp:
            from datetime import datetime
            dt_end = datetime.fromtimestamp(timestamp)
            summary["end_time"] = dt_end.strftime("%Y-%m-%d %H:%M:%S")
            
            try:
                dur_float = float(duration_val)
                dt_start = datetime.fromtimestamp(timestamp - dur_float)
                summary["start_time"] = dt_start.strftime("%Y-%m-%d %H:%M:%S")
            except:
                summary["start_time"] = summary["end_time"]
                
    # 2. Fetch last DQ Score
    try:
        if os.path.exists(DQ_METRICS_PATH) and _spark is not None:
            from pyspark.sql.functions import col
            dq_df = _spark.read.format("delta").load(DQ_METRICS_PATH).orderBy(col("validation_time").desc()).limit(1).toPandas()
            if not dq_df.empty:
                row = dq_df.iloc[0]
                total_val = int(row["total_rows"])
                failed = int(row["null_ids"]) + int(row["invalid_ages"]) + int(row["duplicate_ids"])
                passed = max(0, total_val - failed)
                dq_score = (passed / total_val * 100) if total_val > 0 else 100.0
                summary["dq_score"] = f"{dq_score:.1f}%"
    except Exception as e:
        print(f"Error fetching last DQ score for summary: {e}")
        
    return summary




import os
import json
import streamlit as st
import random
from pipeline.config import BRONZE_PATH, SILVER_PATH, GOLD_PATH, TRACE_PATH, DQ_METRICS_PATH, METRICS_FILE, STATUS_FILE, INPUT_PATH, ARCHIVE_PATH, SAMPLES_PATH
from pipeline.delta_utils import get_spark_session
from airflow_client import check_latest_dag_status

@st.cache_resource
def get_spark():
    return get_spark_session("Dashboard")

def get_kpis(spark):
    try:
        runtime_val = "0.00"
        if os.path.exists(METRICS_FILE):
            with open(METRICS_FILE) as f:
                parts = f.read().strip().split(",")
                if len(parts) >= 2: runtime_val = f"{float(parts[1]):.2f}"

        total_unique = spark.read.format("delta").load(SILVER_PATH).count() if os.path.exists(SILVER_PATH) else 0
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
    Determines current status of pipeline using Airflow API and filesystem state checks.
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

    # 1. Check Airflow API if online
    if api_url:
        state, run_date_str = check_latest_dag_status(api_url, username, password)
        if state is not None:
            # Map Airflow status to app status
            if has_input_file and state in ["success", "failed"]:
                return "Pipeline running", last_success_time, last_file, None, "Airflow API (File Ingesting)", "Bronze", "N/A"
            
            if state in ["running", "queued"]:
                # If running, query local file for active stage if possible
                active_stage = local_stage if local_status == "running" else "Bronze"
                return "Pipeline running", last_success_time, last_file, None, f"Airflow API ({state})", active_stage, local_duration
            elif state == "failed":
                return "Pipeline failed", last_success_time, last_file, f"Latest Airflow DAG run '{run_date_str}' failed.", "Airflow API", "Failed", local_duration
            elif state == "success":
                # If no files in input, we are resting
                if not has_input_file:
                    return "Pipeline completed", last_success_time, last_file, None, "Airflow API", "Finished", local_duration
                return "Pipeline running", last_success_time, last_file, None, "Airflow API (Active)", "Bronze", "N/A"

    # 2. Check local status file
    if local_status:
        file_name = local_file_name or last_file
        if has_input_file:
            return "Pipeline running", last_success_time, input_file_name or file_name, None, "Local Status (New File Ingesting)", "Bronze", "N/A"
        
        if local_status == "running":
            return "Pipeline running", last_success_time, file_name, None, "Local Status", local_stage, "N/A"
        elif local_status == "failed":
            return "Pipeline failed", last_success_time, file_name, local_error, "Local Status", "Failed", local_duration
        elif local_status == "completed":
            return "Pipeline completed", last_success_time, file_name, None, "Local Status", "Finished", local_duration

    # 3. Infer from filesystem
    if has_input_file:
        return "Pipeline running", last_success_time, input_file_name or last_file, None, "Filesystem Inference (Input File)", "Bronze", "N/A"
    
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
        # Inject DQ violation samples
        df.loc[0, "id"] = None       # Null ID
        df.loc[1, "age"] = -5        # Negative Age
        df.loc[2, "age"] = 150       # Out-of-bounds Age
        df.loc[3, "id"] = 1005       # Duplicate ID (ID 1005 already exists elsewhere)
        df.to_csv(small_path, index=False)
        print("Generated small_sample.csv")
        
    # Small JSON (100 rows, newline-delimited JSON)
    if not os.path.exists(small_json_path):
        ids = list(range(1101, 1201))
        names = [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in ids]
        ages = [random.randint(18, 75) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        # Inject DQ violation samples
        df.loc[0, "id"] = None
        df.loc[1, "age"] = -3
        df.to_json(small_json_path, orient="records", lines=True)
        print("Generated small_sample.json")
        
    # Medium: 10,000 rows (semi-clean, test spark scale)
    if not os.path.exists(medium_path):
        ids = list(range(2001, 12001))
        names = [f"User_{i}" for i in ids]
        ages = [random.randint(10, 95) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        # Inject mild DQ issues
        df.loc[0, "id"] = None
        df.loc[1, "age"] = -1
        df.to_csv(medium_path, index=False)
        print("Generated medium_sample.csv")
        
    # Medium JSON (10,000 rows, newline-delimited JSON)
    if not os.path.exists(medium_json_path):
        ids = list(range(12001, 22001))
        names = [f"User_{i}" for i in ids]
        ages = [random.randint(10, 95) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        # Inject DQ violation samples
        df.loc[0, "id"] = None
        df.loc[1, "age"] = -2
        df.to_json(medium_json_path, orient="records", lines=True)
        print("Generated medium_sample.json")
        
    # Large: 100,000 rows (clean data to show PySpark/Delta performance)
    if not os.path.exists(large_path):
        ids = list(range(12001, 112001))
        names = [f"User_{i}" for i in ids]
        ages = [random.randint(18, 70) for _ in ids]
        df = pd.DataFrame({"id": ids, "name": names, "age": ages})
        df.to_csv(large_path, index=False)
        print("Generated large_sample.csv")


def get_file_metadata(filepath):
    """
    Returns (size_mb, row_count) for a dataset file.
    """
    if not os.path.exists(filepath):
        return 0.0, 0
    try:
        size_bytes = os.path.getsize(filepath)
        size_mb = size_bytes / (1024 * 1024)
        
        row_count = 0
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            for _ in f:
                row_count += 1
        
        # Subtract 1 for CSV header
        if filepath.endswith('.csv') and row_count > 0:
            row_count -= 1
        return size_mb, row_count
    except:
        return 0.0, 0



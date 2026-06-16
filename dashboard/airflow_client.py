import os
import requests
import urllib3
import logging

# Suppress insecure connection warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize operational logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
)
log = logging.getLogger("airflow_client")

# Airflow configurations read directly from environment variables
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://localhost:8080").rstrip('/')
AIRFLOW_USER = os.getenv("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin123")

_last_status = None

def _get_auth_headers():
    """
    Acquires JWT token using credentials from environment and returns auth headers.
    No internal/global caching to ensure clean state management.
    """
    url = f"{AIRFLOW_API_URL}/auth/token"
    try:
        response = requests.post(
            url,
            json={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD},
            timeout=3,
            verify=False
        )
        if response.status_code in [200, 201]:
            token = response.json().get("access_token")
            if token:
                return {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }, None
        return None, f"Failed to acquire token: HTTP {response.status_code}"
    except Exception as e:
        return None, f"Airflow service unreachable: {str(e)}"

def is_airflow_healthy() -> bool:
    """
    Checks the public health endpoint of Airflow. Does not depend on token acquisition.
    """
    try:
        # Check Airflow 3 monitor health endpoint
        response = requests.get(f"{AIRFLOW_API_URL}/api/v2/monitor/health", timeout=3, verify=False)
        if response.status_code == 200:
            return True
    except:
        pass
    try:
        # Fallback check on old /health
        response = requests.get(f"{AIRFLOW_API_URL}/health", timeout=3, verify=False)
        if response.status_code == 200:
            return True
    except:
        pass
    return False

def get_airflow_health() -> str:
    """
    Checks Airflow health and returns "AVAILABLE" or "UNAVAILABLE".
    """
    if is_airflow_healthy():
        return "AVAILABLE"
    return "UNAVAILABLE"

def trigger_pipeline(dag_id="file_trigger_pipeline") -> tuple[bool, str]:
    """
    Triggers a run of the pipeline DAG.
    """
    log.info("Pipeline trigger requested")
    
    if not is_airflow_healthy():
        log.warning("Pipeline trigger rejected: Service unavailable")
        return False, "Pipeline service unavailable."
        
    headers, error = _get_auth_headers()
    if error:
        log.warning(f"Pipeline trigger rejected: {error}")
        return False, "Airflow service unreachable."
        
    url = f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/dagRuns"
    try:
        import datetime
        payload = {"logical_date": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        response = requests.post(url, headers=headers, json=payload, timeout=3, verify=False)
        if response.status_code in [200, 201]:
            log.info("Pipeline trigger accepted")
            return True, "Pipeline trigger accepted."
            
        log.warning(f"Pipeline trigger rejected: HTTP {response.status_code}")
        return False, "Unable to trigger pipeline."
    except Exception as e:
        log.warning(f"Pipeline trigger rejected: {str(e)}")
        return False, "Airflow service unreachable."

def get_latest_run(dag_id="file_trigger_pipeline") -> tuple[dict | None, str | None]:
    """
    Retrieves details of the latest DAG run.
    """
    headers, error = _get_auth_headers()
    if error:
        return None, "Airflow service unreachable."
        
    url = f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(url, headers=headers, timeout=3, verify=False)
        if response.status_code == 200:
            dag_runs = response.json().get("dag_runs", [])
            if not dag_runs:
                return None, None
            dag_runs.sort(key=lambda x: x.get("start_date") or x.get("logical_date") or "", reverse=True)
            latest = dag_runs[0]
            
            # calculate duration
            start_date_str = latest.get("start_date")
            end_date_str = latest.get("end_date")
            duration_sec = None
            if start_date_str and end_date_str:
                try:
                    import pandas as pd
                    start_dt = pd.to_datetime(start_date_str)
                    end_dt = pd.to_datetime(end_date_str)
                    duration_sec = (end_dt - start_dt).total_seconds()
                except:
                    pass
            
            try:
                import pandas as pd
                start_fmt = pd.to_datetime(start_date_str).strftime("%Y-%m-%d %H:%M:%S") if start_date_str else "N/A"
                end_fmt = pd.to_datetime(end_date_str).strftime("%Y-%m-%d %H:%M:%S") if end_date_str else "N/A"
            except:
                start_fmt = start_date_str or "N/A"
                end_fmt = end_date_str or "N/A"
                
            details = {
                "run_id": latest.get("run_id") or latest.get("dag_run_id") or "N/A",
                "state": latest.get("state") or "N/A",
                "start_time": start_fmt,
                "end_time": end_fmt,
                "duration": f"{duration_sec:.2f}s" if duration_sec is not None else "N/A"
            }
            return details, None
        return None, f"Failed to get runs: HTTP {response.status_code}"
    except Exception as e:
        return None, f"Airflow service unreachable: {str(e)}"

def get_dag_health(dag_id="file_trigger_pipeline") -> bool:
    """
    Checks DAG presence and status.
    """
    headers, error = _get_auth_headers()
    if error:
        return False
        
    url = f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}"
    try:
        response = requests.get(url, headers=headers, timeout=3, verify=False)
        if response.status_code == 200:
            is_paused = response.json().get("is_paused", False)
            return not is_paused
        return False
    except:
        return False

def get_pipeline_status(dag_id="file_trigger_pipeline") -> str:
    """
    Checks state of the pipeline.
    """
    global _last_status
    details, error = get_latest_run(dag_id)
    if error:
        return "UNAVAILABLE"
    if not details:
        return "READY"
        
    state = details.get("state", "").upper()
    if state == "RUNNING":
        status = "RUNNING"
    elif state == "QUEUED":
        status = "QUEUED"
    elif state in ["SUCCESS", "FAILED"]:
        status = "READY"
    else:
        status = "READY"
        
    # Log state transitions
    if state != _last_status:
        if state == "SUCCESS":
            log.info("Pipeline completed")
        elif state == "FAILED":
            log.info("Pipeline failed")
        _last_status = state
        
    return status

def get_run_counts(dag_id="file_trigger_pipeline") -> dict[str, int]:
    """
    Counts pipeline runs.
    """
    counts = {"queued": 0, "running": 0, "success": 0, "failed": 0}
    headers, error = _get_auth_headers()
    if error:
        return counts
        
    url = f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(url, headers=headers, timeout=3, verify=False)
        if response.status_code == 200:
            runs = response.json().get("dag_runs", [])
            for run in runs:
                state_val = run.get("state")
                if state_val in counts:
                    counts[state_val] += 1
    except:
        pass
    return counts

def get_failed_task_log(dag_run_id, dag_id="file_trigger_pipeline") -> tuple[str | None, str | None]:
    """
    Finds the failed task in the given DAG run, fetches its logs, and returns (task_id, logs_text).
    """
    headers, error = _get_auth_headers()
    if error:
        return None, None
        
    # 1. Get task instances for the DAG run
    url = f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    try:
        response = requests.get(url, headers=headers, timeout=3, verify=False)
        if response.status_code == 200:
            tis = response.json().get("task_instances", [])
            failed_ti = None
            for ti in tis:
                if ti.get("state") == "failed":
                    failed_ti = ti
                    break
            
            # If no failed task found, default to unified_medallion_pipeline
            task_id = failed_ti.get("task_id") if failed_ti else "unified_medallion_pipeline"
            try_number = failed_ti.get("try_number") if failed_ti else 1
            if not try_number or try_number < 1:
                try_number = 1
                
            # 2. Get the log for the task instance
            log_url = f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
            log_response = requests.get(log_url, headers=headers, timeout=3, verify=False)
            if log_response.status_code == 200:
                return task_id, log_response.text
    except Exception as e:
        log.error(f"Error fetching logs: {str(e)}")
    return None, None

def extract_exception_from_log(log_text: str) -> tuple[str | None, str | None, str | None]:
    """
    Parses Airflow task log text to extract:
    (exception_type, exception_message, failed_stage)
    """
    if not log_text:
        return None, None, None

    # Scan for medallions stage markers in log lines
    failed_stage = None
    if "[Layer 3: Gold]" in log_text:
        failed_stage = "Gold"
    elif "[Layer 2: Silver]" in log_text:
        failed_stage = "Silver"
    elif "[Layer 1.5: Data Quality Validation]" in log_text or "DQ Validation" in log_text:
        failed_stage = "Validation"
    elif "[Layer 1: Bronze]" in log_text:
        failed_stage = "Bronze"
    elif "archive" in log_text.lower():
        failed_stage = "Archive"

    # Search for python traceback
    tb_marker = "Traceback (most recent call last):"
    idx = log_text.rfind(tb_marker)
    if idx != -1:
        tb_lines = log_text[idx:].splitlines()
        tb_lines = [line.strip() for line in tb_lines if line.strip()]
        
        # Traverse backwards to find the exception type and message
        for i in range(len(tb_lines) - 1, -1, -1):
            line = tb_lines[i]
            if ":" in line and not line.startswith("File ") and not line.startswith("Traceback"):
                # Clean trace logs line markers like "standalone | [..."
                if " | " in line:
                    line = line.split(" | ", 1)[1].strip()
                parts = line.split(":", 1)
                exc_type = parts[0].strip()
                exc_msg = parts[1].strip()
                return exc_type, exc_msg, failed_stage
        
        # Fallback to last line of traceback
        if len(tb_lines) > 0:
            last_line = tb_lines[-1]
            if ":" in last_line:
                parts = last_line.split(":", 1)
                return parts[0].strip(), parts[1].strip(), failed_stage
            return "Exception", last_line, failed_stage
            
    # If no traceback found, search for generic failed exits or logs
    for line in reversed(log_text.splitlines()):
        line = line.strip()
        if "error" in line.lower() or "failed" in line.lower() or "exception" in line.lower():
            # Clean standard container logging prefix if present
            if " | " in line:
                line = line.split(" | ", 1)[1].strip()
            if ":" in line:
                parts = line.split(":", 1)
                return "Error", parts[1].strip(), failed_stage
            return "Error", line, failed_stage

    return None, None, failed_stage


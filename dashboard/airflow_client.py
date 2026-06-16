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

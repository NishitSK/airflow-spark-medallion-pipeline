import os
import requests
import urllib3
import logging
from pipeline.orchestrator_config import AIRFLOW_API_URL, AIRFLOW_USER, AIRFLOW_PASSWORD

# Suppress insecure connection warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Initialize operational logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
)
log = logging.getLogger("orchestrator_client")

# Internal JWT token cache
_cached_token = None
_last_status = None

def _get_token_internal():
    global _cached_token
    if _cached_token:
        return _cached_token, None
        
    api_url = AIRFLOW_API_URL.rstrip('/')
    
    # 1. Try passwordless GET /auth/token
    try:
        response = requests.get(f"{api_url}/auth/token", timeout=3, verify=False)
        if response.status_code in [200, 201]:
            payload = response.json()
            token = payload.get("access_token") or payload.get("token") or payload.get("jwt")
            if token:
                _cached_token = token
                log.info("Connected to orchestration service")
                return token, None
    except Exception:
        pass
        
    # 2. Fallback to POST /auth/token using env credentials
    try:
        response = requests.post(
            f"{api_url}/auth/token",
            json={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD},
            headers={"Content-Type": "application/json"},
            timeout=3,
            verify=False
        )
        if response.status_code in [200, 201]:
            payload = response.json()
            token = payload.get("access_token") or payload.get("token") or payload.get("jwt")
            if token:
                _cached_token = token
                log.info("Connected to orchestration service")
                return token, None
            return None, "No token returned."
        return None, f"Auth failed (HTTP {response.status_code})"
    except Exception as e:
        return None, f"Service unreachable: {str(e)}"

def _get_auth_headers():
    token, err = _get_token_internal()
    if token:
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }, None
    return None, err

def _clear_token():
    global _cached_token
    _cached_token = None


# --- Public Orchestration API ---

def trigger_pipeline(dag_id="file_trigger_pipeline") -> tuple[bool, str]:
    """
    Triggers a run of the pipeline DAG after performing health checks.
    """
    log.info("Pipeline trigger requested")
    
    if not get_dag_health(dag_id):
        log.warning("Pipeline trigger rejected: Service unavailable")
        return False, "Pipeline service unavailable."
        
    headers, error = _get_auth_headers()
    if error:
        log.warning("Pipeline trigger rejected: Service unreachable")
        return False, "Orchestration service unreachable."
        
    api_url = AIRFLOW_API_URL.rstrip('/')
    url = f"{api_url}/api/v2/dags/{dag_id}/dagRuns"
    try:
        import datetime
        payload = {"logical_date": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        response = requests.post(url, headers=headers, json=payload, timeout=3, verify=False)
        if response.status_code in [401, 403]:
            _clear_token()
            headers, error = _get_auth_headers()
            if not error:
                response = requests.post(url, headers=headers, json=payload, timeout=3, verify=False)
                
        if response.status_code in [200, 201]:
            log.info("Pipeline trigger accepted")
            return True, "Pipeline trigger accepted."
            
        log.warning(f"Pipeline trigger rejected: HTTP {response.status_code}")
        return False, "Unable to trigger pipeline."
    except Exception:
        log.warning("Pipeline trigger rejected: Network exception")
        return False, "Orchestration service unreachable."

def get_latest_run(dag_id="file_trigger_pipeline") -> tuple[dict | None, str | None]:
    """
    Retrieves details of the latest DAG run.
    """
    headers, error = _get_auth_headers()
    if error:
        return None, "Orchestration service unreachable."
        
    api_url = AIRFLOW_API_URL.rstrip('/')
    url = f"{api_url}/api/v2/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(url, headers=headers, timeout=3, verify=False)
        if response.status_code in [401, 403]:
            _clear_token()
            headers, error = _get_auth_headers()
            if not error:
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
        return None, "Orchestration service unreachable."
    except Exception:
        return None, "Orchestration service unreachable."

def get_dag_health(dag_id="file_trigger_pipeline") -> bool:
    """
    Checks DAG presence and status.
    """
    headers, error = _get_auth_headers()
    if error:
        return False
        
    api_url = AIRFLOW_API_URL.rstrip('/')
    url = f"{api_url}/api/v2/dags/{dag_id}"
    try:
        response = requests.get(url, headers=headers, timeout=3, verify=False)
        if response.status_code in [401, 403]:
            _clear_token()
            headers, error = _get_auth_headers()
            if not error:
                response = requests.get(url, headers=headers, timeout=3, verify=False)
                
        if response.status_code == 200:
            is_paused = response.json().get("is_paused", False)
            return not is_paused
        return False
    except Exception:
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

def get_orchestrator_health(dag_id="file_trigger_pipeline") -> str:
    """
    Returns general orchestrator status.
    """
    if not get_dag_health(dag_id):
        return "UNAVAILABLE"
    return get_pipeline_status(dag_id)

def get_run_counts(dag_id="file_trigger_pipeline") -> dict[str, int]:
    """
    Counts pipeline runs internally.
    """
    counts = {"queued": 0, "running": 0, "success": 0, "failed": 0}
    headers, error = _get_auth_headers()
    if error:
        return counts
        
    api_url = AIRFLOW_API_URL.rstrip('/')
    url = f"{api_url}/api/v2/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(url, headers=headers, timeout=3, verify=False)
        if response.status_code in [401, 403]:
            _clear_token()
            headers, error = _get_auth_headers()
            if not error:
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

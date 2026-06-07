import requests
import base64
import urllib3

# Suppress insecure connection warnings if users use self-signed SSL certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_basic_auth_headers(username, password):
    auth_str = f"{username}:{password}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()
    return {
        "Authorization": f"Basic {b64_auth}",
        "Content-Type": "application/json"
    }

def get_dag_runs(api_url="http://airflow:8080", username="admin", password="admin", dag_id="file_trigger_pipeline"):
    """
    Fetches DAG runs from Airflow REST API. 
    First tries Airflow 3.x endpoint (/api/v2) and falls back to Airflow 2.x (/api/v1).
    """
    headers = get_basic_auth_headers(username, password)
    
    # Airflow 3.x endpoint
    v2_url = f"{api_url.rstrip('/')}/api/v2/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(v2_url, headers=headers, timeout=3, verify=False)
        if response.status_code == 200:
            return response.json().get("dag_runs", []), None
        elif response.status_code == 404:
            # Maybe it is Airflow 2.x, try v1
            v1_url = f"{api_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
            response = requests.get(v1_url, headers=headers, timeout=3, verify=False)
            if response.status_code == 200:
                return response.json().get("dag_runs", []), None
            return None, f"Airflow API check failed (HTTP {response.status_code})."
        else:
            return None, f"Airflow API check failed (HTTP {response.status_code})."
    except requests.exceptions.RequestException:
        # Fallback to local v1 check or connection failure report
        try:
            v1_url = f"{api_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
            response = requests.get(v1_url, headers=headers, timeout=2, verify=False)
            if response.status_code == 200:
                return response.json().get("dag_runs", []), None
        except:
            pass
        return None, "Connection failed. Please check if the Airflow server is running and reachable."

def check_latest_dag_status(api_url="http://airflow:8080", username="admin", password="admin", dag_id="file_trigger_pipeline"):
    """
    Returns the state of the latest DAG run: "success", "failed", "running", "queued", or None (if error).
    Also returns the start_date/logical_date of the latest run.
    """
    dag_runs, error = get_dag_runs(api_url, username, password, dag_id)
    if error or not dag_runs:
        return None, error
    
    try:
        dag_runs.sort(key=lambda x: x.get("start_date") or x.get("logical_date") or "", reverse=True)
        latest_run = dag_runs[0]
        
        state = latest_run.get("state")
        run_date = latest_run.get("logical_date") or latest_run.get("start_date") or latest_run.get("execution_date")
        
        return state, run_date
    except Exception:
        return None, "Failed to parse Airflow API response."

def test_airflow_connection(api_url, username, password):
    """
    Tests credentials and connection to Airflow server.
    """
    headers = get_basic_auth_headers(username, password)
    v2_url = f"{api_url.rstrip('/')}/api/v2/dags"
    try:
        response = requests.get(v2_url, headers=headers, timeout=3, verify=False)
        if response.status_code == 200:
            return True, "Successfully connected using Airflow 3.x API (v2)"
        elif response.status_code == 404:
            v1_url = f"{api_url.rstrip('/')}/api/v1/dags"
            response = requests.get(v1_url, headers=headers, timeout=3, verify=False)
            if response.status_code == 200:
                return True, "Successfully connected using Airflow 2.x API (v1)"
            return False, f"Connection failed (HTTP {response.status_code})."
        elif response.status_code == 401:
            return False, "Authentication failed. Please check your username and password."
        else:
            return False, f"Connection failed (HTTP {response.status_code})."
    except requests.exceptions.RequestException:
        return False, "Could not reach Airflow server. Check URL and network connection."

def trigger_airflow_dag(api_url="http://airflow:8080", username="admin", password="admin", dag_id="file_trigger_pipeline"):
    """
    Triggers a run of the specified DAG via Airflow REST API.
    Tries v2 (Airflow 3.x) and falls back to v1 (Airflow 2.x).
    """
    headers = get_basic_auth_headers(username, password)
    v2_url = f"{api_url.rstrip('/')}/api/v2/dags/{dag_id}/dagRuns"
    try:
        response = requests.post(v2_url, headers=headers, json={}, timeout=3, verify=False)
        if response.status_code in [200, 201]:
            return True, "Pipeline triggered successfully."
        elif response.status_code == 404:
            v1_url = f"{api_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
            response = requests.post(v1_url, headers=headers, json={}, timeout=3, verify=False)
            if response.status_code in [200, 201]:
                return True, "Pipeline triggered successfully."
            return False, f"Failed to trigger run (HTTP {response.status_code})."
        else:
            return False, f"Failed to trigger run (HTTP {response.status_code})."
    except requests.exceptions.RequestException:
        return False, "Could not reach Airflow scheduler to trigger pipeline."

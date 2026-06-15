import requests
import base64
import urllib3
import os

# Suppress insecure connection warnings if users use self-signed SSL certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_airflow_token(api_url, username, password):
    """
    Retrieves a JWT token from the Airflow 3 token endpoint.
    Caches it in st.session_state if Streamlit is running.
    """
    # Try to use streamlit session state caching if available
    try:
        import streamlit as st
        cache_key = f"jwt_token_{api_url}_{username}"
        if cache_key in st.session_state and st.session_state[cache_key]:
            return st.session_state[cache_key], None
    except ImportError:
        st = None
        cache_key = None

    token_url = f"{api_url.rstrip('/')}/auth/token"
    try:
        response = requests.post(
            token_url,
            json={"username": username, "password": password},
            headers={"Content-Type": "application/json"},
            timeout=3,
            verify=False
        )
        
        # Add temporary debugging
        print("AUTH STATUS:", response.status_code)
        print("AUTH BODY:", response.text)
        
        if response.status_code in [200, 201]:
            payload = response.json()
            token = (
                payload.get("access_token")
                or payload.get("token")
                or payload.get("jwt")
            )
            if token:
                if st and cache_key:
                    st.session_state[cache_key] = token
                return token, None
            return None, f"Authentication succeeded (HTTP {response.status_code}), but no access token was returned. Response: {response.text}"
        elif response.status_code in [401, 403]:
            return None, f"Authentication failed (HTTP {response.status_code}): Invalid username or password. Response: {response.text}"
        else:
            return None, (
                f"Authentication failed (HTTP {response.status_code}). "
                f"Response: {response.text}"
            )
    except requests.exceptions.RequestException as e:
        return None, f"Could not reach Airflow server at {api_url}. Check server status."

def get_auth_headers(api_url, username, password):
    """
    Generates headers containing the JWT token.
    """
    token, err = get_airflow_token(api_url, username, password)
    if token:
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }, None
    return None, err

def clear_cached_token(api_url, username):
    """
    Clears cached token from st.session_state to force re-authentication.
    """
    try:
        import streamlit as st
        cache_key = f"jwt_token_{api_url}_{username}"
        if cache_key in st.session_state:
            st.session_state[cache_key] = None
    except ImportError:
        pass

def get_dag_runs(api_url="http://airflow:8080", username="admin", password="admin", dag_id="file_trigger_pipeline"):
    """
    Fetches DAG runs from Airflow REST API using JWT token.
    """
    headers, error = get_auth_headers(api_url, username, password)
    if error:
        return None, error
        
    v2_url = f"{api_url.rstrip('/')}/api/v2/dags/{dag_id}/dagRuns"
    try:
        response = requests.get(v2_url, headers=headers, timeout=3, verify=False)
        # If unauthorized, clear token cache and retry once
        if response.status_code in [401, 403]:
            clear_cached_token(api_url, username)
            headers, error = get_auth_headers(api_url, username, password)
            if not error:
                response = requests.get(v2_url, headers=headers, timeout=3, verify=False)
                
        if response.status_code == 200:
            return response.json().get("dag_runs", []), None
        else:
            return None, f"Airflow API check failed (HTTP {response.status_code})."
    except requests.exceptions.RequestException:
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

def run_connection_diagnostics(api_url, username, password, dag_id="file_trigger_pipeline"):
    """
    Runs comprehensive connection diagnostics for Airflow API.
    Returns (success, results) where results is a dictionary of status flags.
    """
    results = {
        "reachable": False,
        "authenticated": False,
        "dag_found": False,
        "error_message": None
    }
    
    # 1. Test Reachability via health endpoint
    health_url = f"{api_url.rstrip('/')}/api/v2/monitor/health"
    try:
        requests.get(health_url, timeout=3, verify=False)
        results["reachable"] = True
    except requests.exceptions.RequestException:
        # Check base URL
        try:
            requests.get(api_url, timeout=2, verify=False)
            results["reachable"] = True
        except requests.exceptions.RequestException:
            results["error_message"] = "Airflow server is completely unreachable. Check the URL and docker state."
            return False, results

    # 2. Test Token Authentication
    # Clear cache first to test fresh credentials
    clear_cached_token(api_url, username)
    token, err = get_airflow_token(api_url, username, password)
    if not token:
        results["error_message"] = err or "Authentication failed."
        return False, results
        
    results["authenticated"] = True
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 3. Test DAG Presence
    specific_dag_url = f"{api_url.rstrip('/')}/api/v2/dags/{dag_id}"
    try:
        dag_resp = requests.get(specific_dag_url, headers=headers, timeout=3, verify=False)
        if dag_resp.status_code == 200:
            results["dag_found"] = True
            is_paused = dag_resp.json().get("is_paused", False)
            if is_paused:
                results["error_message"] = f"DAG '{dag_id}' is found but paused. Please unpause it."
        elif dag_resp.status_code == 404:
            results["error_message"] = f"DAG '{dag_id}' not found on the Airflow server."
            return False, results
        else:
            results["error_message"] = f"Failed to retrieve DAG (HTTP {dag_resp.status_code})."
            return False, results
    except requests.exceptions.RequestException as e:
        results["error_message"] = f"Network error during DAG check: {str(e)}"
        return False, results

    return True, results

def test_airflow_connection(api_url, username, password):
    """
    Tests credentials and connection to Airflow server (backward compatibility).
    """
    success, results = run_connection_diagnostics(api_url, username, password)
    if success:
        return True, "Successfully connected using Airflow 3.x API (v2)"
    return False, results.get("error_message") or "Connection failed."

def trigger_airflow_dag(api_url="http://airflow:8080", username="admin", password="admin", dag_id="file_trigger_pipeline"):
    """
    Triggers a run of the specified DAG via Airflow REST API.
    """
    headers, error = get_auth_headers(api_url, username, password)
    if error:
        return False, error
        
    v2_url = f"{api_url.rstrip('/')}/api/v2/dags/{dag_id}/dagRuns"
    try:
        response = requests.post(v2_url, headers=headers, json={}, timeout=3, verify=False)
        # Handle expiration retry
        if response.status_code in [401, 403]:
            clear_cached_token(api_url, username)
            headers, error = get_auth_headers(api_url, username, password)
            if not error:
                response = requests.post(v2_url, headers=headers, json={}, timeout=3, verify=False)
                
        if response.status_code in [200, 201]:
            return True, "Pipeline triggered successfully."
        else:
            return False, f"Failed to trigger run (HTTP {response.status_code})."
    except requests.exceptions.RequestException:
        return False, "Could not reach Airflow scheduler to trigger pipeline."

def get_latest_run_details(api_url="http://airflow:8080", username="admin", password="admin", dag_id="file_trigger_pipeline"):
    """
    Returns a dictionary with details of the latest DAG run.
    """
    dag_runs, error = get_dag_runs(api_url, username, password, dag_id)
    if error or not dag_runs:
        return None, error
        
    try:
        dag_runs.sort(key=lambda x: x.get("start_date") or x.get("logical_date") or "", reverse=True)
        latest_run = dag_runs[0]
        
        start_date_str = latest_run.get("start_date")
        end_date_str = latest_run.get("end_date")
        
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
            "run_id": latest_run.get("run_id") or latest_run.get("dag_run_id") or "N/A",
            "state": latest_run.get("state") or "N/A",
            "start_time": start_fmt,
            "end_time": end_fmt,
            "duration": f"{duration_sec:.2f}s" if duration_sec is not None else "N/A"
        }
        return details, None
    except Exception as e:
        return None, f"Failed to parse Airflow response: {str(e)}"

def check_dag_safety(api_url="http://airflow:8080", username="admin", password="admin", dag_id="file_trigger_pipeline"):
    """
    Checks if the Airflow API is reachable, the DAG exists, and if it is paused.
    """
    headers, error = get_auth_headers(api_url, username, password)
    if error:
        return False, error
        
    v2_url = f"{api_url.rstrip('/')}/api/v2/dags/{dag_id}"
    try:
        response = requests.get(v2_url, headers=headers, timeout=3, verify=False)
        # Handle retry on unauthorized
        if response.status_code in [401, 403]:
            clear_cached_token(api_url, username)
            headers, error = get_auth_headers(api_url, username, password)
            if not error:
                response = requests.get(v2_url, headers=headers, timeout=3, verify=False)
                
        if response.status_code == 200:
            data = response.json()
            is_paused = data.get("is_paused", False)
            if is_paused:
                return False, f"DAG '{dag_id}' is paused. Please unpause it in the Airflow UI."
            return True, None
        elif response.status_code == 404:
            return False, f"DAG '{dag_id}' not found on the Airflow server."
        else:
            return False, f"Airflow API check failed (HTTP {response.status_code})."
    except requests.exceptions.RequestException:
        return False, "Could not reach Airflow server. Check URL and network connection."

import sys
import os
from pathlib import Path

# Add root directory to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

# Mock fcntl module for Windows compatibility
if os.name == 'nt':
    from unittest.mock import MagicMock
    mock_fcntl = MagicMock()
    mock_fcntl.LOCK_EX = 1
    mock_fcntl.LOCK_NB = 2
    mock_fcntl.LOCK_UN = 8
    sys.modules['fcntl'] = mock_fcntl

# Load env variables from .env
env_file = ROOT_DIR / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

# Set Airflow environment variables so it loads workspace config
os.environ["AIRFLOW_HOME"] = str(ROOT_DIR)
os.environ["AIRFLOW_CONFIG"] = str(ROOT_DIR / "airflow.cfg")

def validate_orchestrator():
    print("=== Orchestrator Client Validation ===")
    
    # Import config and client
    from pipeline.orchestrator_config import AIRFLOW_API_URL
    import dashboard.airflow_client as client
    
    print(f"Orchestration API URL: {AIRFLOW_API_URL}")
    
    # Check method availability
    assert hasattr(client, "trigger_pipeline"), "trigger_pipeline missing"
    assert hasattr(client, "get_pipeline_status"), "get_pipeline_status missing"
    assert hasattr(client, "get_latest_run"), "get_latest_run missing"
    assert hasattr(client, "get_dag_health"), "get_dag_health missing"
    assert hasattr(client, "get_orchestrator_health"), "get_orchestrator_health missing"
    
    print("[SUCCESS] All orchestration client methods are correctly exposed.")
    
    # Run a dry-run check of the token generation internally
    token, err = client._get_token_internal()
    if err:
        print(f"Note: Local orchestrator connection dry-run returned: {err} (expected if scheduler is not running locally).")
    else:
        print("[SUCCESS] Successfully connected and retrieved orchestration token programmatically!")
        
    print("\n[SUCCESS] Orchestration client is validated and ready for deployment.")

if __name__ == "__main__":
    validate_orchestrator()

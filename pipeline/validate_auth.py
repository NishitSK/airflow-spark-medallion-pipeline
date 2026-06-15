import os
import sys
from pathlib import Path

# Add the root directory and pipeline folder to sys.path so we can import modules
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

# Load environment variables from .env if present
env_file = ROOT_DIR / ".env"
if env_file.exists():
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, val = line.split("=", 1)
                os.environ[key.strip()] = val.strip()

# Set Airflow environment variables so it loads the workspace configuration
os.environ["AIRFLOW_HOME"] = str(ROOT_DIR)
os.environ["AIRFLOW_CONFIG"] = str(ROOT_DIR / "airflow.cfg")

# Now import CustomSimpleAuthManager
from pipeline.custom_auth_manager import CustomSimpleAuthManager
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.configuration import conf

def validate():
    print("=== Airflow 3 Auth Manager Validation ===")
    
    # 1. Check Auth Manager Registration in Airflow Configuration
    configured_manager = conf.get("core", "auth_manager")
    print(f"Auth Manager Configuration: {configured_manager}")
    assert configured_manager == "pipeline.custom_auth_manager.CustomSimpleAuthManager", \
        f"Expected CustomSimpleAuthManager, got {configured_manager}"
    
    # 2. Check Active User & Password Source from Environment
    expected_user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
    expected_password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin123")
    print(f"Active User (from .env): {expected_user}")
    print(f"Password Source: .env")
    
    # 3. Simulate and verify static methods
    users = SimpleAuthManager.get_users()
    print(f"SimpleAuthManager.get_users() returned: {users}")
    assert len(users) == 1, "Expected exactly one user"
    assert users[0]["username"] == expected_user, f"Expected user {expected_user}, got {users[0]['username']}"
    
    passwords = SimpleAuthManager.get_passwords(users)
    # Mask password for security
    masked_pw = "*" * len(passwords.get(expected_user, ""))
    print(f"SimpleAuthManager.get_passwords() username: {list(passwords.keys())}, password: {masked_pw}")
    assert passwords.get(expected_user) == expected_password, "Password mismatch!"
    
    print("\n[SUCCESS] Validation successful! Custom auth manager is registered and active.")

if __name__ == "__main__":
    validate()

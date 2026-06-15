import os
import logging
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager

log = logging.getLogger("airflow.api_fastapi.auth.managers.simple")

# Monkeypatch SimpleAuthManager's static methods because SimpleAuthManagerLogin
# imports and calls SimpleAuthManager.get_passwords and SimpleAuthManager.get_users directly
# rather than using get_auth_manager().

def custom_get_users(*args, **kwargs) -> list[dict[str, str]]:
    user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
    return [{"username": user, "role": "ADMIN"}]

def custom_get_passwords(*args, **kwargs) -> dict[str, str]:
    user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
    password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin123")
    return {user: password}

SimpleAuthManager.get_users = staticmethod(custom_get_users)
SimpleAuthManager.get_passwords = staticmethod(custom_get_passwords)


class CustomSimpleAuthManager(SimpleAuthManager):
    """
    Custom Simple Auth Manager that reads credentials from environment variables (.env)
    instead of generating or writing password files to disk.
    """
    
    def init(self) -> None:
        # Avoid running parent's init() to prevent writing/generating password files to disk.
        user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        
        msg_user = f"Configured Airflow User: {user}"
        msg_source = "Credential Source: .env"
        
        # Log to the airflow logger
        log.info(msg_user)
        log.info(msg_source)
        
        # Print using simple auth manager's own standard formatted output so it is
        # explicitly written to stdout/console.
        self._print_output(msg_user)
        self._print_output(msg_source)

    @staticmethod
    def get_passwords(*args, **kwargs) -> dict[str, str]:
        user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin123")
        return {user: password}

    @staticmethod
    def get_users(*args, **kwargs) -> list[dict[str, str]]:
        user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        return [{"username": user, "role": "ADMIN"}]

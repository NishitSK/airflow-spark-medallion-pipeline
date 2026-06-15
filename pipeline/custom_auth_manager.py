import os
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager

class CustomSimpleAuthManager(SimpleAuthManager):
    """
    Custom Simple Auth Manager that reads credentials from environment variables (.env)
    instead of generating or writing password files to disk.
    """
    
    def init(self) -> None:
        user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        print(f"Configured Airflow User: {user}")
        print("Credential Source: .env")
        
    @staticmethod
    def get_passwords() -> dict[str, str]:
        user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin123")
        return {user: password}

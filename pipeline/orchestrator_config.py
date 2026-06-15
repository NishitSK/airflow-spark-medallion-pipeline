import os

# Orchestrator parameters loaded programmatically from the environment
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin123")

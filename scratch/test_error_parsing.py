import sys
import os
from pathlib import Path

# Add project root and dashboard to path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(ROOT_DIR / "dashboard"))

import airflow_client
from app import get_user_friendly_error

def test():
    print("=== Testing Traceback parsing ===")
    log_text = """
    standalone | Starting Airflow Standalone
    standalone | [Layer 1: Bronze]
    standalone | Ingesting Bronze...
    standalone | [Layer 1.5: Data Quality Validation]
    standalone | [Layer 2: Silver]
    standalone | Transforming Silver...
    standalone | [Layer 3: Gold]
    standalone | Generating Gold...
    standalone | Traceback (most recent call last):
    standalone |   File "/opt/airflow/spark_jobs/unified_pipeline.py", line 145, in run_unified_pipeline
    standalone |     with open(tmp_file, "w") as f:
    standalone | PermissionError: [Errno 13] Permission denied: '/data/output/pipeline_metrics.txt'
    standalone | Command exited with return code 1
    """
    
    exc_type, exc_msg, failed_stage = airflow_client.extract_exception_from_log(log_text)
    print("Parsed values:")
    print("Exception Type:", exc_type)
    print("Exception Msg:", exc_msg)
    print("Failed Stage:", failed_stage)
    
    assert exc_type == "PermissionError"
    assert "Permission denied" in exc_msg
    assert failed_stage == "Gold"
    
    # Test friendly conversion
    err_str = f"Exception: {exc_type}: {exc_msg} | Stage: {failed_stage}"
    reason, suggestion = get_user_friendly_error(err_str)
    print("\nFormatted UI Reason:\n", reason)
    print("Formatted UI Suggestion:\n", suggestion)
    
    assert "A storage path or file write operation failed" in reason
    assert "Exception Details" in reason
    assert "PermissionError" in reason
    assert "Failed Stage" in reason
    assert "Gold" in reason
    assert "UID 50000" in suggestion
    
    print("\n[SUCCESS] Error parsing and formatting tests passed successfully!")

if __name__ == "__main__":
    test()

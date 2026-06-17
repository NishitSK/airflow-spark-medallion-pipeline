"""
Schema Validator
================
Defines expected schema from configuration, validates incoming datasets,
and writes a schema compatibility report.
"""
import os
import json
import yaml
from pyspark.sql import DataFrame

class SchemaValidationError(Exception):
    """Custom exception raised when schema validation fails."""
    pass

def _load_config():
    """Load dq_config.yaml. Searches standard locations."""
    search_paths = [
        os.environ.get("DQ_CONFIG_FILE", ""),
        "/opt/airflow/pipeline/dq_config.yaml",
        os.path.join(os.path.dirname(__file__), "dq_config.yaml"),
    ]
    for path in search_paths:
        if path and os.path.exists(path):
            try:
                with open(path, "r") as f:
                    return yaml.safe_load(f) or {}
            except Exception as e:
                print(f"[SchemaValidator] Warning loading config at {path}: {e}")
    return {}

def validate_schema(df: DataFrame, source_file: str = "unknown") -> dict:
    """
    Checks that the dataframe contains all required canonical columns.
    Generates a schema compatibility report JSON file.
    Raises SchemaValidationError if any required column is missing.
    """
    config = _load_config()
    columns_config = config.get("columns", {})
    
    # 1. Determine expected required columns
    required_cols = [col_name for col_name, col_cfg in columns_config.items() if col_cfg.get("required", False)]
    
    # If no configuration found, default to minimal required columns
    if not required_cols:
        required_cols = ["id", "name", "age"]
        
    df_cols = df.columns
    found_cols = [col for col in required_cols if col in df_cols]
    missing_cols = [col for col in required_cols if col not in df_cols]
    
    # Extra columns are anything in df that isn't expected (or metadata)
    system_metadata = ["ingestion_time", "source_file"]
    config_all_cols = list(columns_config.keys()) if columns_config else required_cols
    extra_cols = [col for col in df_cols if col not in config_all_cols and col not in system_metadata]
    
    # 2. Build report
    report = {
        "source_file": source_file,
        "required_columns_expected": required_cols,
        "required_columns_found": found_cols,
        "missing_required_columns": missing_cols,
        "extra_columns_found": extra_cols,
        "final_schema": df_cols,
        "is_compatible": len(missing_cols) == 0
    }
    
    # Save the compatibility report to the output directory
    try:
        from pipeline.config import BASE_DATA_PATH
        report_dir = os.path.join(BASE_DATA_PATH, "output")
        os.makedirs(report_dir, exist_ok=True)
        report_path = os.path.join(report_dir, "schema_compatibility_report.json")
        
        # Write report atomically to avoid partial file reads
        tmp_path = f"{report_path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(report, f, indent=4)
        os.replace(tmp_path, report_path)
        print(f"[SchemaValidator] Schema compatibility report written to {report_path}")
    except Exception as e:
        print(f"[SchemaValidator] Warning: Could not write compatibility report: {e}")
        
    # 3. Fail if missing columns
    if missing_cols:
        if len(missing_cols) == 1:
            msg = f"Unsupported dataset schema. Missing required column: {missing_cols[0]}"
        else:
            msg = f"Unsupported dataset schema. Missing required columns: {', '.join(missing_cols)}"
        raise SchemaValidationError(msg)
        
    return report

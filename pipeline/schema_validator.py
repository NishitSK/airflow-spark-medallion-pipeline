"""
Schema Validator
================
Defines expected schema from configuration, validates incoming datasets,
detects dataset type (CUSTOMER, ORDERS, or GENERIC), and writes a schema compatibility report.
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

def detect_dataset_type(df: DataFrame) -> str:
    """
    Analyzes DataFrame columns to match them against known schemas (CUSTOMER, ORDERS).
    Matches are case-insensitive and allow aliases. Falls back to GENERIC.
    """
    cols = [c.lower() for c in df.columns]
    
    # Define default registry in case config does not have it
    config = _load_config()
    registry = config.get("registry", {
        "CUSTOMER": {
            "required": ["id", "name", "age"],
            "aliases": {
                "id": ["id", "ID", "user_id", "customer_id", "record_id", "uid", "userId", "customerId"],
                "name": ["name", "full_name", "customer_name", "user_name", "fullname", "displayName", "display_name"],
                "age": ["age", "customer_age", "user_age", "years_old", "Age"]
            }
        },
        "ORDERS": {
            "required": ["order_id", "product_name", "quantity", "unit_price"],
            "aliases": {
                "order_id": ["order_id", "orderId", "orderNo", "order_no", "orderid"],
                "product_name": ["product_name", "product", "item_name", "item", "productName"],
                "quantity": ["quantity", "qty", "units", "count", "quantity_ordered"],
                "unit_price": ["unit_price", "price", "unitPrice", "rate", "unitprice"]
            }
        }
    })
    
    def matches_schema(required_cols, aliases):
        for req in required_cols:
            alias_list = [a.lower() for a in aliases.get(req, [])] + [req.lower()]
            found = False
            for a in alias_list:
                if a in cols:
                    found = True
                    break
            if not found:
                return False
        return True

    if matches_schema(registry["CUSTOMER"]["required"], registry["CUSTOMER"]["aliases"]):
        return "CUSTOMER"
    elif matches_schema(registry["ORDERS"]["required"], registry["ORDERS"]["aliases"]):
        return "ORDERS"
    else:
        return "GENERIC"

def validate_schema(df: DataFrame, dataset_type: str, source_file: str = "unknown") -> dict:
    """
    Checks that the dataframe contains all required canonical columns for the active schema.
    Generates a schema compatibility report JSON file.
    Only raises SchemaValidationError if missing required columns on known schemas.
    """
    config = _load_config()
    registry = config.get("registry", {
        "CUSTOMER": {
            "required": ["id", "name", "age"],
        },
        "ORDERS": {
            "required": ["order_id", "product_name", "quantity", "unit_price"],
        }
    })
    
    # 1. Determine expected required columns for this dataset type
    is_supported = dataset_type in ["CUSTOMER", "ORDERS"]
    required_cols = []
    if is_supported:
        required_cols = registry.get(dataset_type, {}).get("required", [])
        
    df_cols = df.columns
    found_cols = [col for col in required_cols if col in df_cols]
    missing_cols = [col for col in required_cols if col not in df_cols]
    
    # 2. Build report
    report = {
        "dataset_type": dataset_type,
        "is_supported": is_supported,
        "found_columns": df_cols
    }
    if is_supported:
        report["expected_columns"] = required_cols
        report["missing_required_columns"] = missing_cols
        report["is_compatible"] = len(missing_cols) == 0
    
    # Save the compatibility report to the output directory
    try:
        from pipeline.config import BASE_DATA_PATH
        report_dir = os.path.join(BASE_DATA_PATH, "output")
        os.makedirs(report_dir, exist_ok=True)
        report_path = os.path.join(report_dir, "schema_compatibility_report.json")
        
        # Write report atomically
        tmp_path = f"{report_path}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(report, f, indent=4)
        os.replace(tmp_path, report_path)
        print(f"[SchemaValidator] Schema compatibility report written to {report_path}")
    except Exception as e:
        print(f"[SchemaValidator] Warning: Could not write compatibility report: {e}")
        
    # 3. Fail if missing columns on known schemas
    if is_supported and missing_cols:
        if len(missing_cols) == 1:
            msg = f"Unsupported dataset schema. Missing required column: {missing_cols[0]}"
        else:
            msg = f"Unsupported dataset schema. Missing required columns: {', '.join(missing_cols)}"
        raise SchemaValidationError(msg)
        
    return report

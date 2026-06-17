"""
Schema Mapper
=============
Reads the dq_config.yaml registry aliases and renames incoming DataFrame columns
to the canonical schema expected by the active dataset mode. Logs all mappings.
"""
import os
import yaml
import json
from pyspark.sql import DataFrame

def _load_config():
    """Load dq_config.yaml. Searches standard locations."""
    search_paths = [
        os.environ.get("DQ_CONFIG_FILE", ""),
        "/opt/airflow/pipeline/dq_config.yaml",
        os.path.join(os.path.dirname(__file__), "dq_config.yaml"),
    ]
    for path in search_paths:
        if path and os.path.exists(path):
            with open(path, "r") as f:
                return yaml.safe_load(f)
    return {}

def apply_schema_mapping(df: DataFrame, dataset_type: str, run_id: str = "unknown", source_file: str = "unknown"):
    """
    Inspect df columns and rename any aliases to canonical names based on dataset type.
    Returns (mapped_df, mappings_applied, unresolved_columns)
    """
    if dataset_type == "GENERIC":
        # No mapping applied for Generic datasets
        unresolved = [c for c in df.columns if c not in ["ingestion_time", "source_file"]]
        _log_mappings([], unresolved, run_id, source_file, df.sparkSession)
        return df, [], unresolved

    config = _load_config()
    registry = config.get("registry", {
        "CUSTOMER": {
            "aliases": {
                "id": ["id", "ID", "user_id", "customer_id", "record_id", "uid", "userId", "customerId"],
                "name": ["name", "full_name", "customer_name", "user_name", "fullname", "displayName", "display_name"],
                "age": ["age", "customer_age", "user_age", "years_old", "Age"]
            }
        },
        "ORDERS": {
            "aliases": {
                "order_id": ["order_id", "orderId", "orderNo", "order_no", "orderid"],
                "product_name": ["product_name", "product", "item_name", "item", "productName"],
                "quantity": ["quantity", "qty", "units", "count", "quantity_ordered"],
                "unit_price": ["unit_price", "price", "unitPrice", "rate", "unitprice"]
            }
        }
    })

    aliases = registry.get(dataset_type, {}).get("aliases", {})

    # Build reverse lookup: alias → canonical
    reverse_map = {}
    for canonical, alias_list in aliases.items():
        for alias in alias_list:
            reverse_map[alias.lower()] = canonical

    mappings_applied = []
    unresolved = []
    rename_map = {}

    for col in df.columns:
        canonical = reverse_map.get(col.lower())
        if canonical and canonical != col:
            rename_map[col] = canonical
            mappings_applied.append({"from_col": col, "to_col": canonical})
        elif canonical == col:
            pass  # Already canonical name
        else:
            if col not in ["ingestion_time", "source_file"]:
                unresolved.append(col)

    # Apply renames
    mapped_df = df
    for old_name, new_name in rename_map.items():
        mapped_df = mapped_df.withColumnRenamed(old_name, new_name)

    # Log mappings
    _log_mappings(mappings_applied, unresolved, run_id, source_file, mapped_df.sparkSession)

    return mapped_df, mappings_applied, unresolved

def _log_mappings(mappings, unresolved, run_id, source_file, spark):
    """Append schema mapping log directly using Pandas to avoid Spark job overhead."""
    try:
        import time
        import pandas as pd
        import uuid
        from pipeline.config import SCHEMA_MAP_LOG_PATH

        records = [{
            "run_id": run_id,
            "source_file": source_file,
            "mapped_columns": json.dumps(mappings),
            "unresolved_columns": json.dumps(unresolved),
            "mapping_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        }]
        
        os.makedirs(SCHEMA_MAP_LOG_PATH, exist_ok=True)
        pdf = pd.DataFrame(records)
        pdf.to_parquet(os.path.join(SCHEMA_MAP_LOG_PATH, f"part-py-{uuid.uuid4().hex}.snappy.parquet"), index=False)
    except Exception as e:
        print(f"[SchemaMapper] Warning: Could not write mapping log: {e}")

def get_mapping_summary(mappings: list, unresolved: list) -> str:
    """Human-readable summary of schema mappings for dashboard display."""
    lines = []
    if mappings:
        for m in mappings:
            lines.append(f"  ✅ '{m['from_col']}' → '{m['to_col']}'")
    if unresolved:
        for col in unresolved:
            lines.append(f"  ⚠️  '{col}' — not mapped to canonical schema (kept as-is)")
    if not mappings and not unresolved:
        lines.append("  ✅ Schema matches canonical format — no mapping required.")
    return "\n".join(lines)

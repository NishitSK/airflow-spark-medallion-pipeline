"""
Quarantine Writer
=================
Stores all rejected rows with enriched metadata into the quarantine Delta table.
Invalid rows are NEVER discarded — they are always traceable.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pipeline.config import QUARANTINE_PATH
from pipeline.delta_utils import write_delta


def write_quarantine(invalid_df: DataFrame, run_id: str = "unknown", source_file: str = "unknown", row_count: int = None):
    """
    Append rejected rows to the quarantine Delta table.
    invalid_df must already contain: quarantine_reason, rule_violated, run_id, dq_source_file
    """
    if invalid_df is None:
        return 0

    try:
        if row_count is None:
            row_count = invalid_df.count()
        if row_count == 0:
            print("[Quarantine] No invalid rows to quarantine.")
            return 0

        # Ensure required columns are present
        enriched = invalid_df
        if "quarantine_time" not in enriched.columns:
            enriched = enriched.withColumn("quarantine_time", current_timestamp())
        if "run_id" not in enriched.columns:
            enriched = enriched.withColumn("run_id", lit(run_id))
        if "dq_source_file" not in enriched.columns:
            enriched = enriched.withColumn("dq_source_file", lit(source_file))
        if "quarantine_reason" not in enriched.columns:
            enriched = enriched.withColumn("quarantine_reason", lit("unknown"))
        if "rule_violated" not in enriched.columns:
            enriched = enriched.withColumn("rule_violated", lit("unknown"))

        write_delta(enriched, QUARANTINE_PATH, mode="append")
        print(f"[Quarantine] {row_count} rows written to quarantine table.")
        return row_count

    except Exception as e:
        print(f"[Quarantine] ERROR writing to quarantine table: {e}")
        return 0


def get_quarantine_count(spark: SparkSession, run_id: str = None) -> int:
    """Returns the count of quarantined rows for a given run_id (or total)."""
    import os
    try:
        if not os.path.exists(QUARANTINE_PATH):
            return 0
        df = spark.read.format("delta").load(QUARANTINE_PATH)
        if run_id:
            df = df.filter(df.run_id == run_id)
        return df.count()
    except Exception:
        return 0

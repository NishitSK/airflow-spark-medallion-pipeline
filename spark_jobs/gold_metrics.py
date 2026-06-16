"""
Gold Metrics
============
Generates enriched Gold analytics including business KPIs plus
a run-level summary row with DQ metadata, rejection stats,
anomaly flags, and schema mapping summary.
"""
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, lit, current_timestamp
from pipeline.config import SILVER_PATH, GOLD_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, read_delta, write_delta


def generate_gold(spark=None, scorecard: dict = None, anomalies: list = None,
                  mappings: list = None, run_id: str = "unknown", runtime_seconds: float = 0.0):
    """
    Generate enriched Gold metrics from Silver layer.
    Includes DQ scorecard data, anomaly flags, and schema mapping summary.
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("GoldMetrics")
        own_spark = True

    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    scorecard  = scorecard  or {}
    anomalies  = anomalies  or []
    mappings   = mappings   or []

    try:
        if not os.path.exists(SILVER_PATH):
            print("[Gold] Silver path does not exist. Skipping Gold generation.")
            return

        silver_df = read_delta(spark, SILVER_PATH)

        # -- Business aggregations (only clean records) --
        business_df = silver_df.groupBy("processed_date").agg(
            avg("age").alias("average_age"),
            count("*").alias("total_users")
        )

        write_delta(business_df, GOLD_PATH, mode="overwrite", partition_by="processed_date")

        # -- Enriched run summary row --
        import json
        anomaly_str  = "; ".join(anomalies)  if anomalies  else "None"
        mapping_str  = json.dumps(mappings)   if mappings   else "[]"

        summary_data = [{
            "run_id":           run_id,
            "source_file":      str(scorecard.get("source_file", "unknown")),
            "total_rows":       float(scorecard.get("total_rows", 0)),
            "valid_rows":       float(scorecard.get("valid_rows", 0)),
            "invalid_rows":     float(scorecard.get("invalid_rows", 0)),
            "dq_score":         float(scorecard.get("dq_score", 100.0)),
            "null_ids":         float(scorecard.get("null_ids", 0)),
            "malformed_ids":    float(scorecard.get("malformed_ids", 0)),
            "duplicate_ids":    float(scorecard.get("duplicate_ids", 0)),
            "null_ages":        float(scorecard.get("null_ages", 0)),
            "invalid_ages":     float(scorecard.get("invalid_ages", 0)),
            "null_names":       float(scorecard.get("null_names", 0)),
            "rows_received":    float(scorecard.get("total_rows", 0)),
            "rows_accepted":    float(scorecard.get("valid_rows", 0)),
            "rows_rejected":    float(scorecard.get("invalid_rows", 0)),
            "runtime_seconds":  float(round(runtime_seconds, 2)),
            "anomaly_flags":    anomaly_str,
            "schema_mappings":  mapping_str,
        }]

        from pipeline.config import DQ_REPORT_PATH
        summary_df = spark.createDataFrame(summary_data) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("report_time", current_timestamp())
        write_delta(summary_df, DQ_REPORT_PATH, mode="append")

        print(f"[Gold] Metrics generation complete. Run: {run_id} | DQ Score: {scorecard.get('dq_score', 'N/A')}%")

    except Exception as e:
        print(f"[Gold] Metrics Generation Failed: {str(e)}")
        if own_spark:
            sys.exit(1)
        raise e
    finally:
        if own_spark:
            spark.stop()


if __name__ == "__main__":
    generate_gold()

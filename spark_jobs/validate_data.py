"""
Validate Data
=============
Delegates to the enterprise DQ Engine for row-level validation.
Writes scorecard metrics. Returns (valid_df, invalid_df, scorecard, should_fail).
"""
import sys
import os
import yaml
from pyspark.sql import SparkSession
from pipeline.config import BRONZE_PATH, DQ_METRICS_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, read_delta, write_delta
from spark_jobs.dq_engine import run_dq_engine, write_dq_scorecard, exceeds_threshold
from pyspark.sql.functions import current_timestamp


def _load_config():
    paths = [
        "/opt/airflow/pipeline/dq_config.yaml",
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "pipeline", "dq_config.yaml"),
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p) as f:
                return yaml.safe_load(f)
    return {}


def validate_data(spark=None, run_id="unknown", source_file="unknown", bronze_df=None):
    """
    Runs enterprise row-level DQ on Bronze data.
    Returns: (valid_df, invalid_df, scorecard, should_fail)
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("DataValidation")
        own_spark = True

    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

    try:
        if bronze_df is not None:
            df = bronze_df
        else:
            if not os.path.exists(BRONZE_PATH):
                print("[Validate] Bronze path does not exist. Nothing to validate.")
                return None, None, {}, False
            df = read_delta(spark, BRONZE_PATH)

        # Run enterprise DQ engine
        valid_df, invalid_df, scorecard = run_dq_engine(df, run_id=run_id, source_file=source_file)

        # Persist DQ scorecard
        write_dq_scorecard(scorecard, spark)

        # Write legacy DQ metrics for backward-compatible dashboard queries
        _write_legacy_dq_metrics(scorecard, spark)

        # Check thresholds
        config = _load_config()
        should_fail, reason = exceeds_threshold(scorecard, config)
        if should_fail:
            print(f"[Validate] CRITICAL DQ FAILURE: {reason}")

        print(f"[Validate] DQ Score: {scorecard['dq_score']}% | "
              f"Valid: {scorecard['valid_rows']} | Invalid: {scorecard['invalid_rows']}")

        return valid_df, invalid_df, scorecard, should_fail

    except Exception as e:
        print(f"[Validate] Error: {str(e)}")
        return None, None, {}, False
    finally:
        if own_spark:
            spark.stop()


def _write_legacy_dq_metrics(scorecard: dict, spark: SparkSession):
    """Write to legacy dq_metrics Delta table for backward-compatible dashboard charts."""
    try:
        metrics_data = [(
            int(scorecard.get("total_rows", 0)),
            int(scorecard.get("null_ids", 0)),
            int(scorecard.get("invalid_ages", 0) + scorecard.get("null_ages", 0)),
            int(scorecard.get("duplicate_ids", 0)),
            int(scorecard.get("null_ages", 0)),
        )]
        cols = ["total_rows", "null_ids", "invalid_ages", "duplicate_ids", "negative_ages"]
        metrics_df = spark.createDataFrame(metrics_data, cols) \
            .withColumn("validation_time", current_timestamp())
        write_delta(metrics_df, DQ_METRICS_PATH, mode="append")
    except Exception as e:
        print(f"[Validate] Warning: Could not write legacy DQ metrics: {e}")


if __name__ == "__main__":
    result = validate_data()
    if result[3]:  # should_fail
        sys.exit(1)

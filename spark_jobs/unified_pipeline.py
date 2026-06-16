"""
Unified Enterprise Medallion Pipeline
======================================
Orchestrates all pipeline stages with full enterprise DQ capabilities:
  1. Bronze Ingestion (with schema mapping)
  2. Data Profiling
  3. DQ Validation (row-level) → valid_df + invalid_df
  4. Quarantine (invalid rows persisted)
  5. Silver Transformation (valid rows only)
  6. Gold Metrics (enriched with DQ scorecard)

The DAG never silently discards data. Invalid rows go to quarantine.
"""
import sys
import time
import os
import json
import uuid
from pipeline.config import (
    METRICS_FILE, INCIDENTS_PATH, STATUS_FILE, INPUT_PATH,
    HISTORY_FILE, SILVER_PATH
)
from pipeline.delta_utils import get_spark_session
from spark_jobs.bronze_ingest import ingest_bronze
from spark_jobs.data_profiler import profile_dataframe, write_profile
from spark_jobs.validate_data import validate_data
from spark_jobs.quarantine import write_quarantine
from spark_jobs.silver_transform import transform_silver
from spark_jobs.gold_metrics import generate_gold
from monitoring.log_incident import log_incident


# ------------------------------------------------------------------ #
# Utility helpers
# ------------------------------------------------------------------ #
def log_diagnostics(file_path):
    import logging
    from pathlib import Path
    logger = logging.getLogger("ResilientWriter")
    uid = gid = None
    try:
        uid = os.getuid()
        gid = os.getgid()
    except AttributeError:
        pass
    path = Path(file_path)
    parent_dir = path.parent
    path_exists = path.exists()
    target = str(path) if path_exists else str(parent_dir)
    is_writable = os.access(target, os.W_OK)
    msg = (f"[DIAGNOSTICS] Writing to: {file_path} | "
           f"Parent exists: {parent_dir.exists()} | File exists: {path_exists} | "
           f"UID: {uid} | GID: {gid} | Writable: {is_writable}")
    logger.warning(msg)
    print(msg)


def write_status(status, run_id, file_name=None, error=None, duration=None, stage=None):
    if not file_name and os.path.exists(INPUT_PATH):
        try:
            files = [f for f in os.listdir(INPUT_PATH) if f.endswith(('.csv', '.json'))]
            if files:
                files.sort(key=lambda x: os.path.getmtime(os.path.join(INPUT_PATH, x)), reverse=True)
                file_name = files[0]
        except Exception:
            pass
    if not file_name and os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE) as f:
                old = json.load(f)
                file_name = old.get("file_name")
        except Exception:
            pass

    status_data = {
        "status":    status,
        "run_id":    run_id,
        "timestamp": time.time(),
        "file_name": file_name or "Unknown",
        "stage":     stage or "Waiting",
        "error":     error,
        "duration":  f"{duration:.2f}" if duration is not None else None,
    }
    try:
        log_diagnostics(STATUS_FILE)
    except Exception as e:
        print(f"Diagnostics error: {e}")
    try:
        from pathlib import Path
        Path(STATUS_FILE).parent.mkdir(parents=True, exist_ok=True)
        tmp = f"{STATUS_FILE}.tmp"
        with open(tmp, "w") as f:
            json.dump(status_data, f, indent=4)
        os.replace(tmp, STATUS_FILE)
    except Exception as e:
        print(f"Error writing status file: {e}")


def append_to_history(status, run_id, file_name=None, error=None, duration=None, rows=0):
    if not file_name and os.path.exists(INPUT_PATH):
        try:
            files = [f for f in os.listdir(INPUT_PATH) if f.endswith(('.csv', '.json'))]
            if files:
                files.sort(key=lambda x: os.path.getmtime(os.path.join(INPUT_PATH, x)), reverse=True)
                file_name = files[0]
        except Exception:
            pass
    if not file_name and os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE) as f:
                old = json.load(f)
                file_name = old.get("file_name")
        except Exception:
            pass
    try:
        log_diagnostics(HISTORY_FILE)
    except Exception as e:
        print(f"Diagnostics error: {e}")
    try:
        from pathlib import Path
        Path(HISTORY_FILE).parent.mkdir(parents=True, exist_ok=True)
        record = {
            "timestamp": time.time(),
            "run_id":    run_id,
            "file_name": file_name or "Unknown",
            "status":    status,
            "duration":  f"{duration:.2f}" if duration is not None else "N/A",
            "rows":      rows,
            "error":     error,
        }
        tmp = f"{HISTORY_FILE}.tmp"
        existing = ""
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE) as f:
                    existing = f.read()
            except Exception:
                pass
        with open(tmp, "w") as f:
            f.write(existing + json.dumps(record) + "\n")
        os.replace(tmp, HISTORY_FILE)
    except Exception as e:
        print(f"Error writing history file: {e}")


# ------------------------------------------------------------------ #
# Main pipeline
# ------------------------------------------------------------------ #
def run_unified_pipeline():
    start_time = time.time()
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    spark = None
    source_file = "unknown"
    anomalies = []
    mappings = []
    scorecard = {}

    print(f"\n{'='*60}")
    print(f"  ENTERPRISE MEDALLION PIPELINE")
    print(f"  Run ID: {run_id}")
    print(f"{'='*60}\n")

    try:
        spark = get_spark_session("UnifiedPipeline")

        # ============================================================
        # STAGE 1: Bronze Ingestion + Schema Mapping
        # ============================================================
        write_status("running", run_id, stage="Bronze")
        print(f"\n[Stage 1] Bronze Ingestion...")

        bronze_df, source_file = ingest_bronze(spark=spark, run_id=run_id)
        if bronze_df is None:
            write_status("failed", run_id, error="No input files found.", stage="Bronze")
            append_to_history("failed", run_id, error="No input files found.")
            return

        # Extract mappings if they were logged during ingestion
        try:
            from pipeline.config import SCHEMA_MAP_LOG_PATH
            if os.path.exists(SCHEMA_MAP_LOG_PATH):
                import json as _json
                ml = spark.read.format("delta").load(SCHEMA_MAP_LOG_PATH) \
                    .filter(f"run_id = '{run_id}'").limit(1).collect()
                if ml:
                    mappings = _json.loads(ml[0]["mapped_columns"] or "[]")
        except Exception:
            pass

        # ============================================================
        # STAGE 2: Data Profiling
        # ============================================================
        print(f"\n[Stage 2] Data Profiling...")
        write_status("running", run_id, stage="Profiling")
        try:
            profile_records, anomalies, _ = profile_dataframe(bronze_df, run_id, source_file, spark)
            write_profile(profile_records, spark)
            if anomalies:
                print(f"[Profiler] Anomalies detected: {anomalies}")
        except Exception as profile_err:
            print(f"[Stage 2] Warning: Profiling failed (non-fatal): {profile_err}")

        # ============================================================
        # STAGE 3: DQ Validation → valid_df + invalid_df
        # ============================================================
        print(f"\n[Stage 3] Enterprise DQ Validation...")
        write_status("running", run_id, stage="Validation")

        valid_df, invalid_df, scorecard, should_fail = validate_data(
            spark=spark, run_id=run_id, source_file=source_file
        )

        # ============================================================
        # STAGE 4: Quarantine invalid rows
        # ============================================================
        quarantine_count = 0
        if invalid_df is not None:
            print(f"\n[Stage 4] Writing quarantine rows...")
            write_status("running", run_id, stage="Quarantine")
            quarantine_count = write_quarantine(invalid_df, run_id=run_id, source_file=source_file)

        if should_fail:
            write_status("failed", run_id, stage="Validation",
                         error=f"DQ threshold exceeded. Score: {scorecard.get('dq_score', 0)}%",
                         duration=time.time() - start_time)
            append_to_history("failed", run_id,
                              error=f"DQ threshold exceeded. Score: {scorecard.get('dq_score', 0)}%",
                              duration=time.time() - start_time, rows=0)
            raise RuntimeError(f"DQ threshold exceeded. Score: {scorecard.get('dq_score', 0)}%")

        # ============================================================
        # STAGE 5: Silver Transformation (valid rows only)
        # ============================================================
        print(f"\n[Stage 5] Silver Transformation...")
        write_status("running", run_id, stage="Silver")
        silver_rows = transform_silver(spark=spark, valid_df=valid_df)

        # ============================================================
        # STAGE 6: Gold Metrics (enriched)
        # ============================================================
        print(f"\n[Stage 6] Gold Metrics Generation...")
        write_status("running", run_id, stage="Gold")
        duration = time.time() - start_time
        generate_gold(
            spark=spark,
            scorecard=scorecard,
            anomalies=anomalies,
            mappings=mappings,
            run_id=run_id,
            runtime_seconds=duration,
        )

        # ============================================================
        # STAGE 7: S3 Exports
        # ============================================================
        print(f"\n[Stage 7] S3 Exports...")
        write_status("running", run_id, stage="S3 Export")
        try:
            from utils.s3_client import export_csv_bytes_to_s3, export_text_to_s3
            from pipeline.delta_utils import read_delta
            from pipeline.config import QUARANTINE_PATH

            # Export Silver CSV
            try:
                silver_df_export = read_delta(spark, SILVER_PATH)
                if silver_df_export is not None:
                    csv_data = silver_df_export.toPandas().to_csv(index=False).encode('utf-8')
                    export_csv_bytes_to_s3(csv_data, f"exports/{run_id}/cleaned_dataset.csv", run_id=run_id, row_count=scorecard.get("valid_rows", 0))
            except Exception as e:
                print(f"[Stage 7] Warning: Silver S3 export failed: {e}")

            # Export Quarantine CSV
            try:
                if os.path.exists(QUARANTINE_PATH):
                    q_df = read_delta(spark, QUARANTINE_PATH)
                    if q_df is not None:
                        q_df = q_df.filter(q_df.run_id == run_id)
                        if q_df.count() > 0:
                            q_csv = q_df.toPandas().to_csv(index=False).encode('utf-8')
                            export_csv_bytes_to_s3(q_csv, f"quarantine/{run_id}/rejected_records.csv", run_id=run_id, row_count=scorecard.get("invalid_rows", 0))
            except Exception as e:
                print(f"[Stage 7] Warning: Quarantine S3 export failed: {e}")

            # Export Gold TXT report
            try:
                from dashboard.queries import get_gold_report_data, generate_txt_report
                report_data = get_gold_report_data(spark)
                if report_data:
                    txt_report = generate_txt_report(report_data)
                    export_text_to_s3(txt_report, f"reports/{run_id}/gold_report.txt", run_id=run_id)
            except Exception as e:
                print(f"[Stage 7] Warning: Gold report S3 export failed: {e}")

        except Exception as s3_err:
            print(f"[Stage 7] S3 export warning (non-fatal): {s3_err}")


        # ============================================================
        # Write metrics file (non-fatal)
        # ============================================================
        try:
            from pathlib import Path
            Path(METRICS_FILE).parent.mkdir(parents=True, exist_ok=True)
            with open(METRICS_FILE, "w") as f:
                f.write(f"{time.time()},{duration:.2f}")
        except Exception as metrics_err:
            print(f"[Pipeline] Warning: metrics file write failed (non-fatal): {metrics_err}")

        # ============================================================
        # Success
        # ============================================================
        final_rows = silver_rows or scorecard.get("valid_rows", 0)
        write_status("completed", run_id, stage="Finished", duration=duration)
        append_to_history("completed", run_id, file_name=source_file,
                          duration=duration, rows=final_rows)

        print(f"\n{'='*60}")
        print(f"  PIPELINE COMPLETE")
        print(f"  Run ID     : {run_id}")
        print(f"  Duration   : {duration:.1f}s")
        print(f"  DQ Score   : {scorecard.get('dq_score', 'N/A')}%")
        print(f"  Accepted   : {scorecard.get('valid_rows', 0):,} rows → Silver")
        print(f"  Quarantined: {quarantine_count:,} rows → Quarantine")
        if anomalies:
            print(f"  Anomalies  : {len(anomalies)} detected")
        print(f"{'='*60}\n")

    except Exception as e:
        duration = time.time() - start_time
        error_msg = str(e)
        print(f"\n[Pipeline] FAILED after {duration:.1f}s: {error_msg}")
        write_status("failed", run_id, error=error_msg, stage="Failed", duration=duration)
        append_to_history("failed", run_id, file_name=source_file,
                          error=error_msg, duration=duration, rows=0)
        try:
            log_incident("file_trigger_pipeline", run_id, "unified_medallion_pipeline", error_msg)
        except Exception:
            pass
        if spark:
            spark.stop()
        sys.exit(1)

    if spark:
        spark.stop()


if __name__ == "__main__":
    run_unified_pipeline()

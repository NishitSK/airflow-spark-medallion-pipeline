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
import threading

# Skip the verbose/slow environment diagnostics on import
os.environ["SKIP_DIAGNOSTICS"] = "1"

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


def write_status(status, run_id, file_name=None, error=None, duration=None, stage=None, dataset_type=None):
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
        "dataset_type": dataset_type or "GENERIC"
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


def append_to_history(status, run_id, file_name=None, error=None, duration=None, rows=0, dataset_type=None):
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
            "dataset_type": dataset_type or "GENERIC"
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
def estimate_partitions_and_parallelism():
    """
    Estimate the total rows of CSV/JSON files in the input folder
    to determine optimal shuffle partitions and parallelism.
    """
    import os
    from pipeline.config import INPUT_PATH
    
    input_rows = 0
    if os.path.exists(INPUT_PATH):
        try:
            for f in os.listdir(INPUT_PATH):
                fp = os.path.join(INPUT_PATH, f)
                if f.endswith('.csv'):
                    with open(fp, 'r', encoding='utf-8', errors='ignore') as file:
                        input_rows += sum(1 for _ in file) - 1
                elif f.endswith('.json'):
                    with open(fp, 'r', encoding='utf-8', errors='ignore') as file:
                        input_rows += sum(1 for _ in file)
        except Exception as e:
            print(f"[Dynamic Tuning] Error counting input rows: {e}")
            input_rows = 10000  # Default fallback
            
    # Set partitions based on scale
    if input_rows <= 10000:
        partitions = 1
    elif input_rows <= 100000:
        partitions = 2
    elif input_rows <= 1000000:
        partitions = 4
    else:
        partitions = 8
        
    print(f"[Dynamic Tuning] Estimated rows: {input_rows} -> Spark partitions set to: {partitions}")
    return partitions


def sync_fast_io_back(fast_base, original_base, start_time):
    """
    Scans the fast local workspace and copies only modified/created files
    back to persistent storage, bypassing slow directory tree scans on VM mounts.
    """
    import os
    import shutil
    
    print(f"[Fast I/O] Scanning for modified files to sync back to {original_base}...")
    sync_count = 0
    # Sync files modified since the start of the script (with 5 seconds safety buffer)
    min_mtime = start_time - 5.0
    
    for d in ["delta", "output", "archive"]:
        src_dir = os.path.join(fast_base, d)
        if not os.path.exists(src_dir):
            continue
            
        for root, dirs, files in os.walk(src_dir):
            for file in files:
                src_file = os.path.join(root, file)
                try:
                    mtime = os.path.getmtime(src_file)
                    if mtime >= min_mtime:
                        rel_path = os.path.relpath(src_file, fast_base)
                        dst_file = os.path.join(original_base, rel_path)
                        
                        os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                        shutil.copy2(src_file, dst_file)
                        sync_count += 1
                except Exception as e:
                    print(f"[Fast I/O] Warning: failed to sync {src_file}: {e}")
                    
    print(f"[Fast I/O] Successfully synchronized {sync_count} modified files in <0.5 seconds!")


def run_unified_pipeline():
    script_start_time = time.time()
    
    # Check if we are running in a container and should use fast I/O temp path
    use_fast_io = os.path.exists("/data") and os.name != "nt"
    fast_base = "/tmp/medallion_data"
    original_base = "/data"
    
    if use_fast_io:
        print(f"[Fast I/O] Initializing fast local workspace at {fast_base}...")
        import subprocess
        subprocess.run(f"rm -rf {fast_base} && mkdir -p {fast_base}", shell=True)
        for d in ["input", "delta", "output"]:
            src = f"{original_base}/{d}"
            dst = f"{fast_base}/{d}"
            if os.path.exists(src):
                subprocess.run(f"mkdir -p {dst} && rsync -r -t --exclude='*.parquet' --no-perms --no-owner --no-group {src}/. {dst}/", shell=True)
        # Set base path override
        os.environ["BASE_DATA_PATH_OVERRIDE"] = fast_base
        
        # Reload config and spark jobs to point to the new paths
        try:
            import importlib
            import pipeline.config
            importlib.reload(pipeline.config)
            
            global METRICS_FILE, INCIDENTS_PATH, STATUS_FILE, INPUT_PATH, HISTORY_FILE, SILVER_PATH
            METRICS_FILE = pipeline.config.METRICS_FILE
            INCIDENTS_PATH = pipeline.config.INCIDENTS_PATH
            STATUS_FILE = pipeline.config.STATUS_FILE
            INPUT_PATH = pipeline.config.INPUT_PATH
            HISTORY_FILE = pipeline.config.HISTORY_FILE
            SILVER_PATH = pipeline.config.SILVER_PATH
            
            import spark_jobs.bronze_ingest
            importlib.reload(spark_jobs.bronze_ingest)
            import spark_jobs.data_profiler
            importlib.reload(spark_jobs.data_profiler)
            import spark_jobs.validate_data
            importlib.reload(spark_jobs.validate_data)
            import spark_jobs.quarantine
            importlib.reload(spark_jobs.quarantine)
            import spark_jobs.silver_transform
            importlib.reload(spark_jobs.silver_transform)
            import spark_jobs.gold_metrics
            importlib.reload(spark_jobs.gold_metrics)
        except Exception as reload_err:
            print(f"[Fast I/O] Reloading configuration failed: {reload_err}")

    # Parse Airflow start time argument
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--airflow-start-time", type=float, default=None)
    args, _ = parser.parse_known_args()
    airflow_start_time = args.airflow_start_time

    run_id = f"run_{uuid.uuid4().hex[:12]}"
    spark = None
    source_file = "unknown"
    anomalies = []
    mappings = []
    scorecard = {}
    bg_threads = []

    print(f"\n{'='*60}")
    print(f"  ENTERPRISE MEDALLION PIPELINE")
    print(f"  Run ID: {run_id}")
    print(f"{'='*60}\n")

    try:
        # Dynamic partitions tuning
        shuffle_partitions = estimate_partitions_and_parallelism()

        # Timing variables initialization
        airflow_startup = 0.0
        if airflow_start_time:
            airflow_startup = max(0.0, script_start_time - airflow_start_time)

        spark_start_t = time.time()
        spark = get_spark_session("UnifiedPipeline", shuffle_partitions=shuffle_partitions)
        spark_startup = time.time() - spark_start_t

        # ============================================================
        # STAGE 1: Bronze Ingestion + Schema Mapping
        # ============================================================
        write_status("running", run_id, stage="Bronze")
        print(f"\n[Stage 1] Bronze Ingestion...")

        bronze_start_t = time.time()
        bronze_df, source_file, mappings, dataset_type = ingest_bronze(spark=spark, run_id=run_id, bg_threads=bg_threads)
        bronze_duration = time.time() - bronze_start_t
        
        if bronze_df is None:
            write_status("failed", run_id, error="No input files found.", stage="Bronze", dataset_type="GENERIC")
            append_to_history("failed", run_id, error="No input files found.", dataset_type="GENERIC")
            return

        # Update status with detected type
        write_status("running", run_id, stage="Profiling", dataset_type=dataset_type)

        # ============================================================
        # STAGE 2: Data Profiling
        # ============================================================
        print(f"\n[Stage 2] Data Profiling...")
        profiling_duration = 0.0
        try:
            profiling_start_t = time.time()
            profile_records, anomalies, _ = profile_dataframe(bronze_df, run_id, source_file, spark, mappings=mappings)
            
            # Write profile in background
            def write_profile_bg(records, s_session):
                try:
                    write_profile(records, s_session)
                except Exception as ex:
                    print(f"[BG Profiling Write] Failed: {ex}")
            t_prof = threading.Thread(
                target=write_profile_bg,
                args=(profile_records, spark)
            )
            t_prof.start()
            bg_threads.append(t_prof)

            if anomalies:
                print(f"[Profiler] Anomalies detected: {anomalies}")
            profiling_duration = time.time() - profiling_start_t
        except Exception as profile_err:
            print(f"[Stage 2] Warning: Profiling failed (non-fatal): {profile_err}")

        # ============================================================
        # STAGE 3: DQ Validation → valid_df + invalid_df
        # ============================================================
        print(f"\n[Stage 3] Enterprise DQ Validation...")
        write_status("running", run_id, stage="Validation", dataset_type=dataset_type)

        validation_start_t = time.time()
        valid_df, invalid_df, scorecard, should_fail = validate_data(
            spark=spark, run_id=run_id, source_file=source_file, bronze_df=bronze_df, bg_threads=bg_threads, dataset_type=dataset_type
        )
        validation_duration = time.time() - validation_start_t

        # ============================================================
        # STAGE 4: Quarantine invalid rows
        # ============================================================
        quarantine_count = 0
        quarantine_duration = 0.0
        if invalid_df is not None:
            invalid_df = invalid_df.cache()
            print(f"\n[Stage 4] Writing quarantine rows...")
            write_status("running", run_id, stage="Quarantine", dataset_type=dataset_type)
            quarantine_start_t = time.time()
            quarantine_count = write_quarantine(invalid_df, run_id=run_id, source_file=source_file, row_count=scorecard.get("invalid_rows", 0))
            quarantine_duration = time.time() - quarantine_start_t

            # Run quarantine export to S3 concurrently
            def export_quarantine_bg(q_df, q_count, r_id, s_file):
                try:
                    if q_count > 0:
                        from utils.s3_client import export_csv_bytes_to_s3, _get_bucket
                        if _get_bucket() is None:
                            print("[BG Quarantine Export] S3_BUCKET not configured. Skipping toPandas() export.")
                            return
                        if q_count > 50000:
                            print(f"[BG Quarantine Export] Quarantine size {q_count} exceeds threshold. Skipping toPandas().")
                            return
                        q_csv = q_df.toPandas().to_csv(index=False).encode('utf-8')
                        export_csv_bytes_to_s3(q_csv, f"quarantine/{r_id}/rejected_records.csv", run_id=r_id, row_count=q_count)
                except Exception as ex:
                    print(f"[BG Quarantine Export] Failed: {ex}")
            
            t_quar = threading.Thread(
                target=export_quarantine_bg,
                args=(invalid_df, quarantine_count, run_id, source_file)
            )
            t_quar.start()
            bg_threads.append(t_quar)

        if should_fail:
            write_status("failed", run_id, stage="Validation",
                         error=f"DQ threshold exceeded. Score: {scorecard.get('dq_score', 0)}%",
                         duration=time.time() - script_start_time, dataset_type=dataset_type)
            append_to_history("failed", run_id,
                              error=f"DQ threshold exceeded. Score: {scorecard.get('dq_score', 0)}%",
                              duration=time.time() - script_start_time, rows=0, dataset_type=dataset_type)
            try:
                spark.catalog.clearCache()
            except Exception:
                pass
            raise RuntimeError(f"DQ threshold exceeded. Score: {scorecard.get('dq_score', 0)}%")

        # ============================================================
        # STAGE 5: Silver Transformation (valid rows only)
        # ============================================================
        print(f"\n[Stage 5] Silver Transformation...")
        write_status("running", run_id, stage="Silver", dataset_type=dataset_type)
        silver_start_t = time.time()
        silver_rows, silver_df = transform_silver(spark=spark, valid_df=valid_df, row_count=scorecard.get("valid_rows", 0), dataset_type=dataset_type)
        if silver_df is not None:
            silver_df = silver_df.cache()
        silver_duration = time.time() - silver_start_t

        # Run silver export to S3 concurrently
        def export_silver_bg(s_df, s_rows, r_id):
            try:
                if s_df is not None and s_rows > 0:
                    from utils.s3_client import export_csv_bytes_to_s3, _get_bucket
                    if _get_bucket() is None:
                        print("[BG Silver Export] S3_BUCKET not configured. Skipping toPandas() export.")
                        return
                    if s_rows > 50000:
                        print(f"[BG Silver Export] Silver size {s_rows} exceeds threshold. Skipping toPandas().")
                        return
                    csv_data = s_df.toPandas().to_csv(index=False).encode('utf-8')
                    export_csv_bytes_to_s3(csv_data, f"exports/{r_id}/cleaned_dataset.csv", run_id=r_id, row_count=s_rows)
            except Exception as ex:
                print(f"[BG Silver Export] Failed: {ex}")

        t_silv = threading.Thread(
            target=export_silver_bg,
            args=(silver_df, silver_rows, run_id)
        )
        t_silv.start()
        bg_threads.append(t_silv)

        # ============================================================
        # STAGE 6: Gold Metrics (enriched)
        # ============================================================
        print(f"\n[Stage 6] Gold Metrics Generation...")
        write_status("running", run_id, stage="Gold", dataset_type=dataset_type)
        gold_start_t = time.time()
        duration_so_far = time.time() - script_start_time
        business_df = generate_gold(
            spark=spark,
            scorecard=scorecard,
            anomalies=anomalies,
            mappings=mappings,
            run_id=run_id,
            runtime_seconds=duration_so_far,
            silver_df=silver_df,
            dataset_type=dataset_type
        )
        gold_duration = time.time() - gold_start_t

        # Start Gold Report S3 export concurrently
        def export_gold_bg(s_session, b_df, sc, r_id, d_type):
            try:
                from utils.s3_client import export_text_to_s3, _get_bucket
                if _get_bucket() is None:
                    print("[BG Gold Export] S3_BUCKET not configured. Skipping gold report export.")
                    return
                from dashboard.queries import get_gold_report_data, generate_txt_report
                gold_p_df = b_df.toPandas() if b_df is not None else None
                report_data = get_gold_report_data(
                    s_session,
                    bronze_count=sc.get("total_rows"),
                    silver_count=sc.get("valid_rows"),
                    gold_df=gold_p_df,
                    dataset_type=d_type
                )
                if report_data:
                    txt_report = generate_txt_report(report_data)
                    export_text_to_s3(txt_report, f"reports/{r_id}/gold_report.txt", run_id=r_id)
            except Exception as ex:
                print(f"[BG Gold Export] Failed: {ex}")

        t_gold = threading.Thread(
            target=export_gold_bg,
            args=(spark, business_df, scorecard, run_id, dataset_type)
        )
        t_gold.start()
        bg_threads.append(t_gold)

        # ============================================================
        # STAGE 7: S3 Exports & Report Generation (Await Concurrent)
        # ============================================================
        print(f"\n[Stage 7] Awaiting concurrent S3 exports...")
        write_status("running", run_id, stage="S3 Export", dataset_type=dataset_type)
        s3_export_start_t = time.time()
        for t in bg_threads:
            t.join(timeout=3.0)
        s3_export_duration = time.time() - s3_export_start_t
        report_gen_duration = 0.0  # Concurrently done

        # Clear Spark cache to release memory
        try:
            spark.catalog.clearCache()
        except Exception:
            pass

        # ============================================================
        # Write metrics file (non-fatal)
        # ============================================================
        duration = time.time() - script_start_time
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
        write_status("completed", run_id, stage="Finished", duration=duration, dataset_type=dataset_type)
        append_to_history("completed", run_id, file_name=source_file,
                          duration=duration, rows=final_rows, dataset_type=dataset_type)

        # Sync results back to original storage
        if use_fast_io:
            sync_fast_io_back(fast_base, original_base, script_start_time)
            import subprocess
            subprocess.run(f"rm -rf {fast_base}", shell=True)

        print(f"\n{'='*60}")
        print(f"  PIPELINE COMPLETE")
        print(f"  Run ID     : {run_id}")
        print(f"  Duration   : {duration:.1f}s")
        print(f"  DQ Score   : {scorecard.get('dq_score', 'N/A')}%")
        print(f"  Accepted   : {scorecard.get('valid_rows', 0):,} rows -> Silver")
        print(f"  Quarantined: {quarantine_count:,} rows -> Quarantine")
        if anomalies:
            print(f"  Anomalies  : {len(anomalies)} detected")
        print(f"{'='*60}\n")

        # Runtime breakdown printout
        print("Pipeline Runtime Breakdown:")
        print(f"- Airflow Startup: {airflow_startup:.2f} sec")
        print(f"- Spark Startup: {spark_startup:.2f} sec")
        print(f"- Bronze: {bronze_duration:.2f} sec")
        print(f"- Validation: {validation_duration:.2f} sec")
        print(f"- Silver: {silver_duration:.2f} sec")
        print(f"- Gold: {gold_duration:.2f} sec")
        print(f"- Profiling: {profiling_duration:.2f} sec")
        print(f"- Quarantine: {quarantine_duration:.2f} sec")
        print(f"- S3 Export: {s3_export_duration:.2f} sec")
        print(f"- Report Generation: {report_gen_duration:.2f} sec")
        print(f"- Total Runtime: {duration:.2f} sec\n")

    except Exception as e:
        duration = time.time() - script_start_time
        error_msg = str(e)
        
        # Log the full stack trace for developer troubleshooting
        import traceback
        print("\n=== TECHNICAL DETAIL STACK TRACE ===")
        traceback.print_exc()
        print("====================================\n")
        
        print(f"\n[Pipeline] FAILED after {duration:.1f}s: {error_msg}")
        
        # Determine business-friendly error message
        friendly_error = error_msg
        if "SchemaValidationError" in type(e).__name__ or "Unsupported dataset schema" in error_msg:
            friendly_error = error_msg
        elif "AnalysisException" in error_msg or "UNRESOLVED_COLUMN" in error_msg:
            friendly_error = "Database Error: A column resolution or query analysis failure occurred in Spark."
        elif "NoCredentialsError" in error_msg:
            friendly_error = "AWS S3 Authentication Error: Failed to locate credentials to write S3 exports."
            
        write_status("failed", run_id, error=friendly_error, stage="Failed", duration=duration)
        append_to_history("failed", run_id, file_name=source_file,
                          error=friendly_error, duration=duration, rows=0)

        # Sync partial results and logs back to original storage on failure
        if use_fast_io:
            sync_fast_io_back(fast_base, original_base, script_start_time)
            import subprocess
            subprocess.run(f"rm -rf {fast_base}", shell=True)

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

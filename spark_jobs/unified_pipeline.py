import sys
import time
import os
import json
from pipeline.config import METRICS_FILE, INCIDENTS_PATH, STATUS_FILE, INPUT_PATH, HISTORY_FILE, SILVER_PATH
from pipeline.delta_utils import get_spark_session
from spark_jobs.bronze_ingest import ingest_bronze
from spark_jobs.validate_data import validate_data
from spark_jobs.silver_transform import transform_silver
from spark_jobs.gold_metrics import generate_gold
from monitoring.log_incident import log_incident


def write_status(status, run_id, file_name=None, error=None, duration=None, stage=None):
    # Detect file in input if not passed
    if not file_name and os.path.exists(INPUT_PATH):
        try:
            files = [f for f in os.listdir(INPUT_PATH) if f.endswith(('.csv', '.json'))]
            if files:
                files.sort(key=lambda x: os.path.getmtime(os.path.join(INPUT_PATH, x)), reverse=True)
                file_name = files[0]
        except:
            pass
            
    # Fallback to existing file_name in status file if missing
    if not file_name and os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r") as f:
                old_status = json.load(f)
                file_name = old_status.get("file_name")
        except:
            pass
            
    status_data = {
        "status": status,
        "run_id": run_id,
        "timestamp": time.time(),
        "file_name": file_name or "Unknown",
        "stage": stage or "Waiting",
        "error": error,
        "duration": f"{duration:.2f}" if duration is not None else None
    }
    try:
        from pathlib import Path
        Path(STATUS_FILE).parent.mkdir(parents=True, exist_ok=True)
        tmp_file = f"{STATUS_FILE}.tmp"
        with open(tmp_file, "w") as f:
            json.dump(status_data, f, indent=4)
        os.replace(tmp_file, STATUS_FILE)
    except Exception as e:
        print(f"Error writing status file: {str(e)}")

def append_to_history(status, run_id, file_name=None, error=None, duration=None, rows=0):
    # Detect file in input if not passed
    if not file_name and os.path.exists(INPUT_PATH):
        try:
            files = [f for f in os.listdir(INPUT_PATH) if f.endswith(('.csv', '.json'))]
            if files:
                files.sort(key=lambda x: os.path.getmtime(os.path.join(INPUT_PATH, x)), reverse=True)
                file_name = files[0]
        except:
            pass
    if not file_name and os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r") as f:
                old_status = json.load(f)
                file_name = old_status.get("file_name")
        except:
            pass
            
    try:
        from pathlib import Path
        Path(HISTORY_FILE).parent.mkdir(parents=True, exist_ok=True)
        record = {
            "timestamp": time.time(),
            "run_id": run_id,
            "file_name": file_name or "Unknown",
            "status": status,
            "duration": f"{duration:.2f}" if duration is not None else "N/A",
            "rows": rows,
            "error": error
        }
        tmp_file = f"{HISTORY_FILE}.tmp"
        existing_content = ""
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, "r") as f:
                    existing_content = f.read()
            except:
                pass
        with open(tmp_file, "w") as f:
            f.write(existing_content + json.dumps(record) + "\n")
        os.replace(tmp_file, HISTORY_FILE)
    except Exception as e:
        print(f"Error writing history file: {str(e)}")


def run_unified_pipeline():
    start_time = time.time()
    spark = get_spark_session("UnifiedMedallionPipeline")
    run_id = f"run_{int(start_time)}"
    current_stage = "Bronze"
    
    try:
        print("--- Starting Unified Medallion Pipeline ---")
        write_status("running", run_id, stage="Bronze")
        
        # 1. Bronze Ingestion
        print("\n[Layer 1: Bronze]")
        ingest_bronze(spark=spark)
        
        # 2. Data Quality Validation (Blocker)
        print("\n[Layer 1.5: Data Quality Validation]")
        current_stage = "Validation"
        write_status("running", run_id, stage="Validation")
        dq_pass = validate_data(spark=spark)
        if not dq_pass:
            error_msg = "Pipeline halted due to critical Data Quality issues in Bronze."
            print(f"\nSTOP: {error_msg}")
            log_incident("unified_pipeline", run_id, "validation_step", error_msg, "CRITICAL")
            write_status("failed", run_id, error=error_msg, stage="Validation")
            append_to_history("failed", run_id, file_name=None, error=error_msg, duration=time.time()-start_time, rows=0)
            return
        
        # 3. Silver Transformation
        print("\n[Layer 2: Silver]")
        current_stage = "Silver"
        write_status("running", run_id, stage="Silver")
        transform_silver(spark=spark)
        
        # 4. Gold Metrics Generation
        print("\n[Layer 3: Gold]")
        current_stage = "Gold"
        write_status("running", run_id, stage="Gold")
        generate_gold(spark=spark)
        
        duration = time.time() - start_time
        print(f"\n--- Unified Pipeline Success: {duration:.2f}s ---")
        
        # Save metrics for dashboard
        try:
            from pathlib import Path
            import logging
            logger = logging.getLogger("UnifiedMedallionPipeline")
            
            Path(METRICS_FILE).parent.mkdir(parents=True, exist_ok=True)
            tmp_file = f"{METRICS_FILE}.tmp"
            with open(tmp_file, "w") as f:
                f.write(f"{time.time()},{duration:.2f}\n")
            os.replace(tmp_file, METRICS_FILE)
        except Exception as e:
            import logging
            logger = logging.getLogger("UnifiedMedallionPipeline")
            logger.error(f"Failed to persist metrics: {str(e)}")
            
        rows_processed = 0
        try:
            if os.path.exists(SILVER_PATH):
                rows_processed = spark.read.format("delta").load(SILVER_PATH).count()
        except:
            pass

        write_status("completed", run_id, duration=duration, stage="Finished")
        append_to_history("completed", run_id, file_name=None, error=None, duration=duration, rows=rows_processed)
            
    except Exception as e:
        error_msg = str(e)
        duration = time.time() - start_time
        print(f"\n--- Unified Pipeline Failed: {error_msg} ---")
        log_incident("unified_pipeline", run_id, "main_process", error_msg, "ERROR")
        write_status("failed", run_id, error=error_msg, stage=current_stage)
        append_to_history("failed", run_id, file_name=None, error=error_msg, duration=duration, rows=0)
        sys.exit(1)

    finally:
        spark.stop()



if __name__ == "__main__":
    run_unified_pipeline()

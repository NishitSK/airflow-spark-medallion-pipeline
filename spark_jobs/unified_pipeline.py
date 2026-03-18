import sys
import time
import os
from pipeline.config import METRICS_FILE, INCIDENTS_PATH
from pipeline.delta_utils import get_spark_session
from spark_jobs.bronze_ingest import ingest_bronze
from spark_jobs.validate_data import validate_data
from spark_jobs.silver_transform import transform_silver
from spark_jobs.gold_metrics import generate_gold
from monitoring.log_incident import log_incident

def run_unified_pipeline():
    start_time = time.time()
    spark = get_spark_session("UnifiedMedallionPipeline")
    run_id = f"run_{int(start_time)}"
    
    try:
        print("--- Starting Unified Medallion Pipeline ---")
        
        # 1. Bronze Ingestion
        print("\n[Layer 1: Bronze]")
        ingest_bronze(spark=spark)
        
        # 2. Data Quality Validation (Blocker)
        print("\n[Layer 1.5: Data Quality Validation]")
        dq_pass = validate_data(spark=spark)
        if not dq_pass:
            error_msg = "Pipeline halted due to critical Data Quality issues in Bronze."
            print(f"\nSTOP: {error_msg}")
            log_incident("unified_pipeline", run_id, "validation_step", error_msg, "CRITICAL")
            return
        
        # 3. Silver Transformation
        print("\n[Layer 2: Silver]")
        transform_silver(spark=spark)
        
        # 4. Gold Metrics Generation
        print("\n[Layer 3: Gold]")
        generate_gold(spark=spark)
        
        duration = time.time() - start_time
        print(f"\n--- Unified Pipeline Success: {duration:.2f}s ---")
        
        # Save metrics for dashboard
        os.makedirs(os.path.dirname(METRICS_FILE), exist_ok=True)
        with open(METRICS_FILE, "w") as f:
            f.write(f"{time.time()},{duration:.2f}\n")
            
    except Exception as e:
        error_msg = str(e)
        print(f"\n--- Unified Pipeline Failed: {error_msg} ---")
        log_incident("unified_pipeline", run_id, "main_process", error_msg, "ERROR")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    run_unified_pipeline()

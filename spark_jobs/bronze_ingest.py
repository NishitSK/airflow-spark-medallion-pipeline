"""
Bronze Ingestion
================
Reads raw CSV/JSON files, detects schema type, applies schema mapping to handle column aliases,
adds metadata, and writes to the Bronze Delta table in raw string format.
"""
import sys
import os
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType
from pipeline.config import BRONZE_PATH, INPUT_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, write_delta


def ingest_bronze(spark=None, run_id="unknown", bg_threads=None):
    own_spark = False
    if spark is None:
        spark = get_spark_session("BronzeIngestion")
        own_spark = True

    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

    # Read files with no schema enforcement — accept anything
    try:
        mappings = []
        df_csv = None
        df_json = None

        csv_files = [f for f in os.listdir(INPUT_PATH) if f.endswith('.csv')] if os.path.exists(INPUT_PATH) else []
        json_files = [f for f in os.listdir(INPUT_PATH) if f.endswith('.json')] if os.path.exists(INPUT_PATH) else []

        # Ingest timing
        import time
        bronze_t0 = time.time()

        if csv_files:
            try:
                df_csv = spark.read.option("header", "true").option("inferSchema", "false").csv(
                    f"{INPUT_PATH}/*.csv"
                )
            except Exception as e:
                print(f"[Bronze] Warning reading CSV: {e}")

        if json_files:
            try:
                df_json = spark.read.option("multiline", "false").json(
                    f"{INPUT_PATH}/*.json"
                )
                # Cast all fields to string for consistency
                for field in df_json.schema.fields:
                    df_json = df_json.withColumn(field.name, df_json[field.name].cast("string"))
            except Exception as e:
                print(f"[Bronze] Warning reading JSON: {e}")

        if df_csv is not None and df_json is not None:
            # Align schemas by adding missing columns as nulls
            csv_cols = set(df_csv.columns)
            json_cols = set(df_json.columns)
            from pyspark.sql.functions import lit
            for c in json_cols - csv_cols:
                df_csv = df_csv.withColumn(c, lit(None).cast("string"))
            for c in csv_cols - json_cols:
                df_json = df_json.withColumn(c, lit(None).cast("string"))
            df = df_csv.unionByName(df_json)
        elif df_csv is not None:
            df = df_csv
        elif df_json is not None:
            df = df_json
        else:
            print("[Bronze] No CSV or JSON files found in input path.")
            return None, "unknown", [], "GENERIC"

        print(f"[Bronze Timing] Reading files took: {time.time() - bronze_t0:.2f} seconds")

        # Add ingestion metadata
        df = df \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("source_file", input_file_name())

        # Detect dataset type from raw columns
        from pipeline.schema_validator import detect_dataset_type, validate_schema
        dataset_type = detect_dataset_type(df)
        print(f"[Bronze] Detected dataset type: {dataset_type}")

        # Apply schema mapping (alias resolution) based on type
        map_t0 = time.time()
        try:
            from pipeline.schema_mapper import apply_schema_mapping
            source_file = csv_files[0] if csv_files else (json_files[0] if json_files else "unknown")
            df, mappings, unresolved = apply_schema_mapping(df, dataset_type, run_id=run_id, source_file=source_file)
            if mappings:
                print(f"[Bronze] Schema mappings applied: {mappings}")
            if unresolved:
                print(f"[Bronze] Warning — unresolved columns (kept as-is): {unresolved}")
        except Exception as schema_err:
            print(f"[Bronze] Warning: Schema mapping failed: {schema_err}")
        print(f"[Bronze Timing] Schema mapping took: {time.time() - map_t0:.2f} seconds")

        # Validate schema after mapping aliases
        validate_schema(df, dataset_type, source_file=source_file)

        write_t0 = time.time()
        import threading
        def write_bronze_bg():
            try:
                write_delta(df, BRONZE_PATH, mode="overwrite")
                print(f"[BG Bronze Write] Delta table overwrite complete.")
            except Exception as e:
                print(f"[BG Bronze Write] Error: {e}")
                
        t_bronze = threading.Thread(target=write_bronze_bg)
        t_bronze.start()
        if bg_threads is not None:
            bg_threads.append(t_bronze)
        else:
            t_bronze.join()
            
        source_file = csv_files[0] if csv_files else (json_files[0] if json_files else "unknown")
        print(f"[Bronze Timing] Launched Bronze Delta write in background | Source: {source_file}")
        return df, source_file, mappings, dataset_type

    except Exception as e:
        print(f"[Bronze] Ingestion Failed: {str(e)}")
        if own_spark:
            sys.exit(1)
        raise e
    finally:
        if own_spark:
            spark.stop()


if __name__ == "__main__":
    ingest_bronze()

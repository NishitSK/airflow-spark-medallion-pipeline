"""
Writes the full detailed PROJECT_DOCUMENTATION.md into the project repo.
Run from: airflow-spark-medallion-pipeline/
"""
import os

DOC = r"""# Airflow · Spark · Delta Lake — Enterprise Medallion Pipeline
## Complete Technical Documentation

> **Repository:** `airflow-spark-medallion-pipeline` · **Branch:** `main`
> **Stack:** Apache Airflow 3.1.8 · Apache Spark 4.0.0 · Delta Lake 4.1.0 · PySpark 4.1.1 · Streamlit · Docker · AWS S3
> **Report Date:** July 12, 2026

---

## Table of Contents

1. [Project Purpose & Scope](#1-project-purpose--scope)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Infrastructure & Docker](#3-infrastructure--docker)
4. [Directory Structure](#4-directory-structure)
5. [Configuration Layer](#5-configuration-layer)
6. [Schema Framework](#6-schema-framework)
7. [Pipeline — Stage by Stage](#7-pipeline--stage-by-stage)
8. [Data Quality Engine — Deep Dive](#8-data-quality-engine--deep-dive)
9. [Airflow Orchestration](#9-airflow-orchestration)
10. [Streamlit Dashboard](#10-streamlit-dashboard)
11. [S3 Shadow Storage](#11-s3-shadow-storage)
12. [Performance Engineering](#12-performance-engineering)
13. [Delta Lake Internals](#13-delta-lake-internals)
14. [Status & History Files](#14-status--history-files)
15. [Error Handling Strategy](#15-error-handling-strategy)
16. [Git Evolution — Commit-by-Commit](#16-git-evolution--commit-by-commit)
17. [Test Data](#17-test-data)
18. [File Reference — Every File Explained](#18-file-reference--every-file-explained)
19. [Spark Session Configuration](#19-spark-session-configuration)
20. [Known Limitations & Future Scope](#20-known-limitations--future-scope)

---

## 1. Project Purpose & Scope

This platform is an **enterprise-grade, self-service data engineering system** built on the Medallion Architecture (Bronze → Silver → Gold). Users upload arbitrary CSV or JSON files through a web UI; the platform automatically detects the schema, validates data quality, cleans the data, generates business analytics, and exposes everything through a rich monitoring dashboard.

### Core Problems Solved

| Problem | Solution |
|---|---|
| Spark crashes with `UNRESOLVED_COLUMN` on unfamiliar CSVs | Schema detection + alias mapping before Spark processing |
| Different column names for same concept (`user_id` vs `id`) | Schema Mapper resolves aliases to canonical names |
| No visibility into what was rejected | Quarantine table stores every rejected row with reason |
| Unsupported schemas fail the pipeline | GENERIC mode accepts *any* file |
| Stack traces shown to users | Friendly error messages at every layer |
| Slow pipeline for large files | Dynamic partitions + concurrent background threads |
| No data lineage | Delta Lake transaction log + Lineage dashboard page |
| No DQ history | Append-only DQ report and run history files |

### Supported Dataset Modes

| Mode | Detection Columns | Special Processing |
|---|---|---|
| **CUSTOMER** | `id`, `name`, `age` (or aliases) | Age range validation, name format, ID deduplication |
| **ORDERS** | `order_id`, `product_name`, `quantity`, `unit_price` (or aliases) | Revenue calculation, date validation, quantity checks |
| **GENERIC** | Any other schema | Single-pass null/completeness/duplicate metrics only |

---

## 2. High-Level Architecture

```
+----------------------------------------------------------+
|                 STREAMLIT (port 8501)                    |
|  Upload → Trigger → Monitor → DQ → Profiling → Gold     |
+----------------------------+-----------------------------+
                             | Airflow REST API (JWT)
                             v
+----------------------------------------------------------+
|              APACHE AIRFLOW 3.1.8 (port 8081)            |
|  DAG: file_trigger_pipeline                              |
|  Task 1: unified_medallion_pipeline (BashOperator)       |
|  Task 2: archive_file (BashOperator)                     |
+----------------------------+-----------------------------+
                             | python unified_pipeline.py
                             v
+----------------------------------------------------------+
|           MEDALLION PIPELINE (PySpark + Delta Lake)      |
|                                                          |
|  Stage 0: Dynamic Partition Tuning (pre-Spark, Python)  |
|      |                                                   |
|  Stage 1: Bronze Ingestion                              |
|      | -> Schema Detect (O(cols)) -> Alias Map -> Validate
|      |                                                   |
|  Stage 2: Data Profiling [background thread]            |
|      |                                                   |
|  Stage 3: DQ Engine                                     |
|      |---------------|                                   |
|   valid_df       invalid_df                              |
|      |               |                                   |
|      |       Stage 4: Quarantine [+ S3 bg thread]       |
|      |                                                   |
|  Stage 5: Silver Transform [+ S3 bg thread]             |
|      |                                                   |
|  Stage 6: Gold Metrics [+ S3 bg thread]                 |
|      |                                                   |
|  Stage 7: Await bg threads (3s timeout)                 |
|      |                                                   |
|  Write status.json + history.jsonl + metrics.txt        |
+----------------------------------------------------------+
                             |
                             v
+----------------------------------------------------------+
|           DELTA LAKE STORAGE  (/data/delta/)             |
|  bronze/    silver/    gold/    quarantine/               |
|  data_profile/   dq_run_report/   schema_mapping_log/    |
|  dq_metrics/     pipeline_trace/  incidents/              |
+----------------------------------------------------------+
                             |
                    optional v
+----------------------------------------------------------+
|              AWS S3 SHADOW STORAGE                       |
|  quarantine/{run_id}/rejected_records.csv               |
|  exports/{run_id}/cleaned_dataset.csv                   |
|  reports/{run_id}/gold_report.txt                       |
+----------------------------------------------------------+
```

---

## 3. Infrastructure & Docker

### Dockerfile (`docker/Dockerfile`)

```dockerfile
FROM apache/airflow:3.1.8

USER root
RUN apt-get update && apt-get install -y openjdk-17-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install pyspark==4.1.1 delta-spark==4.2.0 streamlit plotly \
    pandas pendulum fpdf2 pyyaml boto3

# Pre-download Delta JARs at build time -> no runtime Maven downloads
RUN python -c "
import os, pyspark, glob, shutil
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages io.delta:delta-spark_2.13:4.1.0 pyspark-shell'
from pyspark.sql import SparkSession
SparkSession.builder.getOrCreate().stop()
p = os.path.join(os.path.dirname(pyspark.__file__), 'jars')
[shutil.copy(j, p) for j in glob.glob(os.path.expanduser('~/.ivy2*/jars/*.jar'))]
shutil.rmtree(os.path.expanduser('~/.ivy2*'), ignore_errors=True)"
```

**Key decisions:**
- Java 17 required (Spark 4.0 drops Java 8/11 support)
- JARs pre-downloaded during build so container startup is instant
- Single image used for both Airflow and Streamlit services (deduplicates build)

---

### docker-compose.yml (`docker/docker-compose.yml`)

| Service | Image | Internal Port | External Port | User |
|---|---|---|---|---|
| `spark` | `apache/spark:4.0.0` | 8080 | 8080 | 50000:0 |
| `airflow` | `airflow-custom:latest` | 8080 | 8081 | 50000:0 |
| `streamlit` | `airflow-custom:latest` | 8501 | 8501 | 50000:0 |

**All services run as `UID 50000`** to match Airflow's default container user — ensures Docker volume files are readable/writable by all three services without permission conflicts.

#### Shared Volume: `/data`

```yaml
volumes:
  - ../data:/data   # shared by all 3 services
```

All Delta tables, input files, output JSONs live under `/data` on the container, mapped to `../data` on the host. Every service can read and write.

#### Startup sequence:

**Airflow:**
```bash
python3 -c "import pipeline.config"        # Creates dirs, validates writability
python3 -c "json.dump({user: pass}, open('/opt/airflow/simple_auth_manager_passwords.json.generated','w'))"
exec airflow standalone                    # Starts scheduler + webserver
```

**Streamlit:**
```bash
python3 -c "import pipeline.config"        # Same dir validation
exec streamlit run /app/app.py --server.port=8501 --server.address=0.0.0.0
```

### Environment Variables (`.env`)

```ini
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin123
AIRFLOW_FERNET_KEY=<base64-fernet-key>

# Optional - leave blank to disable S3 exports
S3_BUCKET=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=us-east-1
```

---

## 4. Directory Structure

```
airflow-spark-medallion-pipeline/
|
+-- docker/
|   +-- Dockerfile                  (831 B)  Custom image: Airflow + Java 17 + PySpark
|   +-- docker-compose.yml         (2.0 KB)  3-service orchestration
|
+-- dags/
|   +-- spark_pipeline.py           (873 B)  Simple manual DAG
|   +-- file_trigger_pipeline.py   (1.9 KB)  Main DAG with failure callback
|
+-- spark_jobs/                              All Spark/pipeline business logic
|   +-- unified_pipeline.py        (26 KB)  MAIN ENTRY POINT - 613 lines
|   +-- bronze_ingest.py            (5.3 KB) Stage 1: ingest + detect + map + validate
|   +-- data_profiler.py           (12.9 KB) Stage 2: column stats + anomaly detection
|   +-- validate_data.py            (4.0 KB) Stage 3 wrapper + scorecard builder
|   +-- dq_engine.py               (17.4 KB) Core DQ rules CUSTOMER/ORDERS/GENERIC
|   +-- quarantine.py               (2.4 KB) Stage 4: write invalid rows
|   +-- silver_transform.py         (6.9 KB) Stage 5: type-cast + clean valid rows
|   +-- gold_metrics.py             (6.0 KB) Stage 6: business KPIs per schema type
|   +-- time_travel_demo.py         (1.6 KB) Delta time travel demo
|
+-- pipeline/                                Config, schema, Delta utilities
|   +-- config.py                   (5.3 KB) All paths, Spark config, startup validation
|   +-- dq_config.yaml              (2.6 KB) DQ rules (editable without code changes)
|   +-- schema_validator.py         (5.5 KB) detect_dataset_type() + validate_schema()
|   +-- schema_mapper.py            (4.7 KB) apply_schema_mapping() - alias resolution
|   +-- delta_utils.py              (1.9 KB) SparkSession + write_delta + read_delta
|   +-- custom_auth_manager.py      (2.3 KB) Airflow 3 custom auth override
|   +-- validate_auth.py            (2.8 KB) Auth validation helper
|
+-- dashboard/
|   +-- app.py                     (79.5 KB) Streamlit 9-page UI - 1607 lines
|   +-- queries.py                 (55.6 KB) All Delta read queries + report generation
|   +-- charts.py                   (8.0 KB) Plotly chart builders
|   +-- airflow_client.py          (12.0 KB) Airflow REST API client (JWT auth)
|   +-- logo.png                  (445.6 KB) Dashboard branding
|
+-- utils/
|   +-- s3_client.py               (10.4 KB) AWS S3 upload functions
|
+-- monitoring/
|   +-- log_incident.py                      Incident logger -> Delta incidents table
|
+-- data/
|   +-- input/                               Drop CSV/JSON files here to process
|   +-- archive/                             Processed files moved here post-success
|   +-- samples/                             Pre-built test datasets
|   |   +-- small_sample.csv     (2.1 KB)   ~100 rows CUSTOMER
|   |   +-- medium_sample.csv  (194.0 KB)   ~2K rows CUSTOMER
|   |   +-- large_sample.csv     (2.1 MB)   ~20K rows CUSTOMER
|   |   +-- small_sample.json    (4.3 KB)   ~50 rows CUSTOMER
|   |   +-- medium_sample.json (420.0 KB)   ~2K rows CUSTOMER
|   +-- delta/                               All Delta Lake tables (Parquet + _delta_log)
|   |   +-- bronze/              Raw ingested data
|   |   +-- silver/              Cleaned, typed, valid rows
|   |   +-- gold/                Business KPI aggregations
|   |   +-- quarantine/          Rejected rows with violation tags
|   |   +-- data_profile/        Column-level statistics per run
|   |   +-- dq_metrics/          DQ scorecard per run
|   |   +-- dq_run_report/       Full enriched run summary
|   |   +-- schema_mapping_log/  Alias resolution audit log
|   |   +-- incidents/           Pipeline failure incident log
|   |   +-- pipeline_trace/      Execution trace data
|   +-- output/
|       +-- pipeline_status.json       Live status (polled by dashboard)
|       +-- pipeline_history.jsonl     Append-only run log
|       +-- pipeline_metrics.txt       Last run timing
|       +-- schema_compatibility_report.json  Schema check output
|
+-- scratch/                                 Development scripts (not production)
|   +-- generate_orders_data.py    Generator for 1M/10M ORDERS CSV files
|   +-- test_matrix_runner.py      10-scenario automated test runner
|   +-- test_pipeline_robustness.py Robustness test cases
|   +-- test_error_parsing.py      Error message parsing tests
|
+-- .env                                     Secrets (not committed to git)
+-- .gitignore
+-- requirements.txt               (65 B)   Python deps
+-- airflow.cfg                   (84.4 KB) Full Airflow configuration
+-- README.md                      (3.9 KB) Project overview
+-- PROJECT_DOCUMENTATION.md                This file
```

---

## 5. Configuration Layer

### `pipeline/config.py` — 154 lines

The **central configuration module**. Imported at startup by every other module. Runs directory setup and writability checks automatically on import.

#### Path Resolution Logic

```python
BASE_DATA_PATH = os.environ.get("BASE_DATA_PATH_OVERRIDE")
if not BASE_DATA_PATH:
    if os.path.exists("/data") and os.name != "nt":
        BASE_DATA_PATH = "/data"          # Docker container
    else:
        BASE_DATA_PATH = ".../data"       # Local Windows fallback
```

This means the same code runs correctly in Docker, local Linux, and local Windows.

#### All Derived Paths

| Variable | Resolved Path |
|---|---|
| `INPUT_PATH` | `{BASE}/input` |
| `ARCHIVE_PATH` | `{BASE}/archive` |
| `DELTA_PATH` | `{BASE}/delta` |
| `BRONZE_PATH` | `{BASE}/delta/bronze` |
| `SILVER_PATH` | `{BASE}/delta/silver` |
| `GOLD_PATH` | `{BASE}/delta/gold` |
| `QUARANTINE_PATH` | `{BASE}/delta/quarantine` |
| `PROFILE_PATH` | `{BASE}/delta/data_profile` |
| `DQ_METRICS_PATH` | `{BASE}/delta/dq_metrics` |
| `DQ_REPORT_PATH` | `{BASE}/delta/dq_run_report` |
| `SCHEMA_MAP_LOG_PATH` | `{BASE}/delta/schema_mapping_log` |
| `INCIDENTS_PATH` | `{BASE}/delta/incidents` |
| `TRACE_PATH` | `{BASE}/delta/pipeline_trace` |
| `STATUS_FILE` | `{BASE}/output/pipeline_status.json` |
| `HISTORY_FILE` | `{BASE}/output/pipeline_history.jsonl` |
| `METRICS_FILE` | `{BASE}/output/pipeline_metrics.txt` |

#### Startup Validation (skipped if `SKIP_DIAGNOSTICS=1`)

For each required path:
1. `Path.mkdir(parents=True, exist_ok=True)` — create if missing
2. `path.stat()` — read UID, GID, permissions
3. Touch a temp file to test writability
4. Log all diagnostics to stdout
5. If not writable and inside Docker → `sys.exit(1)` (fail fast)

This catches volume permission issues at startup rather than mid-pipeline.

---

### `pipeline/dq_config.yaml` — 91 lines

All DQ rules in YAML. **Editable without any code changes**.

```yaml
schema_aliases:           # Column alias resolution (CUSTOMER mode)
  id:   [id, ID, user_id, customer_id, record_id, uid, userId, customerId]
  name: [name, full_name, customer_name, user_name, fullname, displayName, display_name]
  age:  [age, customer_age, user_age, years_old, Age]

thresholds:               # When to fail the pipeline
  max_null_id_pct: 0.50   # Fail if >50% of IDs are null
  max_invalid_pct: 0.80   # Warn if >80% of any column has violations
  min_row_count: 1        # Fail if dataset is empty

columns:                  # Per-column validation rules (CUSTOMER)
  id:
    required: true
    type: integer
    allow_null: false
    allow_duplicate: false
  name:
    required: true
    type: string
    allow_null: true      # Nulls allowed but flagged
    min_length: 1
    max_length: 200
  age:
    required: true
    type: integer
    allow_null: false
    min_value: 0
    max_value: 120
    null_impute_value: 45 # Impute missing ages with 45

cleaning:                 # Column transformation rules
  name:
    trim_whitespace: true
    title_case: true      # "john doe" -> "John Doe"
  id:
    strip_decimal_zeros: true   # "1001.0" -> "1001"
  age:
    strip_decimal_zeros: true
    impute_nulls: true

anomaly_detection:        # Compare against previous run
  null_spike_threshold: 0.20    # Alert if null% jumps >20 percentage points
  volume_drop_threshold: 0.30   # Alert if row count drops >30%
  volume_spike_threshold: 2.00  # Alert if row count >2x previous run
  outlier_stddev: 3.0           # Flag values outside mean +/- 3 standard deviations
```

---

## 6. Schema Framework

Three components work together:

| Component | File | Role |
|---|---|---|
| Schema Validator | `pipeline/schema_validator.py` | Detect type, validate required columns |
| Schema Mapper | `pipeline/schema_mapper.py` | Resolve column aliases to canonical names |
| Schema Registry | Embedded in both + YAML | Defines known schemas and alias lists |

### Schema Registry

#### CUSTOMER Schema

| Canonical | Required | Accepted Aliases |
|---|---|---|
| `id` | YES | id, ID, user_id, customer_id, record_id, uid, userId, customerId |
| `name` | YES | name, full_name, customer_name, user_name, fullname, displayName, display_name |
| `age` | YES | age, customer_age, user_age, years_old, Age |

#### ORDERS Schema

| Canonical | Required | Accepted Aliases |
|---|---|---|
| `order_id` | YES | order_id, orderId, orderNo, order_no, orderid |
| `product_name` | YES | product_name, product, item_name, item, productName |
| `quantity` | YES | quantity, qty, units, count, quantity_ordered |
| `unit_price` | YES | unit_price, price, unitPrice, rate, unitprice |

#### GENERIC Mode

- Triggered when no known schema matches
- No required columns
- Any CSV or JSON file is accepted
- Lightweight profiling only — no column-name assumptions anywhere

---

### `pipeline/schema_validator.py`

#### `detect_dataset_type(df: DataFrame) -> str`

```
Input:  Bronze DataFrame (all-string columns)
Output: "CUSTOMER" | "ORDERS" | "GENERIC"
Cost:   O(columns * alias_count) - ZERO row scans, ZERO Spark jobs
```

Algorithm:
1. Lowercase all `df.columns` -> `cols`
2. For each schema in registry (CUSTOMER, ORDERS):
   - For each required canonical field, expand to its full alias list
   - Check if ANY alias is present in `cols`
   - If all required fields found -> return that schema type
3. If no schema matched -> return "GENERIC"

```python
def matches_schema(required_cols, aliases):
    for req in required_cols:
        alias_list = [a.lower() for a in aliases.get(req, [])] + [req.lower()]
        if not any(a in cols for a in alias_list):
            return False
    return True
```

#### `validate_schema(df, dataset_type, source_file) -> dict`

Called **after** schema mapping has been applied. Checks that all required canonical column names are present in the DataFrame.

Writes `output/schema_compatibility_report.json` atomically (tmpfile + os.replace).

Raises `SchemaValidationError` (custom exception) with a friendly message:
```
Unsupported dataset schema. Missing required columns: age
```

Report JSON structure:
```json
{
  "dataset_type": "CUSTOMER",
  "is_supported": true,
  "found_columns": ["id", "name", "age", "ingestion_time", "source_file"],
  "expected_columns": ["id", "name", "age"],
  "missing_required_columns": [],
  "is_compatible": true
}
```

---

### `pipeline/schema_mapper.py`

#### `apply_schema_mapping(df, dataset_type, run_id, source_file)`

Returns: `(mapped_df, mappings_applied, unresolved_columns)`

Algorithm:
1. Build reverse lookup: `alias.lower() -> canonical_name`
2. For each column in `df.columns`:
   - If it resolves to a canonical name (and differs) -> add to `rename_map`
   - If it resolves to itself -> already canonical, no rename needed
   - If not resolved -> add to `unresolved` list (kept as-is, not dropped)
3. Apply all renames via chained `withColumnRenamed`
4. Log all mappings to `delta/schema_mapping_log/` as Parquet (Pandas write - no Spark job)

Example mappings applied:
```
'user_id'       -> 'id'           (renamed)
'full_name'     -> 'name'         (renamed)
'customer_age'  -> 'age'          (renamed)
'phone_number'  -> unresolved     (kept as-is)
'email'         -> unresolved     (kept as-is)
```

**GENERIC mode:** No mapping applied. All columns kept as-is. Unresolved list = all columns (for info only).

---

## 7. Pipeline — Stage by Stage

### Entry: `spark_jobs/unified_pipeline.py` — 613 lines

Called by Airflow BashOperator with:
```bash
python /opt/airflow/spark_jobs/unified_pipeline.py \
  --airflow-start-time '{{ dag_run.start_date.timestamp() }}'
```

The `--airflow-start-time` lets the pipeline measure Airflow scheduling overhead separately from actual pipeline runtime.

---

### Stage 0 — Dynamic Partition Tuning

**Before Spark initializes**, count rows in Python:

```python
def estimate_partitions_and_parallelism():
    for f in os.listdir(INPUT_PATH):
        if f.endswith('.csv'):
            with open(fp) as file:
                input_rows += sum(1 for _ in file) - 1    # subtract header
        elif f.endswith('.json'):
            with open(fp) as file:
                input_rows += sum(1 for _ in file)
    # Map row count to partition count
    if input_rows <= 10_000:   return 1
    if input_rows <= 100_000:  return 2
    if input_rows <= 1_000_000: return 4
    return 8
```

This is passed to `get_spark_session(shuffle_partitions=N)` before the session starts.

**Why it matters:** Spark's default shuffle partitions (200) would create massive overhead for a 100-row test file. This avoids both under-utilization and over-shuffling.

---

### Stage 1 — Bronze Ingestion

**File:** `spark_jobs/bronze_ingest.py` — 134 lines
**Function:** `ingest_bronze(spark, run_id, bg_threads)`

#### Step 1: List input files

```python
csv_files = [f for f in os.listdir(INPUT_PATH) if f.endswith('.csv')]
json_files = [f for f in os.listdir(INPUT_PATH) if f.endswith('.json')]
```

#### Step 2: Read all columns as strings (no inferSchema)

```python
df_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(f"{INPUT_PATH}/*.csv")
```

All columns ingested as strings intentionally. Type casting is explicit and logged at Silver.

#### Step 3: Align schemas if CSV + JSON both present

```python
for c in json_cols - csv_cols:
    df_csv = df_csv.withColumn(c, lit(None).cast("string"))
for c in csv_cols - json_cols:
    df_json = df_json.withColumn(c, lit(None).cast("string"))
df = df_csv.unionByName(df_json)
```

#### Step 4: Add metadata columns

```python
df = df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file", input_file_name())
```

#### Step 5: Schema detection (O(columns))

```python
dataset_type = detect_dataset_type(df)   # "CUSTOMER" | "ORDERS" | "GENERIC"
```

#### Step 6: Alias mapping

```python
df, mappings, unresolved = apply_schema_mapping(df, dataset_type, run_id, source_file)
```

#### Step 7: Schema validation

```python
validate_schema(df, dataset_type, source_file)
# Raises SchemaValidationError if required columns missing after mapping
```

#### Step 8: Write Bronze Delta (background thread)

```python
def write_bronze_bg():
    write_delta(df, BRONZE_PATH, mode="overwrite")

t_bronze = threading.Thread(target=write_bronze_bg)
t_bronze.start()
bg_threads.append(t_bronze)
# Pipeline continues immediately - does not wait for Delta write
```

Returns: `(df, source_file, mappings, dataset_type)`

---

### Stage 2 — Data Profiling

**File:** `spark_jobs/data_profiler.py` — 291 lines
**Function:** `profile_dataframe(df, run_id, source_file, spark, mappings)`

#### Size detection (determines Pandas vs Spark path):

```python
file_sizes = [os.path.getsize(os.path.join(INPUT_PATH, f))
              for f in os.listdir(INPUT_PATH) if f.endswith(('.csv', '.json'))]
total_bytes = sum(file_sizes)
large_dataset = total_bytes > 5 * 1024 * 1024   # 5 MB threshold
```

#### Small dataset path (< 5 MB): Pandas

1. `df.toPandas()` - safe for small data
2. Vectorized pandas operations:
   - `.isnull().sum()` - null counts
   - `.nunique()` - distinct values
   - `.describe()` - min/max/mean/std
   - `.value_counts().head(5)` - sample values

#### Large dataset path (>= 5 MB): Single-pass Spark aggregation

```python
agg_exprs = [F.count("*").alias("total_rows")]
for c in columns_to_profile:
    null_expr = F.when(
        col(c).isNull() | (F.trim(col(c)) == "") | 
        (col(c) == "None") | (col(c) == "nan") | (col(c) == "NaN"), 1
    ).otherwise(0)
    agg_exprs += [
        F.sum(null_expr).alias(f"{c}_nulls"),
        F.countDistinct(c).alias(f"{c}_distinct"),
        F.min(col(c)).alias(f"{c}_min"),
        F.max(col(c)).alias(f"{c}_max"),
        F.mean(col(c).cast("double")).alias(f"{c}_mean"),
        F.stddev(col(c).cast("double")).alias(f"{c}_stddev"),
    ]
metrics_row = df.select(*agg_exprs).collect()[0]   # ONE Spark action for ALL columns
```

#### Anomaly detection (compare vs previous run's profile):

| Condition | Alert Generated |
|---|---|
| Null % for column X increased by > 20pp | `NULL_SPIKE: col {X}` |
| Row count dropped > 30% | `VOLUME_DROP` |
| Row count increased > 2x | `VOLUME_SPIKE` |
| Numeric value outside mean +/- 3*stddev | `OUTLIER: col {X}` |

#### Profile record per column:

```python
{
    "run_id":         "run_abc123",
    "source_file":    "customers.csv",
    "column_name":    "age",
    "total_rows":     10000.0,
    "null_count":     120.0,
    "null_pct":       1.2,
    "distinct_count": 89.0,
    "min_val":        "18",
    "max_val":        "95",
    "mean_val":       42.3,
    "stddev_val":     15.7,
    "sample_vals":    "25, 34, 67, 45, 29",
    "profiled_at":    "2026-07-12T10:00:00"
}
```

Profile written to `delta/data_profile/` as Parquet in a **background thread** (does not block DQ).

---

### Stage 3 — DQ Engine

**Files:** `spark_jobs/validate_data.py` + `spark_jobs/dq_engine.py`

#### `validate_data(spark, run_id, source_file, bronze_df, bg_threads, dataset_type)`

1. Calls `run_dq_engine(bronze_df, run_id, source_file, dataset_type)`
2. Receives `(valid_df, invalid_df, scorecard)`
3. Loads thresholds from `dq_config.yaml`
4. Checks: `scorecard["total_rows"] < min_row_count` -> fail
5. Checks: `null_ids / total_rows > max_null_id_pct` -> fail
6. Returns `(valid_df, invalid_df, scorecard, should_fail: bool)`

#### `run_dq_engine(df, run_id, source_file, dataset_type)`

Dispatcher pattern - routes to correct implementation:

```python
if dataset_type == "CUSTOMER":  return _run_customer_dq(df, run_id, source_file)
elif dataset_type == "ORDERS":  return _run_orders_dq(df, run_id, source_file)
else:                           return _run_generic_dq(df, run_id, source_file)
```

Full DQ details in Section 8.

---

### Stage 4 — Quarantine

**File:** `spark_jobs/quarantine.py` — 62 lines
**Function:** `write_quarantine(invalid_df, run_id, source_file, row_count)`

```python
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
```

Every column check is guarded (`if col not in enriched.columns`) - safe even if invalid_df structure varies.

**S3 export** runs concurrently in background thread. Safety limit: only if `row_count <= 50000`.

**Design principle:** *No data is ever discarded. Every rejected row is traceable indefinitely.*

---

### Stage 5 — Silver Transformation

**File:** `spark_jobs/silver_transform.py` — 174 lines
**Function:** `transform_silver(spark, valid_df, row_count, dataset_type)`

Accepts ONLY `valid_df` from Stage 3. Rejected rows never reach Silver.

#### CUSTOMER transformations (all column-existence-guarded):

```python
if "id" in bronze_df.columns:
    cleaned_id = regexp_replace(trim(col("id")), r"\.0+$", "").cast("int")
    transformed_df = transformed_df.withColumn("id", cleaned_id)

if "age" in bronze_df.columns:
    cleaned_age = regexp_replace(trim(col("age")), r"\.0+$", "").cast("int")
    null_impute = age_cfg.get("null_impute_value", 45)
    final_age = when(cleaned_age.isNull(), lit(null_impute)).otherwise(cleaned_age)
    transformed_df = transformed_df.withColumn("age", final_age)

if "name" in bronze_df.columns:
    name_col = trim(col("name"))
    if cleaning.get("name", {}).get("title_case", True):
        name_col = initcap(name_col)     # "john doe" -> "John Doe"
    transformed_df = transformed_df.withColumn("name", name_col)

if "id" in transformed_df.columns:
    final_df = transformed_df.dropDuplicates(["id"])
```

#### ORDERS transformations:

```python
if "order_id" in bronze_df.columns:
    transformed_df = transformed_df.withColumn("order_id", trim(col("order_id")))
if "quantity" in bronze_df.columns:
    transformed_df = transformed_df.withColumn(
        "quantity", regexp_replace(trim(col("quantity")), r"\.0+$", "").cast("int"))
if "unit_price" in bronze_df.columns:
    transformed_df = transformed_df.withColumn(
        "unit_price", trim(col("unit_price")).cast("double"))
if "product_name" in bronze_df.columns:
    transformed_df = transformed_df.withColumn(
        "product_name", regexp_replace(trim(col("product_name")), r"\s+", " "))
```

#### GENERIC transformations (dynamic type inference):

```python
# 1. Sample 1000 rows with Pandas for fast type inference
sample_pdf = bronze_df.limit(1000).toPandas()

col_types = {}
for c in sample_pdf.columns:
    series = sample_pdf[c].dropna()
    # Try integer
    try:
        num = pd.to_numeric(series)
        col_types[c] = "int" if (num % 1 == 0).all() else "double"
        continue
    except: pass
    # Try date
    try:
        if series.astype(str).str.len().mean() >= 8:
            pd.to_datetime(series)
            col_types[c] = "date"
            continue
    except: pass
    col_types[c] = "string"

# 2. Apply inferred types to full Spark DataFrame
for c, t in col_types.items():
    if t == "int":
        transformed_df = transformed_df.withColumn(
            c, regexp_replace(trim(col(c)), r"\.0+$", "").cast("int"))
    elif t == "double":
        transformed_df = transformed_df.withColumn(c, trim(col(c)).cast("double"))
    elif t == "date":
        transformed_df = transformed_df.withColumn(c, to_date(col(c)))
    else:
        transformed_df = transformed_df.withColumn(
            c, regexp_replace(trim(col(c)), r"\s+", " "))
```

#### Common final step (all modes):

```python
final_df = final_df.withColumn("processed_date", to_date("ingestion_time"))
write_delta(final_df, SILVER_PATH, mode="overwrite", partition_by="processed_date")
```

---

### Stage 6 — Gold Metrics

**File:** `spark_jobs/gold_metrics.py` — 133 lines
**Function:** `generate_gold(spark, scorecard, anomalies, mappings, run_id, runtime_seconds, silver_df, dataset_type)`

#### CUSTOMER KPIs:

```python
agg_ops = [count("*").alias("total_users")]
if "age" in df_to_use.columns:
    agg_ops.append(avg("age").alias("average_age"))
business_df = silver_df.groupBy("processed_date").agg(*agg_ops)
write_delta(business_df, GOLD_PATH, mode="overwrite", partition_by="processed_date")
```

#### ORDERS KPIs:

```python
agg_ops = []
if "order_id" in df_to_use.columns:
    agg_ops.append(countDistinct("order_id").alias("total_orders"))
else:
    agg_ops.append(lit(0).alias("total_orders"))

if "quantity" in df_to_use.columns and "unit_price" in df_to_use.columns:
    agg_ops.append(sum(col("quantity") * col("unit_price")).alias("total_revenue"))
    agg_ops.append(avg(col("quantity") * col("unit_price")).alias("avg_order_value"))
else:
    agg_ops.append(lit(0.0).alias("total_revenue"))
    agg_ops.append(lit(0.0).alias("avg_order_value"))

business_df = silver_df.groupBy("processed_date").agg(*agg_ops)
```

#### GENERIC overview:

```python
generic_data = [{
    "total_rows":         float(scorecard.get("total_rows", 0)),
    "total_columns":      float(scorecard.get("total_columns", 0)),
    "duplicate_rate":     float(scorecard.get("duplicate_rate", 0.0)),
    "completeness_score": float(scorecard.get("completeness_score", 100.0)),
    "column_metrics":     json.dumps(scorecard.get("column_metrics", {}))
}]
```

#### DQ Run Report (all modes - always written):

```python
summary_data = [{
    "run_id":          run_id,
    "source_file":     scorecard.get("source_file", "unknown"),
    "total_rows":      float(scorecard.get("total_rows", 0)),
    "valid_rows":      float(scorecard.get("valid_rows", 0)),
    "invalid_rows":    float(scorecard.get("invalid_rows", 0)),
    "dq_score":        float(scorecard.get("dq_score", 100.0)),
    "null_ids":        float(scorecard.get("null_ids", 0)),
    "malformed_ids":   float(scorecard.get("malformed_ids", 0)),
    "duplicate_ids":   float(scorecard.get("duplicate_ids", 0)),
    "null_ages":       float(scorecard.get("null_ages", 0)),
    "invalid_ages":    float(scorecard.get("invalid_ages", 0)),
    "null_names":      float(scorecard.get("null_names", 0)),
    "runtime_seconds": float(round(runtime_seconds, 2)),
    "anomaly_flags":   "; ".join(anomalies) if anomalies else "None",
    "schema_mappings": json.dumps(mappings) if mappings else "[]",
    "dataset_type":    dataset_type
}]
summary_df = spark.createDataFrame(summary_data) \
    .withColumn("processed_at", current_timestamp()) \
    .withColumn("report_time", current_timestamp())
write_delta(summary_df, DQ_REPORT_PATH, mode="append")
```

---

### Stage 7 — S3 Export & Report (Background Join)

```python
for t in bg_threads:
    t.join(timeout=3.0)    # 3-second max wait per thread
```

All three S3 export threads (Silver, Quarantine, Gold) were started during their respective stages. This stage collects them with a timeout. If S3 is slow, threads continue independently without blocking pipeline completion.

After joining:
```python
spark.catalog.clearCache()   # Release cached DataFrames from memory
```

---

## 8. Data Quality Engine — Deep Dive

### `_run_customer_dq(df, run_id, source_file)`

All column references are guarded:

```python
if "id" in cols:
    cleaned_id  = regexp_replace(trim(col("id")), r"\.0+$", "")
    parsed_id   = cleaned_id.cast("int")
    is_null_id      = col("id").isNull() | (trim(col("id")) == "")
    is_malformed_id = (~is_null_id) & parsed_id.isNull()
else:
    # Safe defaults if column doesn't exist
    cleaned_id  = lit(None).cast("string")
    parsed_id   = lit(None).cast("int")
    is_null_id      = lit(False)
    is_malformed_id = lit(False)
```

#### Duplicate detection via Window function (no collect()):

```python
from pyspark.sql import Window
w = Window.partitionBy("id")
df_with_counts = df.withColumn("_id_count", count("id").over(w))
is_dup_id = col("_id_count") > 1
```

#### All CUSTOMER violation tags:

| Tag | Condition |
|---|---|
| `NULL_ID` | `id` is null or empty string |
| `MALFORMED_ID` | `id` is not null but cannot cast to integer |
| `DUPLICATE_ID` | Same `id` value appears more than once |
| `NULL_NAME` | `name` is null or empty string |
| `SUSPICIOUS_NAME` | `name` consists entirely of digits |
| `NULL_AGE` | `age` is null or empty string |
| `INVALID_AGE` | `age` < 0, > 120, or cannot cast to integer |

#### Row classification and tagging:

```python
is_invalid = (is_null_id | is_malformed_id | is_dup_id |
              is_null_name | is_null_age | is_invalid_age)

# Build violation list per row using array of conditionals
violation_array = array(
    when(is_null_id,      lit("NULL_ID")),
    when(is_malformed_id, lit("MALFORMED_ID")),
    when(is_dup_id,       lit("DUPLICATE_ID")),
    when(is_null_name,    lit("NULL_NAME")),
    when(is_null_age,     lit("NULL_AGE")),
    when(is_invalid_age,  lit("INVALID_AGE")),
)
violations_str = concat_ws(", ", array_remove(violation_array, None))

invalid_df = df_tagged \
    .filter(is_invalid) \
    .withColumn("quarantine_reason", violations_str) \
    .withColumn("rule_violated",     violations_str) \
    .withColumn("run_id",            lit(run_id)) \
    .withColumn("dq_source_file",    lit(source_file))
```

---

### `_run_orders_dq(df, run_id, source_file)`

#### All ORDERS violation tags:

| Tag | Condition |
|---|---|
| `NULL_ORDER_ID` | `order_id` is null or empty |
| `DUPLICATE_ORDER_ID` | Same `order_id` appears more than once |
| `NULL_QUANTITY` | `quantity` is null or empty |
| `NEGATIVE_QUANTITY` | `quantity` cast to int is < 0 |
| `NULL_UNIT_PRICE` | `unit_price` is null or empty |
| `NEGATIVE_UNIT_PRICE` | `unit_price` cast to double is < 0 |
| `NULL_ORDER_DATE` | `order_date` is null or empty |
| `INVALID_ORDER_DATE` | `order_date` cannot be parsed as a date |

---

### `_run_generic_dq(df, run_id, source_file)`

**Zero column-name assumptions.** Single aggregation pass:

```python
columns_to_check = [c for c in df.columns
                    if c not in ("ingestion_time", "source_file")]

# Build one giant agg() call for ALL columns simultaneously
agg_exprs = [F.count("*").alias("total_rows")]

for c in columns_to_check:
    null_expr = F.when(
        col(c).isNull() | (F.trim(col(c).cast("string")) == "") |
        (col(c).cast("string") == "None") | (col(c).cast("string") == "nan"), 1
    ).otherwise(0)
    agg_exprs.append(F.sum(null_expr).alias(f"nulls_{c}"))
    agg_exprs.append(F.countDistinct(c).alias(f"distinct_{c}"))

# Add full-row duplicate detection
df_with_hash = df.withColumn("_row_hash", F.md5(F.concat_ws("|", *columns_to_check)))
agg_exprs.append(F.countDistinct("_row_hash").alias("unique_rows"))

# ONE Spark action -> all metrics computed in a single pass
result = df_with_hash.agg(*agg_exprs).collect()[0]
```

Scorecard assembled from result:
```python
scorecard = {
    "total_rows":         total_rows,
    "valid_rows":         total_rows,    # All rows accepted in GENERIC mode
    "invalid_rows":       0,
    "dq_score":           100.0,
    "total_columns":      len(columns_to_check),
    "completeness_score": (1 - avg_null_rate) * 100,
    "duplicate_rate":     (total_rows - unique_rows) / total_rows * 100,
    "column_metrics":     {col: {"null_pct": ..., "distinct": ...} for col in columns}
}
```

In GENERIC mode, `valid_df = df` (all rows pass). No rows go to quarantine.

---

### DQ Scorecard Schema

```python
{
    # Universal fields
    "total_rows":      10000,
    "valid_rows":      9650,
    "invalid_rows":    350,
    "dq_score":        96.5,        # valid_rows / total_rows * 100
    "source_file":     "customers.csv",

    # CUSTOMER-specific
    "null_ids":        12,
    "malformed_ids":   3,
    "duplicate_ids":   5,
    "null_names":      20,
    "null_ages":       8,
    "invalid_ages":    15,

    # GENERIC-specific
    "total_columns":      7,
    "completeness_score": 98.2,
    "duplicate_rate":     0.5,
    "column_metrics": {
        "order_id":   {"null_pct": 0.5, "distinct": 980000},
        "quantity":   {"null_pct": 0.0, "distinct": 48},
        "unit_price": {"null_pct": 0.5, "distinct": 12847},
        ...
    }
}
```

---

## 9. Airflow Orchestration

### DAG: `file_trigger_pipeline` (`dags/file_trigger_pipeline.py`)

```python
with DAG(
    dag_id="file_trigger_pipeline",
    default_args={
        "owner":            "airflow",
        "start_date":       pendulum.datetime(2024, 1, 1, tz="UTC"),
        "retries":          1,
        "retry_delay":      timedelta(minutes=5),
        "on_failure_callback": on_dag_failure,
    },
    schedule=None,        # Manual trigger only
    catchup=False,
    max_active_runs=1,    # No concurrent pipeline runs
) as dag:

    unified_pipeline = BashOperator(
        task_id="unified_medallion_pipeline",
        bash_command="python /opt/airflow/spark_jobs/unified_pipeline.py "
                     "--airflow-start-time '{{ dag_run.start_date.timestamp() }}'",
    )

    archive_file = BashOperator(
        task_id="archive_file",
        bash_command="python /opt/airflow/monitoring/archive_file.py",
    )

    unified_pipeline >> archive_file
```

#### Failure callback:

```python
def on_dag_failure(context):
    dag_id   = context["dag"].dag_id
    run_id   = context["dag_run"].run_id
    task_id  = context["task_instance"].task_id
    exception = context.get("exception", "Unknown Error")
    message  = f"FAILURE in task '{task_id}': {str(exception)[:200]}..."
    os.system(f"python3 /opt/airflow/monitoring/log_incident.py "
              f"'{dag_id}' '{run_id}' '{task_id}' '{message}'")
```

### Triggering from Streamlit

```
POST http://airflow:8080/api/v2/dags/file_trigger_pipeline/dagRuns
Authorization: Bearer {jwt_token}
Content-Type: application/json

{"logical_date": "2026-07-12T10:00:00+00:00"}
```

Dashboard polls status every 2 seconds:
```
GET http://airflow:8080/api/v2/dags/file_trigger_pipeline/dagRuns/{run_id}
```

---

## 10. Streamlit Dashboard

**File:** `dashboard/app.py` — ~1,607 lines  
**Port:** 8501  
**URL:** `http://localhost:8501`

### Page 1: Home / Upload

- `st.file_uploader` for CSV and JSON files
- On upload: writes file to `/data/input/` via temp directory + `shutil.copy`
- Detects schema from column headers (no Spark needed — pure Python column check)
- Shows schema detection badge: CUSTOMER / ORDERS / GENERIC
- Shows schema compatibility report if one exists
- **Run Pipeline** button → triggers Airflow DAG via `airflow_client.trigger_dag()`
- Real-time status polling every 2 seconds via `st.rerun()` while running
- Shows progress: stage name + duration + DQ score (once available)

### Page 2: Pipeline Monitor

- Current run status card (status, run_id, stage, duration)
- Last 20 runs from `pipeline_history.jsonl`:
  - Columns: timestamp, run_id, file, status, dataset_type, rows, duration, error
  - Color-coded by status (green=completed, red=failed, orange=running)
- Airflow task log viewer (expandable, fetched via REST API)

### Page 3: Data Lineage

- Reads Delta Lake `_delta_log/` transaction log files
- Parses each `*.json` log file for file operations:
  - `add` — file written
  - `remove` — file removed
- Shows table: timestamp | operation | rows written | output bytes
- Bar chart: processing volume over time

### Page 4: Data Quality & Observability

- DQ scorecard: total rows / valid / invalid / DQ score %
- Per-column violation breakdown (bar chart)
- Quarantine table viewer: filterable by run_id, shows rejection reasons
- Anomaly alerts from profiler (null spikes, volume changes)
- DQ score trend across multiple runs (line chart)

### Page 5: Data Profiling

- Column statistics: null %, distinct count, min/max, mean, stddev
- Sample values per column
- Run selector to browse historical profile results

### Page 6: Gold Analytics

- **CUSTOMER:** Average age trend by date, total users bar chart
- **ORDERS:** Revenue by date, total orders, avg order value
- **GENERIC:** Completeness score gauge, row count, duplicate rate

### Page 7: Schema Mapping Log

- Table of all alias resolutions applied:
  - `from_col`, `to_col`, `run_id`, `source_file`, `mapping_time`
- Grouped by run for easy audit

### Page 8: Reports

- Run selector dropdown
- DQ run report viewer (from `delta/dq_run_report/`)
- Downloadable TXT report button
- Schema compatibility report JSON viewer

### Page 9: System Health

- Docker container ping status
- Disk usage per Delta table directory
- S3 connectivity test (if `S3_BUCKET` is set)
- Last pipeline timing breakdown

---

### `dashboard/queries.py` — ~55 KB

All data access functions. Schema-aware throughout — never references a column without `if "col" in df.columns`.

Key functions:

| Function | Purpose |
|---|---|
| `read_delta_pandas(path)` | Read Delta table as Pandas DataFrame |
| `get_latest_dataset_type()` | Read dataset_type from status JSON |
| `get_latest_successful_run_id()` | Scan history for last completed run |
| `get_dq_run_report(run_id)` | Load full run report from Delta |
| `get_gold_report_data(spark, ...)` | Assemble Gold analytics data per schema type |
| `generate_txt_report(report_data)` | Build downloadable plain-text report |
| `col(name)` | Schema-safe column accessor (checks existence) |

### `dashboard/airflow_client.py` — 12 KB

- JWT token authentication (obtained via `POST /auth/token`)
- Token auto-refresh on 401 responses
- `trigger_dag(dag_id)` — triggers pipeline run
- `get_dag_run_status(dag_id, run_id)` — polls run status
- `get_task_log(dag_id, run_id, task_id)` — fetches task stdout/stderr

### `dashboard/charts.py` — 8 KB

Plotly chart builders:
- `dq_score_gauge(score)` — circular gauge chart
- `violation_bar(violations_dict)` — per-violation-type bar chart
- `volume_trend(history_df)` — row count over time
- `age_distribution(gold_df)` — histogram for CUSTOMER
- `revenue_trend(gold_df)` — line chart for ORDERS

---

## 11. S3 Shadow Storage

**File:** `utils/s3_client.py` — 10 KB

### Purpose

Optional AWS S3 backup of pipeline outputs. Useful for:
- Long-term archival of quarantine records
- Downstream consumption of cleaned Silver data
- Sharing Gold reports outside the platform

### Functions

| Function | Description |
|---|---|
| `_get_bucket()` | Returns `S3_BUCKET` env var value; None if unset |
| `export_csv_bytes_to_s3(bytes, key, run_id, row_count)` | Upload CSV bytes to S3 |
| `export_text_to_s3(text, key, run_id)` | Upload plain text to S3 |

### S3 Key Structure

```
s3://{bucket}/
    quarantine/{run_id}/rejected_records.csv
    exports/{run_id}/cleaned_dataset.csv
    reports/{run_id}/gold_report.txt
```

### Safety Limits

All `toPandas()` calls gated by row count:

```python
if q_count > 50000:
    print("[BG Quarantine Export] Size exceeds threshold. Skipping toPandas().")
    return
```

This prevents OOM errors on large quarantine/silver datasets.

### Fail-Safe Design

```python
if _get_bucket() is None:
    print("[BG Export] S3_BUCKET not configured. Skipping.")
    return
```

Pipeline **never fails** due to missing S3 credentials or S3 connectivity issues. All exports are best-effort.

---

## 12. Performance Engineering

### Optimization Techniques

| Technique | Where Applied | Impact |
|---|---|---|
| Pre-Spark row counting | Stage 0 | Dynamic shuffle partitions (1-8) |
| Background Bronze write | Stage 1 | DQ starts while Bronze writes |
| Background profile write | Stage 2 | DQ not blocked by profile I/O |
| Background S3 exports | Stages 4/5/6 | Pipeline not blocked by S3 |
| Single-pass GENERIC DQ | Stage 3 | O(1) Spark jobs regardless of column count |
| Single-pass distributed profiling | Stage 2 | O(1) Spark jobs for all column stats |
| Pandas profiling for small files | Stage 2 | Zero Spark overhead for < 5MB files |
| Window function deduplication | Stages 3/5 | Avoids full groupBy().count().join() |
| Spark DataFrame cache | Stages 3/5/6 | Avoids recomputing shared DataFrames |
| Cache clear after Gold | Stage 7 | Releases memory promptly |
| Fast I/O mode (Docker) | All stages | Works on /tmp (local NVMe), syncs back |
| SKIP_DIAGNOSTICS=1 env var | Unified pipeline | Skips expensive startup I/O validation |
| Pre-downloaded Delta JARs | Dockerfile | No Maven/Ivy downloads at runtime |
| `inferSchema=false` on CSV read | Stage 1 | Skips schema inference scan (one pass) |
| `spark.sql.ansi.enabled=false` | SparkSession | Avoids type strictness errors |
| `spark.sql.adaptive.enabled=false` | SparkSession | Deterministic, predictable partitioning |
| Parquet compression none | SparkSession | Less CPU = faster writes |
| Delta checkpoint every 100 | SparkSession | Less metadata overhead |
| Memory: 1536m driver + executor | SparkSession | Tuned for container environment |

### Fast I/O Mode (Docker Only)

When running inside a Docker container with a slow NFS/overlay2 volume:

```python
use_fast_io = os.path.exists("/data") and os.name != "nt"

if use_fast_io:
    # 1. Copy input to fast local /tmp (excluding large parquet files)
    subprocess.run(
        f"mkdir -p {fast_base} && rsync -r -t "
        f"--exclude='*.parquet' --no-perms {src}/. {dst}/",
        shell=True
    )
    # 2. Override all paths to use /tmp
    os.environ["BASE_DATA_PATH_OVERRIDE"] = "/tmp/medallion_data"
    importlib.reload(pipeline.config)

    # ... run all pipeline stages using /tmp paths ...

    # 3. After completion, sync only modified files back
    sync_fast_io_back(fast_base, original_base, script_start_time)
    subprocess.run(f"rm -rf {fast_base}", shell=True)
```

`sync_fast_io_back` uses `mtime >= start_time - 5` to find only files modified during this run — avoiding a full directory sync.

### Benchmark Results

| Dataset | Rows | Size | Total Time |
|---|---|---|---|
| Small CUSTOMER | 100 | 2 KB | ~35s |
| Medium CUSTOMER | 2,000 | 194 KB | ~38s |
| Large CUSTOMER | 20,000 | 2.1 MB | ~42s |
| ORDERS 1M | 1,000,000 | 65.6 MB | ~90-120s |
| ORDERS 10M | 10,000,000 | 656.4 MB | ~300-500s |

*Majority of time for small datasets is Spark JVM startup (~15s) and Delta JAR loading.*

---

## 13. Delta Lake Internals

### Why Delta Lake?

| Feature | Benefit |
|---|---|
| ACID transactions | No partial writes; always consistent state |
| Schema evolution | Add columns without breaking existing data |
| Time travel | Query data at any historical version |
| Transaction log | Full audit trail of every file operation |
| Z-ordering | Faster queries on high-cardinality columns |
| Partition pruning | Skip files not matching `WHERE processed_date =` |

### Write Modes Used

| Layer | Mode | Schema Option | Reason |
|---|---|---|---|
| Bronze | overwrite | overwriteSchema=true | Replace raw data each run |
| Silver | overwrite | overwriteSchema=true | Replace cleaned data each run |
| Gold | overwrite | overwriteSchema=true | Replace KPIs each run |
| Quarantine | append | mergeSchema=true | Never lose rejected rows |
| DQ Report | append | mergeSchema=true | Accumulate run history |
| Data Profile | append | mergeSchema=true | Track profiling trends |
| Schema Map Log | Pandas Parquet | N/A | Lightweight, no Spark overhead |

### `delta_utils.py` SparkSession Builder

```python
def get_spark_session(app_name="MedallionPipeline", shuffle_partitions=1):
    os.environ["PYSPARK_SUBMIT_ARGS"] = \
        f"--packages {DELTA_PACKAGE} pyspark-shell"

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions",  str(shuffle_partitions)) \
        .config("spark.default.parallelism",      str(shuffle_partitions)) \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.ansi.enabled",         "false") \
        .config("spark.driver.extraJavaOptions",  JAVA_OPTS) \
        .config("spark.executor.memory",          "1536m") \
        .config("spark.driver.memory",            "1536m") \
        .config("spark.ui.enabled",               "false") \
        .config("spark.sql.adaptive.enabled",     "false") \
        .config("spark.sql.parquet.compression.codec", "none") \
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", "100") \
        .getOrCreate()
```

### Time Travel

```python
# Read specific version
spark.read.format("delta").option("versionAsOf", 2).load(SILVER_PATH)

# Read at specific timestamp
spark.read.format("delta") \
    .option("timestampAsOf", "2026-06-15 10:00:00") \
    .load(SILVER_PATH)

# Show table history
DeltaTable.forPath(spark, SILVER_PATH).history().show()
```

---

## 14. Status & History Files

### `pipeline_status.json`

Written atomically after every stage transition. Polled by Streamlit every 2 seconds.

```json
{
    "status":       "completed",
    "run_id":       "run_abc123def456",
    "timestamp":    1752304800.0,
    "file_name":    "customers.csv",
    "stage":        "Finished",
    "error":        null,
    "duration":     "45.30",
    "dataset_type": "CUSTOMER"
}
```

**Status values:** `running` | `completed` | `failed`

**Stage progression:**
```
Waiting -> Bronze -> Profiling -> Validation -> Quarantine -> Silver -> Gold -> S3 Export -> Finished
```

**Atomic write pattern:**
```python
tmp = f"{STATUS_FILE}.tmp"
with open(tmp, "w") as f:
    json.dump(status_data, f, indent=4)
os.replace(tmp, STATUS_FILE)   # Atomic rename - dashboard never reads partial writes
```

---

### `pipeline_history.jsonl`

Append-only. One JSON object per line. Never overwritten.

```jsonl
{"timestamp":1752304800.0,"run_id":"run_abc123","file_name":"customers.csv","status":"completed","duration":"45.30","rows":9650,"error":null,"dataset_type":"CUSTOMER"}
{"timestamp":1752308400.0,"run_id":"run_def456","file_name":"orders.csv","status":"failed","duration":"12.10","rows":0,"error":"DQ threshold exceeded. Score: 45.0%","dataset_type":"ORDERS"}
```

---

## 15. Error Handling Strategy

### Philosophy

1. **Non-fatal stages** (profiling, mapping log, metrics file): wrapped in `try/except` with `print` warning. Pipeline succeeds even if these fail.
2. **Fatal stages** (DQ threshold, schema validation, Spark errors): propagate through `run_unified_pipeline()`'s main `except` block.
3. **User-facing errors**: translated to friendly English before being written to `pipeline_status.json`.
4. **Developer errors**: full stack trace always printed to stdout.

### Error Translation

```python
friendly_error = error_msg
if "SchemaValidationError" in type(e).__name__ or \
   "Unsupported dataset schema" in error_msg:
    friendly_error = error_msg              # Already friendly
elif "AnalysisException" in error_msg or \
     "UNRESOLVED_COLUMN" in error_msg:
    friendly_error = ("Database Error: A column resolution or "
                      "query analysis failure occurred in Spark.")
elif "NoCredentialsError" in error_msg:
    friendly_error = ("AWS S3 Authentication Error: "
                      "Failed to locate credentials to write S3 exports.")
```

### DQ Threshold Failure

```python
if should_fail:
    write_status("failed", run_id, stage="Validation",
                 error=f"DQ threshold exceeded. Score: {scorecard['dq_score']}%")
    raise RuntimeError(f"DQ threshold exceeded. Score: {scorecard['dq_score']}%")
```

This goes through the main except block for consistent logging, history writing, and incident logging.

---

## 16. Git Evolution — Commit-by-Commit

| # | Commit Hash | Description |
|---|---|---|
| 1 | `ef78f37` | Airflow 3 JWT auth, connection diagnostics, startup validation |
| 2 | `e1dafbd` | Custom auth manager: load passwords from env, display auth source in Streamlit |
| 3 | `c126f60` | Monkeypatch base static auth methods, add validate_auth.py script |
| 4 | `f54cb03` | Programmatic auth, hidden credentials, self-service product refactor |
| 5 | `0d0d098` | Lightweight orchestrator client, hidden auth completely from UI |
| 6 | `c8bac41` | Postgres backend for Airflow (later reverted - SQLite simpler) |
| 7 | `a9f7823` | Dockerfile layer caching - only reinstall deps when requirements change |
| 8 | `98c7fce` | Deduplicate Docker Compose - single custom image for Airflow + Streamlit |
| 9 | `24e8fdb` | Pin pyspark==4.1.1, delta-spark==4.2.0 for reproducibility |
| 10 | `cf2dc4c` | Fix logical_date requirement in Airflow 3 API, platform-aware fcntl mocking |
| 11 | `1bb5d6a` | Revert to Airflow standalone + SQLite - simpler, more reliable in Docker |
| 12 | `4e69d9f` | Remove custom startup hooks, restore native credentials file provisioning |
| 13 | `120fe69` | Atomic file writes everywhere: tmpfile + os.replace pattern |
| 14 | `bda3127` | Fix failed stage reporting in unified pipeline status file fallback |
| 15 | `267ebc0` | Better failure stage tracking - correct stage name in failed runs |
| 16 | `b069b52` | Dashboard upload: local temp dir + write access validation before copy |
| 17 | `2a19a07` | Remove orchestrator abstraction, restore simple Airflow REST client |
| 18 | `dc562a4` | Fix st.rerun() no-op: remove on_click callbacks, use inline conditionals |
| 19 | `add0463` | Isolate metrics/status/log writes - pipeline success != file write success |
| 20 | `43d228a` | UID 50000 for all services, startup path validation, fail-fast Docker check |
| 21 | `d824611` | Phase 6: Enterprise DQ (quarantine, scorecard, anomaly detection, profiling) + Phase 7: S3 shadow storage |
| 22 | `6bc5928` | S3 architecture: direct bytes upload, runtime optimization |
| 23 | `5da2bb9` | Sub-40s performance: concurrent threads, Window deduplication, vectorized Pandas profiling |
| 24 | `d9243e7` | Schema-aware refactor: early validation, schema compatibility report, friendly errors |
| 25 | `a07ad18` | Full dashboard schema-awareness, unicode fix for Windows console, 10-scenario test matrix |
| 26 | `1a578b0` | CUSTOMER + ORDERS + GENERIC multi-schema support, single-pass DQ, memory tuning, lazy profiling |
| 27 | `8b3715f` | Fix IndentationError in dashboard/app.py line 1272 (unclosed try block) |

---

## 17. Test Data

### Pre-Built Sample Files (`data/samples/`)

| File | Schema | Approx Rows | Size | DQ Issues |
|---|---|---|---|---|
| `small_sample.csv` | CUSTOMER | ~100 | 2.1 KB | Missing IDs, negative ages, duplicate IDs |
| `medium_sample.csv` | CUSTOMER | ~2,000 | 194 KB | Realistic mix of violations |
| `large_sample.csv` | CUSTOMER | ~20,000 | 2.1 MB | Realistic mix of violations |
| `small_sample.json` | CUSTOMER | ~50 | 4.3 KB | Mostly clean |
| `medium_sample.json` | CUSTOMER | ~2,000 | 420 KB | Realistic mix |

These files are automatically staged to `data/input/` when the user clicks "Load Sample" in the dashboard.

### Generated Performance Test Files (`C:\Users\Acer\Documents\`)

Generated by `scratch/generate_orders_data.py`.

| File | Schema | Rows | Size | Generation Time |
|---|---|---|---|---|
| `orders_1m.csv` | ORDERS | 1,000,000 | 65.6 MB | 34.6s |
| `orders_10m.csv` | ORDERS | 10,000,000 | 656.4 MB | 6m 57s |

#### ORDERS file columns:

```
order_id, customer_id, product_name, quantity, unit_price, order_date, status
```

#### Realistic DQ issues built into generated files:

| Issue | Rate | DQ Rule Triggered |
|---|---|---|
| Missing `order_id` (blank string) | ~0.5% | `NULL_ORDER_ID` |
| Negative `quantity` (-1) | ~2.0% | `NEGATIVE_QUANTITY` |
| Missing `unit_price` (blank string) | ~0.5% | `NULL_UNIT_PRICE` |
| Invalid `order_date` ("NOT-A-DATE") | ~0.5% | `INVALID_ORDER_DATE` |

#### 20 product names in the dataset:

```
Laptop Pro 15, Wireless Mouse, USB-C Hub, Mechanical Keyboard, 4K Monitor,
Webcam HD, Noise Cancelling Headphones, SSD 1TB, Gaming Chair, Standing Desk,
Tablet 10.5, Smartwatch Series X, Bluetooth Speaker, External GPU, RAM 32GB DDR5,
Router WiFi 6, NVMe Drive 2TB, LED Desk Lamp, Ergonomic Mouse Pad, Cable Management Kit
```

#### Date range: January 1, 2022 — December 31, 2024 (3 years)

#### Status values: COMPLETED, SHIPPED, PENDING, CANCELLED, RETURNED, PROCESSING

---

## 18. File Reference — Every File Explained

### `spark_jobs/unified_pipeline.py` — 613 lines — THE MAIN ENTRY POINT

| Function | Lines | Description |
|---|---|---|
| `log_diagnostics(file_path)` | ~10 | Prints UID/GID/writability before file writes |
| `write_status(status, run_id, ...)` | ~25 | Atomic write to pipeline_status.json |
| `append_to_history(status, run_id, ...)` | ~30 | Append-only write to pipeline_history.jsonl |
| `estimate_partitions_and_parallelism()` | ~35 | Pre-Spark row count -> partition tuning |
| `sync_fast_io_back(fast_base, orig, start)` | ~30 | rsync modified files from /tmp back to /data |
| `run_unified_pipeline()` | ~380 | Main function: 7 stages + error handling |

### `spark_jobs/bronze_ingest.py` — 134 lines

Reads all CSV/JSON files from INPUT_PATH as strings. Detects schema. Maps aliases. Validates. Writes Bronze Delta in background thread. Returns `(df, source_file, mappings, dataset_type)`.

### `spark_jobs/data_profiler.py` — 291 lines

| Function | Description |
|---|---|
| `profile_dataframe(df, run_id, source_file, spark, mappings)` | Main profiler. Hybrid Pandas/Spark. Returns (records, anomalies, row_count) |
| `write_profile(records, spark)` | Writes profile records to delta/data_profile/ |

### `spark_jobs/dq_engine.py` — 411 lines

| Function | Description |
|---|---|
| `run_dq_engine(df, run_id, source_file, dataset_type)` | Dispatcher |
| `_run_customer_dq(df, run_id, source_file)` | CUSTOMER rules (ID, name, age) |
| `_run_orders_dq(df, run_id, source_file)` | ORDERS rules (order_id, quantity, price, date) |
| `_run_generic_dq(df, run_id, source_file)` | GENERIC single-pass aggregation |
| `_load_config()` | Load dq_config.yaml from standard paths |

### `spark_jobs/validate_data.py` — ~100 lines

Wrapper around dq_engine. Builds scorecard from results. Checks thresholds. Returns `(valid_df, invalid_df, scorecard, should_fail)`. Writes DQ metrics to Delta.

### `spark_jobs/quarantine.py` — 62 lines

| Function | Description |
|---|---|
| `write_quarantine(invalid_df, run_id, source_file, row_count)` | Append invalid rows to quarantine Delta table |
| `get_quarantine_count(spark, run_id)` | Count quarantined rows for a run |

### `spark_jobs/silver_transform.py` — 174 lines

| Function | Description |
|---|---|
| `transform_silver(spark, valid_df, row_count, dataset_type)` | Type-cast + clean valid rows -> Silver Delta |
| `_load_config()` | Load dq_config.yaml for cleaning rules |

### `spark_jobs/gold_metrics.py` — 133 lines

| Function | Description |
|---|---|
| `generate_gold(spark, scorecard, anomalies, mappings, run_id, runtime_seconds, silver_df, dataset_type)` | Business KPIs per schema. Always writes DQ run summary row. |

### `pipeline/config.py` — 154 lines

Central config. Auto-detects environment. Creates dirs. Validates writability. Provides all path constants and Spark config constants.

### `pipeline/schema_validator.py` — 142 lines

| Function | Description |
|---|---|
| `detect_dataset_type(df)` | O(cols) schema detection. Returns CUSTOMER/ORDERS/GENERIC |
| `validate_schema(df, dataset_type, source_file)` | Post-mapping required column check. Writes report JSON. Raises SchemaValidationError. |
| `_load_config()` | Load schema registry from YAML |

### `pipeline/schema_mapper.py` — 122 lines

| Function | Description |
|---|---|
| `apply_schema_mapping(df, dataset_type, run_id, source_file)` | Rename aliases to canonicals. Returns (df, mappings, unresolved) |
| `_log_mappings(mappings, unresolved, run_id, source_file, spark)` | Pandas Parquet write to schema_mapping_log |
| `get_mapping_summary(mappings, unresolved)` | Human-readable string for dashboard display |

### `pipeline/delta_utils.py` — 45 lines

| Function | Description |
|---|---|
| `get_spark_session(app_name, shuffle_partitions)` | Standard SparkSession with Delta config |
| `read_delta(spark, path)` | `spark.read.format("delta").load(path)` |
| `write_delta(df, path, mode, partition_by)` | With mergeSchema/overwriteSchema + parent mkdir |

### `dashboard/app.py` — ~1,607 lines

9-page Streamlit application. Reads Delta tables via `queries.py`. Triggers pipeline via `airflow_client.py`. Polls `pipeline_status.json` for live updates.

### `dashboard/queries.py` — ~55 KB

All data access layer. Schema-aware throughout. Contains every Delta read query, all report generation logic, and Gold data assembly functions.

### `dashboard/airflow_client.py` — 12 KB

Airflow REST API client. JWT token auth with auto-refresh. Trigger DAG, poll status, fetch logs.

### `dashboard/charts.py` — 8 KB

Plotly chart builders for DQ trends, violation bars, volume charts, age/revenue analytics.

### `utils/s3_client.py` — 10 KB

AWS S3 upload functions using `boto3`. Safety-gated by row count and `S3_BUCKET` env var.

### `dags/file_trigger_pipeline.py` — 66 lines

Airflow DAG with two tasks: unified_pipeline (BashOperator) -> archive_file (BashOperator). Manual trigger only, max 1 concurrent run.

### `scratch/generate_orders_data.py` — ~87 lines

Batch CSV generator. Writes 1M and 10M ORDERS rows with realistic DQ issues. Uses 50K-row batches for memory efficiency. Outputs to configurable directory.

### `scratch/test_matrix_runner.py` — 9 KB

10-scenario automated test matrix covering:
- CUSTOMER clean data
- CUSTOMER with various violations
- ORDERS schema
- GENERIC schema
- Empty file
- File with only headers
- Mixed CSV + JSON
- Very large file
- File with all violations
- Schema with aliases

---

## 19. Spark Session Configuration

### Full Configuration Reference

```python
# Delta Lake integration
"spark.sql.extensions"         = "io.delta.sql.DeltaSparkSessionExtension"
"spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

# Dynamic partitioning (set per-run based on input size)
"spark.sql.shuffle.partitions" = "1" | "2" | "4" | "8"
"spark.default.parallelism"    = "1" | "2" | "4" | "8"

# Schema evolution
"spark.databricks.delta.schema.autoMerge.enabled" = "true"

# Type safety
"spark.sql.ansi.enabled" = "false"   # Allow lenient casting

# Memory
"spark.executor.memory" = "1536m"
"spark.driver.memory"   = "1536m"

# Performance
"spark.ui.enabled"                         = "false"   # No web UI
"spark.sql.adaptive.enabled"               = "false"   # Deterministic partitions
"spark.sql.parquet.compression.codec"      = "none"    # Fast writes
"spark.databricks.delta.properties.defaults.checkpointInterval" = "100"

# Java 17 compatibility (13 module opens)
"spark.driver.extraJavaOptions" = "--add-opens=java.base/... ..."
```

### Delta Package

```
io.delta:delta-spark_2.13:4.1.0
```

Pre-downloaded at Docker build time. Available in `/opt/airflow/.local/lib/python3.12/site-packages/pyspark/jars/`.

---

## 20. Known Limitations & Future Scope

### Current Limitations

| Area | Limitation | Impact |
|---|---|---|
| Spark mode | Single-node, local mode | No horizontal scaling |
| Airflow backend | SQLite | Not production multi-user |
| S3 export | 50K row limit for toPandas() | Large datasets not exported |
| GENERIC type inference | 1K-row sample | May misclassify edge-case types |
| Silver write | Full overwrite each run | Cannot do incremental append/merge |
| Auth | Single admin user | No per-user isolation |
| Schema types | Only CUSTOMER + ORDERS defined | Other schemas go GENERIC |
| Delta storage | Local filesystem | Not cloud-native |

### Future Enhancement Roadmap

| Priority | Enhancement | Description |
|---|---|---|
| High | Postgres Airflow backend | Production-grade metadata store, multiple users |
| High | Incremental Silver MERGE | Use Delta `MERGE INTO` instead of full overwrite |
| High | Additional schemas | PRODUCTS, TRANSACTIONS, EVENTS, EMPLOYEES |
| Medium | Spark cluster mode | Multi-node via spark:// or Kubernetes |
| Medium | Great Expectations | Declarative DQ rules with auto-documentation |
| Medium | ML anomaly detection | Replace static thresholds with learned baselines |
| Medium | S3/GCS Delta storage | Move Delta tables from local disk to object storage |
| Medium | RBAC | Per-user pipeline history, data access control |
| Low | Grafana/Prometheus | Export metrics to monitoring stack |
| Low | Streaming mode | Spark Structured Streaming via Kafka |
| Low | Schema versioning | Track schema evolution, migration scripts |
| Low | Multi-tenant | Per-tenant data isolation, namespaced Delta tables |
| Low | CI/CD | GitHub Actions for automated testing on PR |

---

*End of Documentation — Generated July 12, 2026*
"""

with open("PROJECT_DOCUMENTATION.md", "w", encoding="utf-8") as f:
    f.write(DOC)

size = os.path.getsize("PROJECT_DOCUMENTATION.md")
lines = DOC.count("\n")
print(f"Written: PROJECT_DOCUMENTATION.md")
print(f"  Size:  {size:,} bytes ({size/1024:.1f} KB)")
print(f"  Lines: {lines:,}")

import os
import sys

# Ensure PySpark uses the correct Python executable across environments
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Base paths
# Detect if we are in the container (/data exists) or local (use relative path)
if os.path.exists("/data") and os.name != "nt":
    BASE_DATA_PATH = "/data"
else:

    # Fallback for local development outside Docker
    BASE_DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

INPUT_PATH = f"{BASE_DATA_PATH}/input"
ARCHIVE_PATH = f"{BASE_DATA_PATH}/archive"
DELTA_PATH = f"{BASE_DATA_PATH}/delta"

# Medallion layers
BRONZE_PATH = f"{DELTA_PATH}/bronze"
SILVER_PATH = f"{DELTA_PATH}/silver"
GOLD_PATH = f"{DELTA_PATH}/gold"

# Monitoring & Quality
INCIDENTS_PATH = f"{DELTA_PATH}/incidents"
DQ_METRICS_PATH = f"{DELTA_PATH}/dq_metrics"
TRACE_PATH = f"{DELTA_PATH}/pipeline_trace"
METRICS_FILE = f"{BASE_DATA_PATH}/output/pipeline_metrics.txt"
STATUS_FILE = f"{BASE_DATA_PATH}/output/pipeline_status.json"
SAMPLES_PATH = f"{BASE_DATA_PATH}/samples"
HISTORY_FILE = f"{BASE_DATA_PATH}/output/pipeline_history.jsonl"




# Spark Config
DELTA_PACKAGE = "io.delta:delta-spark_2.13:4.1.0"
SPARK_LOG_LEVEL = "ERROR"

# Java 17+ opens for Spark/Delta
JAVA_OPTS = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

# Startup Directory Creation and Writability Validation
import os
import sys
import stat
from pathlib import Path

# Required paths for the application
REQUIRED_PATHS = [
    INPUT_PATH,
    f"{BASE_DATA_PATH}/output",
    ARCHIVE_PATH,
    BRONZE_PATH,
    SILVER_PATH,
    GOLD_PATH,
    DQ_METRICS_PATH,
    TRACE_PATH,
]

# Print current environment diagnostics
uid = None
gid = None
try:
    uid = os.getuid()
    gid = os.getgid()
except AttributeError:
    pass

print("=== SYSTEM ENVIRONMENT DIAGNOSTICS ===")
print(f"Current Execution UID: {uid} | GID: {gid}")
print("======================================")

validation_failed = False

for path_str in REQUIRED_PATHS:
    try:
        path = Path(path_str)
        # Ensure directory exists
        path.mkdir(parents=True, exist_ok=True)
        
        # Log ownership and permissions
        stat_info = path.stat()
        mode = stat_info.st_mode
        perms = stat.filemode(mode)
        path_uid = stat_info.st_uid
        path_gid = stat_info.st_gid
        
        # Test writability by touching a temp file
        test_file = path / f".write_test_{os.getpid()}"
        try:
            test_file.touch(exist_ok=True)
            test_file.unlink()
            writable = True
        except Exception:
            writable = False
            
        print(f"Directory: {path_str} | Owner UID: {path_uid} | Owner GID: {path_gid} | Perms: {perms} | Writable: {writable}")
        
        if not writable:
            # We fail fast for crucial medallion data directories
            validation_failed = True
            
    except Exception as e:
        print(f"Directory: {path_str} | Error accessing/creating: {str(e)}")
        validation_failed = True

print("======================================")

if validation_failed:
    print("FATAL: One or more required data engineering directories are not writable by the current container user.", file=sys.stderr)
    print("Please check docker volume permissions or ensure UID 50000 has write access.", file=sys.stderr)
    # Fail fast if we are running inside a container
    if os.path.exists("/data"):
        sys.exit(1)


import os

# Base paths
# Detect if we are in the container (/data exists) or local (use relative path)
if os.path.exists("/data"):
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

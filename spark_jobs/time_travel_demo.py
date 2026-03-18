from pyspark.sql import SparkSession
from delta import DeltaTable
import time
import os

# Initialize Spark Session with Delta Lake support
builder = SparkSession.builder \
    .appName("DeltaTimeTravelDemo") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data_path = "/data/output/cleaned_data"

if not os.path.exists(data_path):
    print(f"Error: Delta table not found at {data_path}. Please run the main pipeline first.")
    exit(1)

delta_table = DeltaTable.forPath(spark, data_path)

print("\n--- Starting Delta Lake Time-Travel Demo ---")
print("\n--- Current Table History ---")
delta_table.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# Operation 1: UPDATE
print("\n--- DEMO: Updating age for specific records (Version N+1) ---")
# Increment age for users with id < 5
delta_table.update(
    condition="id < 5",
    set={"age": "age + 1"}
)

# Operation 2: DELETE
print("\n--- DEMO: Deleting records with age > 80 (Version N+2) ---")
delta_table.delete(condition="age > 80")

print("\n--- Final Table History ---")
delta_table.history().select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

print("\nDemo script execution completed. You can now check the dashboard for the changes.")

import os
from pyspark.sql import SparkSession
from pipeline.config import DELTA_PACKAGE, JAVA_OPTS

def get_spark_session(app_name="MedallionPipeline"):
    """
    Standardized SparkSession builder with Delta support and Java 17+ fixes.
    """
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {DELTA_PACKAGE} pyspark-shell"
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.driver.extraJavaOptions", JAVA_OPTS) \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()

def read_delta(spark, path):
    return spark.read.format("delta").load(path)

def write_delta(df, path, mode="append", partition_by=None):
    writer = df.write.format("delta").mode(mode).option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(path)

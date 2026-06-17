import os
from pyspark.sql import SparkSession
from pipeline.config import DELTA_PACKAGE, JAVA_OPTS

def get_spark_session(app_name="MedallionPipeline", shuffle_partitions=1):
    """
    Standardized SparkSession builder with Delta support and Java 17+ fixes.
    """
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {DELTA_PACKAGE} pyspark-shell"
    
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.default.parallelism", str(shuffle_partitions)) \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.driver.extraJavaOptions", JAVA_OPTS) \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.memory", "512m") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "none") \
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", "100") \
        .getOrCreate()

def read_delta(spark, path):
    return spark.read.format("delta").load(path)

def write_delta(df, path, mode="append", partition_by=None):
    writer = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    else:
        writer = writer.option("mergeSchema", "true")
        
    if partition_by:
        writer = writer.partitionBy(partition_by)
        
    from pathlib import Path
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    writer.save(path)


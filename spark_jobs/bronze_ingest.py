import sys
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pipeline.config import BRONZE_PATH, INPUT_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, write_delta

def ingest_bronze(spark=None):
    own_spark = False
    if spark is None:
        spark = get_spark_session("BronzeIngestion")
        own_spark = True
        
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    try:
        # 1. Read CSV files
        df_csv = None
        try:
            df_csv = spark.read.csv(f"{INPUT_PATH}/*.csv", header=True, schema=schema)
        except Exception:
            pass
            
        # 2. Read JSON files
        df_json = None
        try:
            df_json = spark.read.json(f"{INPUT_PATH}/*.json", schema=schema)
        except Exception:
            pass
            
        # 3. Combine if data exists
        if df_csv and df_json:
            df = df_csv.unionByName(df_json)
        elif df_csv:
            df = df_csv
        elif df_json:
            df = df_json
        else:
            print("No new CSV or JSON files found in input path.")
            return

        df = df.withColumn("ingestion_time", current_timestamp()) \
               .withColumn("source_file", input_file_name())
        
        row_count = df.count()
        if row_count > 0:
            write_delta(df, BRONZE_PATH, mode="overwrite")
            print(f"Bronze Ingestion Success: {row_count} rows ingested (CSV and/or JSON).")
        else:
            print("No new files found in input path.")
            
    except Exception as e:
        print(f"Bronze Ingestion Failed: {str(e)}")
        if own_spark: sys.exit(1)
        raise e
    finally:
        if own_spark: spark.stop()

if __name__ == "__main__":
    ingest_bronze()

import sys
import os
from pyspark.sql.functions import to_date, col
from pipeline.config import BRONZE_PATH, SILVER_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, read_delta, write_delta

def transform_silver(spark=None):
    own_spark = False
    if spark is None:
        spark = get_spark_session("SilverTransform")
        own_spark = True
        
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

    try:
        if not os.path.exists(BRONZE_PATH):
            print("Bronze path does not exist. Skipping Silver transform.")
            return

        bronze_df = read_delta(spark, BRONZE_PATH)
        
        # Deduplicate batch
        new_batch = bronze_df.dropDuplicates(["id"])
        
        final_df = new_batch.withColumn("processed_date", to_date("ingestion_time")) \
                            .fillna({"age": 45})
        
        row_count = final_df.count()
        if row_count > 0:
            write_delta(final_df, SILVER_PATH, mode="overwrite", partition_by="processed_date")
            print(f"Silver Transformation Success: {row_count} new rows processed.")
        else:
            print("No new data to process into Silver.")

    except Exception as e:
        print(f"Silver Transformation Failed: {str(e)}")
        if own_spark: sys.exit(1)
        raise e
    finally:
        if own_spark: spark.stop()

if __name__ == "__main__":
    transform_silver()

import sys
import os
from pyspark.sql.functions import avg, count
from pipeline.config import SILVER_PATH, GOLD_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, read_delta, write_delta

def generate_gold(spark=None):
    own_spark = False
    if spark is None:
        spark = get_spark_session("GoldMetrics")
        own_spark = True
        
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

    try:
        if not os.path.exists(SILVER_PATH):
            print("Silver path does not exist. Skipping Gold generation.")
            return

        silver_df = read_delta(spark, SILVER_PATH)
        
        # Business metrics aggregation
        gold_df = silver_df.groupBy("processed_date").agg(
            avg("age").alias("average_age"),
            count("*").alias("total_users")
        )
        
        write_delta(gold_df, GOLD_PATH, mode="overwrite", partition_by="processed_date")
        print("Gold Metrics Generation Success.")

    except Exception as e:
        print(f"Gold Metrics Generation Failed: {str(e)}")
        if own_spark: sys.exit(1)
        raise e
    finally:
        if own_spark: spark.stop()

if __name__ == "__main__":
    generate_gold()

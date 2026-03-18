import sys
from pyspark.sql.functions import col, count, when, sum as _sum
from pipeline.config import BRONZE_PATH, DQ_METRICS_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, read_delta, write_delta

def validate_data(spark=None):
    """
    Performs DQ checks on Bronze data before Silver transformation.
    Returns True if data quality is acceptable, False otherwise.
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("DataValidation")
        own_spark = True
        
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)

    try:
        df = read_delta(spark, BRONZE_PATH)
        total_rows = df.count()
        
        if total_rows == 0:
            print("Validation: No data found in Bronze.")
            return True

        # DQ Checks
        dq_results = df.select(
            _sum(when(col("id").isNull(), 1).otherwise(0)).alias("null_ids"),
            _sum(when(col("age") < 0, 1).otherwise(0)).alias("negative_ages"),
            _sum(when(col("age") > 120, 1).otherwise(0)).alias("invalid_ages")
        ).collect()[0]

        # Duplicate check
        dupe_count = total_rows - df.dropDuplicates(["id"]).count()

        # Prepare metrics dataframe
        from pyspark.sql.functions import current_timestamp
        metrics_data = [(
            total_rows,
            int(dq_results["null_ids"] or 0),
            int(dq_results["negative_ages"] or 0),
            int(dq_results["invalid_ages"] or 0),
            int(dupe_count)
        )]
        
        cols = ["total_rows", "null_ids", "negative_ages", "invalid_ages", "duplicate_ids"]
        metrics_df = spark.createDataFrame(metrics_data, cols) \
                          .withColumn("validation_time", current_timestamp())
        
        # Write DQ metrics (append for history)
        write_delta(metrics_df, DQ_METRICS_PATH, mode="append")
        
        # Validation Logic: Fail if 50% or more rows have Null IDs (Arbitrary threshold)
        null_id_pct = (dq_results["null_ids"] or 0) / total_rows
        if null_id_pct > 0.5:
            print(f"CRITICAL DQ FAILURE: {null_id_pct:.2%} of rows have Null IDs.")
            return False

        print(f"Validation Success: {total_rows} rows checked. [{dupe_count} duplicates, {dq_results['null_ids']} null IDs]")
        return True

    except Exception as e:
        print(f"Validation Error: {str(e)}")
        return False
    finally:
        if own_spark: spark.stop()

if __name__ == "__main__":
    if not validate_data():
        sys.exit(1)

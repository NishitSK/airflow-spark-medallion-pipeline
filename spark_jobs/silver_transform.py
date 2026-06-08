import sys
import os
from pyspark.sql.functions import to_date, col, regexp_replace, trim
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
        
        # Normalize whole numbers written as floats (e.g. 1001.0 or 1001.000 -> 1001)
        cleaned_id = regexp_replace(trim(col("id")), r"\.0+$", "")
        cleaned_age = regexp_replace(trim(col("age")), r"\.0+$", "")
        
        # Cast to IntegerType
        id_int = cleaned_id.cast("int")
        age_int = cleaned_age.cast("int")
        
        # Defensive validation: Count and log integer conversion failures for ID
        malformed_ids = bronze_df.filter(
            col("id").isNotNull() & (trim(col("id")) != "") & id_int.isNull()
        )
        malformed_count = malformed_ids.count()
        if malformed_count > 0:
            print(f"WARNING: {malformed_count} records had malformed IDs that failed conversion to integers.")
            sample_failures = malformed_ids.select("id", "name").limit(5).collect()
            print("Sample malformed IDs in Silver: " + ", ".join([f"'{r['id']}' ({r['name']})" for r in sample_failures]))

        # Defensive validation: Count and log integer conversion failures for age
        malformed_ages = bronze_df.filter(
            col("age").isNotNull() & (trim(col("age")) != "") & age_int.isNull()
        )
        malformed_age_count = malformed_ages.count()
        if malformed_age_count > 0:
            print(f"WARNING: {malformed_age_count} records had malformed ages that failed conversion to integers.")
            sample_age_failures = malformed_ages.select("age", "name").limit(5).collect()
            print("Sample malformed ages in Silver: " + ", ".join([f"'{r['age']}' ({r['name']})" for r in sample_age_failures]))

        # Transform and cast raw fields to clean Silver schema
        transformed_df = bronze_df.withColumn("id", id_int) \
                                   .withColumn("age", age_int)
        
        # Deduplicate batch on the normalized integer ID
        new_batch = transformed_df.dropDuplicates(["id"])
        
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

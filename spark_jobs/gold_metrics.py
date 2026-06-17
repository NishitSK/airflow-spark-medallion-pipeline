"""
Gold Metrics
============
Generates enriched Gold analytics including business KPIs based on dataset type.
Persists run scorecard report to DQ_REPORT_PATH.
"""
import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, lit, current_timestamp, sum as spark_sum, countDistinct, to_date, col
from pipeline.config import SILVER_PATH, GOLD_PATH, SPARK_LOG_LEVEL
from pipeline.delta_utils import get_spark_session, read_delta, write_delta


def generate_gold(spark=None, scorecard: dict = None, anomalies: list = None,
                  mappings: list = None, run_id: str = "unknown", runtime_seconds: float = 0.0,
                  silver_df=None, dataset_type="GENERIC"):
    """
    Generate enriched Gold metrics from Silver layer.
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("GoldMetrics")
        own_spark = True

    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    scorecard  = scorecard  or {}
    anomalies  = anomalies  or []
    mappings   = mappings   or []

    try:
        if silver_df is not None:
            df_to_use = silver_df
        else:
            if not os.path.exists(SILVER_PATH):
                print("[Gold] Silver path does not exist. Skipping Gold generation.")
                return None
            df_to_use = read_delta(spark, SILVER_PATH)

        # -- Business aggregations based on dataset type --
        if dataset_type == "CUSTOMER":
            agg_ops = [count("*").alias("total_users")]
            if "age" in df_to_use.columns:
                agg_ops.append(avg("age").alias("average_age"))
            business_df = df_to_use.groupBy("processed_date").agg(*agg_ops)
            write_delta(business_df, GOLD_PATH, mode="overwrite", partition_by="processed_date")

        elif dataset_type == "ORDERS":
            # order_id, product_name, quantity, unit_price
            agg_ops = []
            if "order_id" in df_to_use.columns:
                agg_ops.append(countDistinct("order_id").alias("total_orders"))
            else:
                agg_ops.append(lit(0).alias("total_orders"))
                
            if "quantity" in df_to_use.columns and "unit_price" in df_to_use.columns:
                # Sum of quantity * price
                revenue_expr = spark_sum(col("quantity") * col("unit_price"))
                agg_ops.append(revenue_expr.alias("total_revenue"))
                
                # Average order value
                avg_val_expr = avg(col("quantity") * col("unit_price"))
                agg_ops.append(avg_val_expr.alias("avg_order_value"))
            else:
                agg_ops.append(lit(0.0).alias("total_revenue"))
                agg_ops.append(lit(0.0).alias("avg_order_value"))

            business_df = df_to_use.groupBy("processed_date").agg(*agg_ops)
            write_delta(business_df, GOLD_PATH, mode="overwrite", partition_by="processed_date")

        else:  # GENERIC mode
            # Persist generic overview scorecard metrics in Gold table to avoid redundant aggregations
            generic_data = [{
                "total_rows": float(scorecard.get("total_rows", 0)),
                "total_columns": float(scorecard.get("total_columns", 0)),
                "duplicate_rate": float(scorecard.get("duplicate_rate", 0.0)),
                "completeness_score": float(scorecard.get("completeness_score", 100.0)),
                "column_metrics": json.dumps(scorecard.get("column_metrics", {}))
            }]
            business_df = spark.createDataFrame(generic_data) \
                .withColumn("processed_date", to_date(current_timestamp()))
            write_delta(business_df, GOLD_PATH, mode="overwrite", partition_by="processed_date")

        # -- Enriched run summary row in DQ report --
        anomaly_str  = "; ".join(anomalies)  if anomalies  else "None"
        mapping_str  = json.dumps(mappings)   if mappings   else "[]"

        summary_data = [{
            "run_id":           run_id,
            "source_file":      str(scorecard.get("source_file", "unknown")),
            "total_rows":       float(scorecard.get("total_rows", 0)),
            "valid_rows":       float(scorecard.get("valid_rows", 0)),
            "invalid_rows":     float(scorecard.get("invalid_rows", 0)),
            "dq_score":         float(scorecard.get("dq_score", 100.0)),
            "null_ids":         float(scorecard.get("null_ids", 0)),
            "malformed_ids":    float(scorecard.get("malformed_ids", 0)),
            "duplicate_ids":    float(scorecard.get("duplicate_ids", 0)),
            "null_ages":        float(scorecard.get("null_ages", 0)),
            "invalid_ages":     float(scorecard.get("invalid_ages", 0)),
            "null_names":       float(scorecard.get("null_names", 0)),
            "rows_received":    float(scorecard.get("total_rows", 0)),
            "rows_accepted":    float(scorecard.get("valid_rows", 0)),
            "rows_rejected":    float(scorecard.get("invalid_rows", 0)),
            "runtime_seconds":  float(round(runtime_seconds, 2)),
            "anomaly_flags":    anomaly_str,
            "schema_mappings":  mapping_str,
            "dataset_type":     dataset_type
        }]

        from pipeline.config import DQ_REPORT_PATH
        summary_df = spark.createDataFrame(summary_data) \
            .withColumn("processed_at", current_timestamp()) \
            .withColumn("report_time", current_timestamp())
        
        # MergeSchema implicitly handled by write_delta
        write_delta(summary_df, DQ_REPORT_PATH, mode="append")
        print(f"[Gold] Metrics generation complete. Run: {run_id} | Type: {dataset_type} | DQ Score: {scorecard.get('dq_score', 'N/A')}%")
        return business_df

    except Exception as e:
        print(f"[Gold] Metrics Generation Failed: {str(e)}")
        if own_spark:
            sys.exit(1)
        raise e
    finally:
        if own_spark:
            spark.stop()


if __name__ == "__main__":
    generate_gold()

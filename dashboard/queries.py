import os
import streamlit as st
from pipeline.config import BRONZE_PATH, SILVER_PATH, GOLD_PATH, TRACE_PATH, DQ_METRICS_PATH, METRICS_FILE
from pipeline.delta_utils import get_spark_session

@st.cache_resource
def get_spark():
    return get_spark_session("Dashboard")

def get_kpis(spark):
    try:
        runtime_val = "0.00"
        if os.path.exists(METRICS_FILE):
            with open(METRICS_FILE) as f:
                parts = f.read().strip().split(",")
                if len(parts) >= 2: runtime_val = f"{float(parts[1]):.2f}"

        total_unique = spark.read.format("delta").load(SILVER_PATH).count() if os.path.exists(SILVER_PATH) else 0
        return runtime_val, total_unique
    except:
        return "0.00", 0

def load_layer_data(spark, path):
    if os.path.exists(path):
        return spark.read.format("delta").load(path)
    return None

def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

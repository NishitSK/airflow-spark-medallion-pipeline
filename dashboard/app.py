import streamlit as st
import pandas as pd
import os
from queries import get_spark, get_kpis, load_layer_data, convert_df_to_csv
from charts import plot_age_distribution, plot_gold_trends
from pipeline.config import BRONZE_PATH, SILVER_PATH, GOLD_PATH, INPUT_PATH

st.set_page_config(
    page_title="Medallion Pipeline Dashboard",
    page_icon=":material/analytics:",
    layout="wide"
)

# Sidebar Design
with st.sidebar:
    st.title("Medallion Control")
    page = st.radio(
        "Navigation", 
        ["Medallion Dashboard", "Processed Files History", "Data Engineering Infographics"],
        index=0,
        label_visibility="collapsed"
    )
    
    st.divider()
    st.subheader("📤 Data Ingestion")
    uploaded_file = st.file_uploader("Upload CSV or JSON", type=['csv', 'json'])
    
    if uploaded_file is not None:
        save_path = os.path.join(INPUT_PATH, uploaded_file.name)
        with open(save_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.success(f"Saved: {uploaded_file.name}")
        
    if st.button("🚀 Run Pipeline Now", type="primary", use_container_width=True):
        with st.status("Executing Pipeline..."):
            st.write("Running Unified Medallion Process...")
            # We run it inside the container using the path /opt/airflow/... 
            # or use the local path if it's mapped correctly. 
            # In the container, airflow is at /opt/airflow.
            result = os.system("python /opt/airflow/spark_jobs/unified_pipeline.py")
            if result == 0:
                st.success("Pipeline Run Success!")
                st.rerun()
            else:
                st.error("Pipeline Run Failed. Check incidents log.")
    
    st.divider()
    st.subheader("📋 Download Test Examples")
    st.write("Use these to test Data Quality (DQ) features:")
    
    # Example CSV (1000s Range)
    example_csv = "id,name,age\n1001,Alice,25\n1002,Bob,-5\n,Charlie,30\n1004,David,150\n1005,Eve,22\n1001,Alice Duplicate,25"
    st.download_button(
        "Download Example CSV", 
        example_csv, 
        "test_csv_1000s.csv", 
        "text/csv", 
        use_container_width=True,
        help="Contains: ID 1001 (Dupe), ID Null, IDs 1002 & 1004 (Age Issues)"
    )
    
    # Example JSON (2000s Range)
    example_json = '{"id": 2001, "name": "Json Alice", "age": 28}\n{"id": 2002, "name": "Json Bob", "age": -10}\n{"id": null, "name": "Json Null", "age": 45}\n{"id": 2001, "name": "Json Duplicate", "age": 28}'
    st.download_button(
        "Download Example JSON", 
        example_json, 
        "test_json_2000s.json", 
        "application/json", 
        use_container_width=True,
        help="Contains: ID 2001 (Dupe), ID Null, ID 2002 (Neg Age)"
    )

    st.divider()
    st.caption("v2.7 - Test Examples Integration")

if page == "Medallion Dashboard":
    st.title("📊 Medallion Pipeline Stats")
    
    spark = get_spark()
    
    # KPI Row
    b_runtime, b_total = get_kpis(spark)
    with st.container(border=True):
        c1, c2 = st.columns(2)
        c1.metric("Pipeline Runtime", f"{b_runtime}s")
        c2.metric("Silver Inventory", f"{b_total:,}")
    
    # Medallion Tabs
    tabs = st.tabs([
        ":material/layers: Bronze (Raw)", 
        ":material/cleaning_services: Silver (Cleaned)", 
        ":material/trending_up: Gold (Trends)"
    ])
    
    with tabs[0]:
        with st.container(border=True):
            st.subheader("Raw Ingestion Feed")
            df = load_layer_data(spark, BRONZE_PATH)
            if df is not None:
                from pyspark.sql.functions import col
                total_raw = df.count()
                st.metric("Total Raw Records", f"{total_raw:,}")
                sorted_df = df.orderBy(col("ingestion_time").desc()).limit(100).toPandas()
                st.dataframe(sorted_df, use_container_width=True)
            else:
                st.info("Bronze data not available.", icon=":material/info:")
            
    with tabs[1]:
        df = load_layer_data(spark, SILVER_PATH)
        if df is not None:
            from pyspark.sql.functions import col
            df_sorted = df.orderBy(col("processed_date").desc(), col("id"))
            
            # Convert full dataframe to pandas for CSV download
            full_pdf = df_sorted.toPandas()
            total_cleaned = len(full_pdf)
            
            with st.container(border=True):
                st.metric("Total Cleaned Rows", f"{total_cleaned:,}")
                col_btn, col_stats = st.columns([1, 2])
                with col_btn:
                    csv_data = convert_df_to_csv(full_pdf)
                    st.download_button(
                        label="Download Full Silver CSV",
                        data=csv_data,
                        file_name="silver_cleaned_data_full.csv",
                        mime="text/csv",
                        icon=":material/download:",
                        type="primary"
                    )
                
                # Display limited dataframe in UI for performance
                display_pdf = full_pdf.head(1000)
                st.plotly_chart(plot_age_distribution(display_pdf), use_container_width=True)
                st.dataframe(display_pdf.head(100), use_container_width=True)
        else:
            st.info("Silver data not available.", icon=":material/info:")
            
    with tabs[2]:
        df = load_layer_data(spark, GOLD_PATH)
        if df:
            pdf = df.toPandas()
            with st.container(border=True):
                st.plotly_chart(plot_gold_trends(pdf), use_container_width=True)
                st.dataframe(pdf, use_container_width=True)
        else:
            st.info("Gold data not available.", icon=":material/info:")

elif page == "Processed Files History":
    from delta.tables import DeltaTable
    
    st.title(":material/history: Delta Transaction Log")
    st.write("Dynamic execution history retrieved from Delta Lake metadata.")
    
    spark = get_spark()
    
    try:
        dt_silver = DeltaTable.forPath(spark, SILVER_PATH)
        history_df = dt_silver.history().select("version", "timestamp", "operation", "operationMetrics").toPandas()
        
        history_df.sort_values("version", ascending=False, inplace=True)
        
        file_data = []
        for _, row in history_df.iterrows():
            metrics = row["operationMetrics"] if row["operationMetrics"] else {}
            output_rows = metrics.get('numOutputRows', 0)
            
            file_data.append({
                "Version": row["version"],
                "Timestamp": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(row["timestamp"]) else "",
                "Operation": row["operation"],
                "Rows Written": int(output_rows) if output_rows else 0,
                "Output Bytes": metrics.get('numOutputBytes', 0)
            })
            
        if file_data:
            with st.container(border=True):
                st.dataframe(pd.DataFrame(file_data), use_container_width=True)
            
            st.subheader("Processing Volume History")
            df_files = pd.DataFrame(file_data)
            df_files = df_files[df_files["Rows Written"] > 0]
            if not df_files.empty:
                with st.container(border=True):
                    st.bar_chart(df_files.set_index("Timestamp")["Rows Written"])
        else:
            st.info("No processing history found.", icon=":material/info:")
            
    except Exception as e:
        st.error(f"Error loading history: {str(e)}", icon=":material/error:")

elif page == "Data Engineering Infographics":
    from charts import plot_data_funnel, plot_pipeline_history, plot_dq_violations
    from delta.tables import DeltaTable
    from pipeline.config import DQ_METRICS_PATH
    from pyspark.sql.functions import col
    
    st.title(":material/insights: Pipeline Infographics")
    
    spark = get_spark()
    
    # Calculate counts
    bronze_cnt = spark.read.format("delta").load(BRONZE_PATH).count() if os.path.exists(BRONZE_PATH) else 0
    silver_cnt = spark.read.format("delta").load(SILVER_PATH).count() if os.path.exists(SILVER_PATH) else 0
    gold_cnt = spark.read.format("delta").load(GOLD_PATH).count() if os.path.exists(GOLD_PATH) else 0
    
    # Layout with DQ at top
    with st.container(border=True):
        st.subheader("Data Quality Pulse")
        try:
            dq_df = spark.read.format("delta").load(DQ_METRICS_PATH).orderBy(col("validation_time").desc()).limit(1).toPandas()
            if not dq_df.empty:
                latest = dq_df.iloc[0]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Null IDs", int(latest['null_ids']))
                c2.metric("Neg. Ages", int(latest['negative_ages']))
                c3.metric("Inv. Ages", int(latest['invalid_ages']))
                c4.metric("Duplicates", int(latest['duplicate_ids']))
                
                st.plotly_chart(plot_dq_violations(latest), use_container_width=True)
            else:
                st.info("No DQ metrics captured yet.", icon=":material/info:")
        except Exception:
            st.info("DQ metrics unavailable.", icon=":material/info:")

    col_funnel, col_history = st.columns(2)
    
    with col_funnel:
        with st.container(border=True):
            st.plotly_chart(plot_data_funnel(bronze_cnt, silver_cnt, gold_cnt), use_container_width=True)
        
    with col_history:
        try:
            dt_silver = DeltaTable.forPath(spark, SILVER_PATH)
            history_df = dt_silver.history().select("timestamp", "operationMetrics").toPandas()
            
            file_data = []
            for _, row in history_df.iterrows():
                metrics = row["operationMetrics"] if row["operationMetrics"] else {}
                output_rows = metrics.get('numOutputRows', 0)
                file_data.append({
                    "Timestamp": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(row["timestamp"]) else "",
                    "Cleaned Rows Written": int(output_rows) if output_rows else 0
                })
            
            df_hist = pd.DataFrame(file_data)
            fig = plot_pipeline_history(df_hist)
            with st.container(border=True):
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No volume history available.", icon=":material/info:")
        except Exception:
            st.info("Volume history unavailable.", icon=":material/info:")


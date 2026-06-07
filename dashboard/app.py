import streamlit as st
import pandas as pd
import os
import time
from queries import get_spark, get_kpis, load_layer_data, convert_df_to_csv, get_consolidated_status, generate_sample_datasets, get_file_metadata
from charts import plot_age_distribution, plot_gold_trends
from pipeline.config import BRONZE_PATH, SILVER_PATH, GOLD_PATH, INPUT_PATH

# Generate built-in samples on startup
try:
    generate_sample_datasets()
except Exception as e:
    st.sidebar.error(f"Failed to generate samples: {e}")


# Initialize session state for Airflow settings
if "use_airflow_api" not in st.session_state:
    st.session_state.use_airflow_api = True
if "airflow_api_url" not in st.session_state:
    st.session_state.airflow_api_url = "http://airflow:8080"
if "airflow_username" not in st.session_state:
    st.session_state.airflow_username = "admin"
if "airflow_password" not in st.session_state:
    st.session_state.airflow_password = "admin"


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
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
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
    with st.expander("⚙️ Airflow Connection Settings"):
        st.session_state.use_airflow_api = st.checkbox("Enable Airflow API Sync", value=st.session_state.use_airflow_api)
        st.session_state.airflow_api_url = st.text_input("Airflow API URL", value=st.session_state.airflow_api_url)
        st.session_state.airflow_username = st.text_input("API Username", value=st.session_state.airflow_username)
        st.session_state.airflow_password = st.text_input("API Password", value=st.session_state.airflow_password, type="password")
        
        if st.button("🔌 Test Connection", use_container_width=True):
            from airflow_client import test_airflow_connection
            success, msg = test_airflow_connection(
                st.session_state.airflow_api_url,
                st.session_state.airflow_username,
                st.session_state.airflow_password
            )
            if success:
                st.success(msg)
            else:
                st.error(msg)

    st.divider()
    st.caption("v2.8 - Live Pipeline Status Integration")

# Resolve Airflow credentials from session state
api_url = st.session_state.airflow_api_url if st.session_state.use_airflow_api else None
username = st.session_state.airflow_username
password = st.session_state.airflow_password

status, last_success, last_file, error_msg, status_src, stage, duration = get_consolidated_status(api_url, username, password)

# Check for completion notification toast
if "last_status" not in st.session_state:
    st.session_state.last_status = status

if st.session_state.last_status == "Pipeline running" and status == "Pipeline completed":
    st.toast("🎉 Pipeline run succeeded! Fresh gold insights are now available.", icon="📈")
st.session_state.last_status = status

# Styles and Badge
st.markdown(
    """
    <style>
    @keyframes pulse {
        0% { opacity: 0.6; }
        50% { opacity: 1.0; }
        100% { opacity: 0.6; }
    }
    .status-badge {
        padding: 6px 12px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 13px;
        display: inline-flex;
        align-items: center;
        gap: 6px;
    }
    .pulse-animation {
        animation: pulse 2.5s infinite;
    }
    </style>
    """,
    unsafe_allow_html=True
)

status_styles = {
    "Waiting for file": "background-color: rgba(71, 85, 105, 0.15); border: 1px solid rgba(71, 85, 105, 0.4); color: #94a3b8;",
    "Pipeline running": "background-color: rgba(234, 179, 8, 0.15); border: 1px solid rgba(234, 179, 8, 0.5); color: #facc15;",
    "Pipeline completed": "background-color: rgba(34, 197, 94, 0.15); border: 1px solid rgba(34, 197, 94, 0.5); color: #4ade80;",
    "Pipeline failed": "background-color: rgba(239, 68, 68, 0.15); border: 1px solid rgba(239, 68, 68, 0.5); color: #f87171;"
}
status_icons = {
    "Waiting for file": "📥",
    "Pipeline running": "⚙️",
    "Pipeline completed": "✅",
    "Pipeline failed": "❌"
}

style = status_styles.get(status, status_styles["Waiting for file"])
icon = status_icons.get(status, "❓")
pulse_class = "pulse-animation" if status == "Pipeline running" else ""

def render_timeline_html(status, stage):
    stages = ["Upload", "Bronze Ingestion", "Silver Transformation", "Gold Aggregation"]
    states = [2, 0, 0, 0] # Upload is always completed
    
    if status == "Pipeline running":
        if stage == "Bronze":
            states[1] = 1
        elif stage == "Validation":
            states[1] = 2
            states[2] = 1
        elif stage == "Silver":
            states[1] = 2
            states[2] = 1
        elif stage == "Gold":
            states[1] = 2
            states[2] = 2
            states[3] = 1
    elif status == "Pipeline completed":
        states[1] = 2
        states[2] = 2
        states[3] = 2
    elif status == "Pipeline failed":
        if stage == "Bronze":
            states[1] = 3
        elif stage == "Validation":
            states[1] = 2
            states[2] = 3
        elif stage == "Silver":
            states[1] = 2
            states[2] = 3
        elif stage == "Gold":
            states[1] = 2
            states[2] = 2
            states[3] = 3
            
    html = '<div style="display: flex; align-items: center; justify-content: space-between; margin: 15px 0; font-family: sans-serif; overflow-x: auto; padding: 12px 10px; background: rgba(15, 23, 42, 0.3); border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.05);">'
    
    colors = {
        0: {"bg": "#1e293b", "border": "#334155", "text": "#64748b", "label": "Queued"},
        1: {"bg": "rgba(234, 179, 8, 0.1)", "border": "#eab308", "text": "#facc15", "label": "Processing..."},
        2: {"bg": "rgba(34, 197, 94, 0.1)", "border": "#22c55e", "text": "#4ade80", "label": "Completed"},
        3: {"bg": "rgba(239, 68, 68, 0.1)", "border": "#ef4444", "text": "#f87171", "label": "Failed"}
    }
    
    icons_list = {
        0: "⏳",
        1: "⚙️",
        2: "✅",
        3: "❌"
    }
    
    for i, name in enumerate(stages):
        state = states[i]
        c = colors[state]
        ic = icons_list[state]
        pulse = "pulse-animation" if state == 1 else ""
        
        html += f'''
        <div style="text-align: center; flex: 1; min-width: 120px; position: relative; padding: 0 10px;">
            <div class="{pulse}" style="width: 36px; height: 36px; border-radius: 50%; background: {c["bg"]}; border: 2px solid {c["border"]}; display: flex; align-items: center; justify-content: center; margin: 0 auto 6px auto; color: {c["text"]}; font-size: 15px;">
                {ic}
            </div>
            <div style="font-weight: 600; font-size: 12px; color: {c["text"]}; white-space: nowrap;">{name}</div>
            <div style="font-size: 10px; color: #64748b; margin-top: 1px;">{c["label"]}</div>
        </div>
        '''
        
        if i < len(stages) - 1:
            line_color = "#22c55e" if states[i+1] == 2 else ("#ef4444" if states[i+1] == 3 else ("#eab308" if states[i+1] == 1 else "#334155"))
            html += f'''
            <div style="flex-grow: 1; height: 2px; background: {line_color}; margin-top: -22px; min-width: 15px;"></div>
            '''
            
    html += '</div>'
    return html

# Layout the status header
with st.container(border=True):
    col_status, col_file, col_time = st.columns([1, 1, 1])
    with col_status:
        st.markdown(f'**Pipeline Status:**  \n<div class="status-badge {pulse_class}" style="{style}">{icon} {status}</div>', unsafe_allow_html=True)
        if status == "Pipeline failed" and error_msg:
            st.caption(f"⚠️ `{error_msg}`")
        else:
            st.caption(f"Sync: `{status_src}`")
    with col_file:
        st.markdown(f"**Last Ingested File:**  \n`{last_file}`")
    with col_time:
        st.markdown(f"**Last Successful Run:**  \n`{last_success}`")

st.markdown(render_timeline_html(status, stage), unsafe_allow_html=True)
st.divider()

if page == "Medallion Dashboard":
    st.title("📊 Medallion Pipeline Stats")
    
    spark = get_spark()
    
    # 1. Quick Start: Load Sample Data
    with st.container(border=True):
        st.subheader("🚀 Quick Start: Load Built-in Sample Data")
        st.write(
            "Load built-in mock datasets instantly to see the pipeline in action. "
            "Clicking a button copies the CSV to `/data/input` and requests an Airflow run."
        )
        
        from pipeline.config import SAMPLES_PATH, INPUT_PATH
        import shutil
        
        def load_sample(size_label, file_name):
            src = os.path.join(SAMPLES_PATH, file_name)
            dest = os.path.join(INPUT_PATH, file_name)
            try:
                os.makedirs(INPUT_PATH, exist_ok=True)
                shutil.copy(src, dest)
                st.session_state.last_uploaded_file = file_name
                
                api_url = st.session_state.airflow_api_url if st.session_state.use_airflow_api else None
                username = st.session_state.airflow_username
                password = st.session_state.airflow_password
                
                triggered = False
                msg = ""
                if api_url:
                    from airflow_client import trigger_airflow_dag
                    triggered, msg = trigger_airflow_dag(api_url, username, password)
                
                if triggered:
                    st.toast(f"Successfully loaded {size_label} dataset and triggered DAG run!", icon="🚀")
                    st.success(f"Loaded {size_label} dataset and successfully triggered Airflow DAG! ({msg})")
                else:
                    st.toast(f"Sample copied to input folder.", icon="📥")
                    st.info(f"Loaded {size_label} dataset to `/data/input`. (Bypassed API sync: {msg or 'API offline'})")
                    st.warning("Airflow API is offline or disabled. Airflow will pick up the file on its next periodic sensor poke, or click 'Run Pipeline Now' in the sidebar.")
                
                time.sleep(1)
                st.rerun()
            except Exception as ex:
                st.error(f"Failed to load sample: {str(ex)}")

        col_btn1, col_btn2, col_btn3 = st.columns(3)
        with col_btn1:
            st.button("📈 Load Small CSV (100 rows)", use_container_width=True, on_click=load_sample, args=("Small CSV", "small_sample.csv"), help="Contains duplicates & negative ages to test DQ rules.")
        with col_btn2:
            st.button("📊 Load Medium CSV (10K rows)", use_container_width=True, on_click=load_sample, args=("Medium CSV", "medium_sample.csv"), help="Tests intermediate PySpark aggregation performance.")
        with col_btn3:
            st.button("⚡ Load Large CSV (100K+ rows)", use_container_width=True, on_click=load_sample, args=("Large CSV", "large_sample.csv"), help="Demonstrates high-volume Delta Lake processing scales.")
            
        col_json1, col_json2, _ = st.columns(3)
        with col_json1:
            st.button("📋 Load Small JSON (100 rows)", use_container_width=True, on_click=load_sample, args=("Small JSON", "small_sample.json"), help="Newline-delimited JSON format testing DQ features.")
        with col_json2:
            st.button("📂 Load Medium JSON (10K rows)", use_container_width=True, on_click=load_sample, args=("Medium JSON", "medium_sample.json"), help="Newline-delimited JSON format testing Spark aggregations.")

            
    # 2. Dataset Information & System Health (2 columns)
    col_info, col_health = st.columns(2)
    
    with col_info:
        with st.container(border=True):
            st.subheader("ℹ️ Dataset Information")
            
            # Find the path of the last file
            last_file_path = None
            for folder in [INPUT_PATH, ARCHIVE_PATH]:
                if last_file and last_file != "None":
                    fp = os.path.join(folder, last_file)
                    if os.path.exists(fp):
                        last_file_path = fp
                        break
            
            file_size, record_count = 0.0, 0
            if last_file_path:
                file_size, record_count = get_file_metadata(last_file_path)
                
            st.markdown(f"**Active File:** `{last_file}`")
            st.markdown(f"**Total Records:** `{record_count:,}`")
            st.markdown(f"**Size on Disk:** `{file_size:.4f} MB`")
            
            loc = "Input Ingest (/data/input)" if status == "Pipeline running" else "Archive Folder (/data/archive)"
            st.markdown(f"**File Location:** `{loc}`")
            
    with col_health:
        with st.container(border=True):
            st.subheader("🏥 System Health")
            
            # Airflow API Health
            airflow_active = False
            if st.session_state.use_airflow_api:
                from airflow_client import test_airflow_connection
                airflow_active, _ = test_airflow_connection(
                    st.session_state.airflow_api_url,
                    st.session_state.airflow_username,
                    st.session_state.airflow_password
                )
            airflow_text = "🟢 Active (REST API)" if airflow_active else "🔴 Offline (Using File Fallback)"
            
            # Spark Health
            spark_active = spark is not None
            spark_text = "🟢 Active (PySpark Catalogs)" if spark_active else "🔴 Offline"
            
            # Data Lake health
            lake_ok = os.path.exists(GOLD_PATH) and os.path.exists(SILVER_PATH)
            lake_text = "🟢 Active (Gold Lakehouse Available)" if lake_ok else "🔴 Missing Lake Tables"
            
            st.markdown(f"**Airflow Sync Status:** {airflow_text}")
            st.markdown(f"**Spark SQL Cluster:** {spark_text}")
            st.markdown(f"**Delta Storage Lake:** {lake_text}")
            
            from datetime import datetime
            st.caption(f"Last Health Check: `{datetime.now().strftime('%H:%M:%S')}`")
            
    st.divider()
    
    # 3. KPI Row & Processing metrics
    st.subheader("📋 Pipeline Monitoring")
    b_runtime, b_total = get_kpis(spark)
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Pipeline Runtime", f"{b_runtime}s")
        c2.metric("Silver Inventory", f"{b_total:,}")
        
        # Duration from consolidated status
        c3.metric("Last Run Duration", f"{duration}s" if duration != "N/A" else "N/A")
        
        # Current stage indicator
        c4.metric("Active Stage", f"{stage}")
    
    # Medallion Tabs
    tabs = st.tabs([
        ":layers: Bronze (Raw)", 
        ":cleaning_services: Silver (Cleaned)", 
        ":trending_up: Gold (Trends)"
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
            
            full_pdf = df_sorted.toPandas()
            total_cleaned = len(full_pdf)
            
            with st.container(border=True):
                col_btn, col_slider = st.columns([1, 2])
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
                
                with col_slider:
                    age_range = st.slider("Interactive Filter: Select Age Range", 0, 120, (18, 65), key="silver_age_slider")
                
                # Apply filter to display dataset
                filtered_pdf = full_pdf[(full_pdf['age'] >= age_range[0]) & (full_pdf['age'] <= age_range[1])]
                
                # Display Summary Statistics
                c_rows, c_avg, c_min, c_max = st.columns(4)
                c_rows.metric("Filtered Records", f"{len(filtered_pdf):,}")
                c_avg.metric("Average Filtered Age", f"{filtered_pdf['age'].mean():.1f} yrs" if len(filtered_pdf) > 0 else "N/A")
                c_min.metric("Minimum Filtered Age", f"{int(filtered_pdf['age'].min())} yrs" if len(filtered_pdf) > 0 else "N/A")
                c_max.metric("Maximum Filtered Age", f"{int(filtered_pdf['age'].max())} yrs" if len(filtered_pdf) > 0 else "N/A")
                
                # Display limited dataframe in UI for performance
                display_pdf = filtered_pdf.head(1000)
                st.plotly_chart(plot_age_distribution(display_pdf), use_container_width=True)
                st.dataframe(display_pdf.head(100), use_container_width=True)
        else:
            st.info("Silver data not available.", icon=":material/info:")
            
    with tabs[2]:
        df = load_layer_data(spark, GOLD_PATH)
        if df:
            pdf = df.toPandas()
            with st.container(border=True):
                # Summary Statistics for Gold
                g1, g2, g3 = st.columns(3)
                g1.metric("Average Gold Age", f"{pdf['average_age'].mean():.1f} yrs" if not pdf.empty else "N/A")
                g2.metric("Total Gold Users", f"{pdf['total_users'].sum():,}" if not pdf.empty else "N/A")
                g3.metric("Aggregated Process Dates", f"{len(pdf)}" if not pdf.empty else "N/A")
                
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

# Automatic Refresh Loop
if status == "Pipeline running":
    st.info("🔄 Pipeline is processing. This page will refresh automatically every 5 seconds.")
    time.sleep(5)
    st.rerun()



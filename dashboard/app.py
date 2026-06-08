import streamlit as st
import pandas as pd
import os
import time
import shutil
from datetime import datetime

from queries import (
    get_spark, 
    get_kpis, 
    load_layer_data, 
    convert_df_to_csv, 
    get_consolidated_status, 
    generate_sample_datasets, 
    get_file_metadata, 
    get_file_preview, 
    get_pipeline_history,
    get_last_uploaded_file,
    get_dq_audit_details,
    get_dq_trends,
    get_incidents
)
from charts import (
    plot_age_distribution, 
    plot_gold_trends, 
    plot_data_funnel, 
    plot_pipeline_history, 
    plot_dq_violations,
    plot_dq_trends
)
from pipeline.config import (
    BRONZE_PATH, 
    SILVER_PATH, 
    GOLD_PATH, 
    INPUT_PATH, 
    SAMPLES_PATH, 
    HISTORY_FILE, 
    DQ_METRICS_PATH, 
    ARCHIVE_PATH,
    STATUS_FILE,
    TRACE_PATH
)

st.set_page_config(
    page_title="Medallion Pipeline Dashboard",
    page_icon="📊",
    layout="wide"
)

# Initialize sample datasets on startup
try:
    generate_sample_datasets()
except:
    pass

# Initialize session state for Airflow settings
if "use_airflow_api" not in st.session_state:
    st.session_state.use_airflow_api = True
if "airflow_api_url" not in st.session_state:
    st.session_state.airflow_api_url = "http://airflow:8080"
if "airflow_username" not in st.session_state:
    st.session_state.airflow_username = "admin"
if "airflow_password" not in st.session_state:
    st.session_state.airflow_password = "admin"

# Sidebar Design
with st.sidebar:
    st.title("🛡️ Pipeline Control")
    st.caption("Lakehouse Orchestrator")
    st.divider()
    
    page = st.radio(
        "Navigation", 
        ["Pipeline Dashboard", "Delta Lake Transaction Log", "Data Quality & Observability"],
        index=0
    )
    
    st.divider()
    with st.expander("⚙️ Connection Settings"):
        st.session_state.use_airflow_api = st.checkbox("Enable Orchestrator Sync", value=st.session_state.use_airflow_api)
        st.session_state.airflow_api_url = st.text_input("Scheduler API URL", value=st.session_state.airflow_api_url)
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
                st.success("Successfully connected to the pipeline scheduler REST API.")
            else:
                st.error("Failed to connect to the pipeline scheduler. Verify connection settings and logs.")

    st.divider()
    st.caption("Production Build: v3.0")

# Fetch Connection Settings
api_url = st.session_state.airflow_api_url if st.session_state.use_airflow_api else None
username = st.session_state.airflow_username
password = st.session_state.airflow_password

# Retrieve status from consolidated pipeline states
status, last_success, last_file, error_msg, status_src, stage, duration = get_consolidated_status(api_url, username, password)

# Completion notify check
if "last_status" not in st.session_state:
    st.session_state.last_status = status

if st.session_state.last_status == "Pipeline running" and status == "Pipeline completed":
    st.toast("🎉 Ingestion run successfully completed! Lakehouse tables updated.", icon="✅")
st.session_state.last_status = status

# Setup Spark
spark = get_spark()

# Retrieve latest metrics
b_runtime, b_total = get_kpis(spark)
history_records = get_pipeline_history(spark)
latest_run_rows = history_records[0].get("rows", 0) if history_records else 0

# Convert status to display state
status_indicator = "🟢 Healthy"
if status == "Pipeline running":
    status_indicator = "🟡 Running"
elif status == "Pipeline failed":
    status_indicator = "🔴 Failed"

# ----------------- TOP METRIC SECTION -----------------
with st.container(border=True):
    col_status, col_file, col_time, col_dur, col_rows = st.columns(5)
    with col_status:
        st.metric(label="Pipeline Status", value=status_indicator)
    with col_file:
        st.metric(label="Last Processed File", value=last_file[:25] if last_file else "None")
    with col_time:
        st.metric(label="Last Successful Run", value=last_success if last_success != "N/A" else "N/A")
    with col_dur:
        dur_val = f"{duration:.2f}s" if isinstance(duration, (int, float)) else (f"{float(duration):.2f}s" if (duration != "N/A" and duration is not None and str(duration).replace('.', '', 1).isdigit()) else "N/A")
        st.metric(label="Processing Duration", value=dur_val)
    with col_rows:
        st.metric(label="Records Processed", value=f"{latest_run_rows:,}" if (latest_run_rows is not None and isinstance(latest_run_rows, (int, float))) else "0")

# ----------------- LIVE PROCESSING PROGRESS -----------------
if status == "Pipeline running":
    st.divider()
    st.info("🟢 **Live Monitoring**\nRefreshing every 10 seconds")
    
    # Map raw stage to standard pipeline steps: Queued -> Bronze -> Silver -> Gold -> Completed
    current_stage = "Queued"
    if "queued" in status_src.lower():
        current_stage = "Queued"
    elif stage == "Waiting" or not stage:
        current_stage = "Queued"
    elif stage in ["Bronze", "Validation"]:
        current_stage = "Bronze"
    elif stage == "Silver":
        current_stage = "Silver"
    elif stage == "Gold":
        current_stage = "Gold"
    elif stage in ["Finished", "Completed"]:
        current_stage = "Completed"
    else:
        current_stage = "Queued"

    stage_pcts = {
        "Queued": 20,
        "Bronze": 40,
        "Silver": 60,
        "Gold": 80,
        "Completed": 100
    }
    pct = stage_pcts.get(current_stage, 20)
    st.progress(pct)
    
    with st.status(f"Pipeline Processing (Active Stage: {current_stage})", expanded=True) as status_container:
        q_icon = "🟢 Completed" if current_stage in ["Bronze", "Silver", "Gold", "Completed"] else "🟡 Running"
        st.markdown(f"**{q_icon}: Queued**")
        st.write("↓")
        
        b_icon = "🟢 Completed" if current_stage in ["Silver", "Gold", "Completed"] else ("🟡 Running" if current_stage == "Bronze" else "⚪ Pending")
        st.markdown(f"**{b_icon}: Bronze (Raw Ingest & Quality Rules)**")
        st.write("↓")
        
        s_icon = "🟢 Completed" if current_stage in ["Gold", "Completed"] else ("🟡 Running" if current_stage == "Silver" else "⚪ Pending")
        st.markdown(f"**{s_icon}: Silver (Transform & Cleanse)**")
        st.write("↓")
        
        g_icon = "🟢 Completed" if current_stage == "Completed" else ("🟡 Running" if current_stage == "Gold" else "⚪ Pending")
        st.markdown(f"**{g_icon}: Gold (KPI Aggregations)**")
        st.write("↓")
        
        c_icon = "🟢 Completed" if current_stage == "Completed" else "⚪ Pending"
        st.markdown(f"**{c_icon}: Completed**")

# ----------------- ERROR HANDLING PANEL -----------------
def get_user_friendly_error(err):
    if not err:
        return "An unknown error occurred.", "Verify that the pipeline is deployed and folders are accessible."
    err_lower = str(err).lower()
    if "not found" in err_lower or "does not exist" in err_lower or "no such file" in err_lower or "missing" in err_lower:
        return (
            "A required storage path or Delta table directory is unavailable.",
            "Verify that your local storage directories under data/ are mounted and writable. Ensure a dataset has been ingested first."
        )
    elif "halted due to critical data quality" in err_lower or "critical dq failure" in err_lower or "duplicate" in err_lower:
        return (
            "Halted due to critical Data Quality violations in raw data.",
            "Verify that your input file does not contain duplicate IDs, null keys, or negative ages. Run the 'Medium Sample' to test clean execution."
        )
    elif "connection" in err_lower or "refused" in err_lower or "unreachable" in err_lower:
        return (
            "Unable to connect to the scheduling system or Spark cluster.",
            "Check that the scheduler services and local cluster configurations are running."
        )
    return (
        "An unexpected PySpark cluster or Delta Lake table write error occurred.",
        "Check that the input format matches the expected columns (id, name, age) and that Spark cluster memory is healthy."
    )

if status == "Pipeline failed" and error_msg:
    reason, fix = get_user_friendly_error(error_msg)
    with st.container(border=True):
        st.error("❌ Pipeline Failed")
        st.markdown(f"**Reason:**\n{reason}")
        st.markdown(f"**Suggested Fix:**\n{fix}")

# ----------------- RENDER NAVIGATION PAGES -----------------
if page == "Pipeline Dashboard":
    st.divider()
    
    # Ingestion Tabs in Main Body
    st.subheader("📥 Data Ingestion Control")
    tab_upload, tab_sample = st.tabs(["Upload Dataset", "Use Sample Dataset"])
    
    with tab_upload:
        uploaded_file = st.file_uploader("Drop CSV or JSON format file here", type=['csv', 'json'])
        if uploaded_file is not None:
            # Save file temporarily to extract metadata and render preview
            temp_dir = os.path.join(INPUT_PATH, "temp")
            os.makedirs(temp_dir, exist_ok=True)
            temp_path = os.path.join(temp_dir, uploaded_file.name)
            
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
                
            # Fetch Metadata
            sz_mb, rows, cols = get_file_metadata(temp_path)
            
            # Display metadata summary
            st.markdown(f"**File Name:** `{uploaded_file.name}`")
            col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
            col_m1.metric("Rows", f"{rows:,}")
            col_m2.metric("Columns", f"{cols}")
            col_m3.metric("Type", uploaded_file.name.split('.')[-1].upper())
            col_m4.metric("Size", f"{sz_mb:.4f} MB")
            col_m5.metric("Est. Processing Cost", f"${rows * 0.00001:.5f} USD")
            
            # Preview Frame
            st.markdown("**Dataset Preview (First 10 records):**")
            preview_df = get_file_preview(temp_path)
            if preview_df is not None:
                st.dataframe(preview_df, use_container_width=True)
            else:
                st.info("Preview unavailable for this schema.")
                
            if st.button("Process Dataset", type="primary", use_container_width=True):
                # Copy file from temp to final input folder
                final_path = os.path.join(INPUT_PATH, uploaded_file.name)
                shutil.move(temp_path, final_path)
                st.session_state.last_uploaded_file = uploaded_file.name
                
                # Trigger pipeline
                triggered = False
                msg = ""
                if api_url:
                    from airflow_client import trigger_airflow_dag
                    triggered, msg = trigger_airflow_dag(api_url, username, password)
                    
                if triggered:
                    st.success(f"File uploaded and Pipeline Scheduler triggered successfully!")
                else:
                    st.info(f"File saved to input directory. Sync bypassed: {msg or 'API offline/disabled'}.")
                
                time.sleep(1)
                st.rerun()
                
    with tab_sample:
        st.write("Load one of the project's pre-packaged datasets directly into the pipeline:")
        
        # Loader trigger helper
        def run_sample(label, file):
            src = os.path.join(SAMPLES_PATH, file)
            dest = os.path.join(INPUT_PATH, file)
            try:
                os.makedirs(INPUT_PATH, exist_ok=True)
                shutil.copy(src, dest)
                st.session_state.last_uploaded_file = file
                
                triggered = False
                msg = ""
                if api_url:
                    from airflow_client import trigger_airflow_dag
                    triggered, msg = trigger_airflow_dag(api_url, username, password)
                    
                if triggered:
                    st.toast(f"Loading {label} sample dataset...", icon="🚀")
                else:
                    st.toast(f"{label} dataset placed in input directory.", icon="📥")
                    
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to load sample: {str(e)}")

        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            with st.container(border=True):
                st.markdown("##### 📈 Small Sample (100 rows)")
                st.write("Contains duplicate entries and null rows to evaluate Data Quality validation blocks.")
                st.write("**Est. Ingestion Time:** `~2 seconds`")
                st.write("**Processing Cost:** `$0.001 USD`")
                st.divider()
                st.button("Load Small CSV", type="secondary", use_container_width=True, on_click=run_sample, args=("Small CSV", "small_sample.csv"))
                st.button("Load Small JSON", type="secondary", use_container_width=True, on_click=run_sample, args=("Small JSON", "small_sample.json"))
                
        with col_s2:
            with st.container(border=True):
                st.markdown("##### 📊 Medium Sample (10K rows)")
                st.write("Clean records demonstrating Spark execution metrics and cleansing workflows.")
                st.write("**Est. Ingestion Time:** `~5 seconds`")
                st.write("**Processing Cost:** `$0.10 USD`")
                st.divider()
                st.button("Load Medium CSV", type="secondary", use_container_width=True, on_click=run_sample, args=("Medium CSV", "medium_sample.csv"))
                st.button("Load Medium JSON", type="secondary", use_container_width=True, on_click=run_sample, args=("Medium JSON", "medium_sample.json"))
                
        with col_s3:
            with st.container(border=True):
                st.markdown("##### ⚡ Large Sample (100K rows)")
                st.write("Large data block designed to evaluate PySpark Delta Lake scaling limits.")
                st.write("**Est. Ingestion Time:** `~15 seconds`")
                st.write("**Processing Cost:** `$1.00 USD`")
                st.divider()
                st.button("Load Large CSV", type="secondary", use_container_width=True, on_click=run_sample, args=("Large CSV", "large_sample.csv"))

    # System Health Panel & Dataset Information
    st.divider()
    col_system, col_info = st.columns(2)
    with col_system:
        with st.container(border=True):
            st.subheader("🏥 System Health Monitor")
            
            # Health check variables
            airflow_status = "🔴 Disconnected"
            if st.session_state.use_airflow_api:
                from airflow_client import test_airflow_connection
                success, _ = test_airflow_connection(
                    st.session_state.airflow_api_url,
                    st.session_state.airflow_username,
                    st.session_state.airflow_password
                )
                if success:
                    airflow_status = "🟢 Connected"
            
            spark_status = "🟢 Available" if spark is not None else "🔴 Unavailable"
            input_writable = "🟢 Accessible" if os.access(INPUT_PATH, os.W_OK) else "🔴 Inaccessible"
            
            output_dir = os.path.dirname(STATUS_FILE)
            output_writable = "🟢 Accessible" if os.access(output_dir, os.W_OK) else "🔴 Inaccessible"
            
            st.markdown(f"**Airflow Connection:** {airflow_status}")
            st.markdown(f"**Spark Availability:** {spark_status}")
            st.markdown(f"**Input Folder Access:** {input_writable}")
            st.markdown(f"**Output Folder Access:** {output_writable}")

    with col_info:
        with st.container(border=True):
            st.subheader("ℹ️ Dataset Information")
            
            active_file = get_last_uploaded_file()
            rows_val, cols_val, type_val, cost_val, time_val = "N/A", "N/A", "N/A", "N/A", "N/A"
            
            if active_file:
                filepath = os.path.join(INPUT_PATH, active_file)
                if not os.path.exists(filepath):
                    filepath = os.path.join(ARCHIVE_PATH, active_file)
                
                if os.path.exists(filepath):
                    sz_mb, r_cnt, c_cnt = get_file_metadata(filepath)
                    rows_val = f"{r_cnt:,}"
                    cols_val = f"{c_cnt}"
                    type_val = active_file.split('.')[-1].upper() if '.' in active_file else "Unknown"
                    cost_val = f"${max(0.001, r_cnt * 0.00001):.4f} USD"
                    time_val = f"~{max(2, int(r_cnt / 10000 * 1.5))} seconds"
                    
            st.markdown(f"**Active File:** `{active_file or 'None'}`")
            st.markdown(f"**Rows:** `{rows_val}`")
            st.markdown(f"**Columns:** `{cols_val}`")
            st.markdown(f"**File Type:** `{type_val}`")
            st.markdown(f"**Estimated Processing Cost:** `{cost_val}`")
            st.markdown(f"**Estimated Processing Time:** `{time_val}`")

    st.divider()
    
    # Medallion Tabs
    st.subheader("📊 Lakehouse Visualizations")
    tabs = st.tabs([
        "Bronze Layer (Raw)", 
        "Silver Layer (Cleaned)", 
        "Gold Layer (Metrics)"
    ])
    
    with tabs[0]:
        with st.container(border=True):
            st.subheader("Raw Ingest Inbound Log")
            df = load_layer_data(spark, BRONZE_PATH)
            if df is not None:
                from pyspark.sql.functions import col
                total_raw = df.count()
                st.metric("Total Ingested Records", f"{total_raw:,}")
                sorted_df = df.orderBy(col("ingestion_time").desc()).limit(100).toPandas()
                st.dataframe(sorted_df, use_container_width=True)
            else:
                st.info("Bronze layer data is currently empty.")
            
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
                        label="Download Silver Table CSV",
                        data=csv_data,
                        file_name="silver_table_cleaned.csv",
                        mime="text/csv",
                        type="primary"
                    )
                with col_slider:
                    age_range = st.slider("Interactive Filter: Select Age Scope", 0, 120, (18, 65), key="silver_age_slider")
                
                filtered_pdf = full_pdf[(full_pdf['age'] >= age_range[0]) & (full_pdf['age'] <= age_range[1])]
                
                # Statistics Metrics
                m_avg, m_min, m_max = st.columns(3)
                m_avg.metric("Average User Age", f"{filtered_pdf['age'].mean():.1f} yrs" if len(filtered_pdf) > 0 else "N/A")
                m_min.metric("Minimum User Age", f"{int(filtered_pdf['age'].min())} yrs" if len(filtered_pdf) > 0 else "N/A")
                m_max.metric("Maximum User Age", f"{int(filtered_pdf['age'].max())} yrs" if len(filtered_pdf) > 0 else "N/A")
                
                display_pdf = filtered_pdf.head(1000)
                st.plotly_chart(plot_age_distribution(display_pdf), use_container_width=True)
                st.dataframe(display_pdf.head(100), use_container_width=True)
        else:
            st.info("Silver layer data is currently empty.")
            
    with tabs[2]:
        df = load_layer_data(spark, GOLD_PATH)
        if df:
            pdf = df.toPandas()
            with st.container(border=True):
                # Summary Statistics for Gold
                g1, g2, g3 = st.columns(3)
                g1.metric("Average Gold Age", f"{pdf['average_age'].mean():.1f} yrs" if not pdf.empty else "N/A")
                g2.metric("Total Managed Users", f"{pdf['total_users'].sum():,}" if not pdf.empty else "N/A")
                g3.metric("Aggregated Dates", f"{len(pdf)}" if not pdf.empty else "N/A")
                
                st.plotly_chart(plot_gold_trends(pdf), use_container_width=True)
                st.dataframe(pdf, use_container_width=True)
        else:
            st.info("Gold layer metrics are currently empty.")

    # 5. Pipeline Run History (Bottom of Main Dashboard)
    st.divider()
    st.subheader("📋 Recent Pipeline Runs")
    if history_records:
        history_table_data = []
        for r in history_records:
            t_str = datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
            status_text = "🟢 Completed" if r.get("status") == "completed" else "🔴 Failed"
            
            rows_val = r.get("rows", 0)
            rows_str = f"{rows_val:,}" if (rows_val is not None and isinstance(rows_val, (int, float))) else "0"
            
            dur_val = r.get("duration", "N/A")
            if isinstance(dur_val, (int, float)):
                dur_str = f"{dur_val:.2f}s"
            else:
                dur_str = str(dur_val)
                if dur_str.replace('.', '', 1).isdigit():
                    dur_str = f"{float(dur_str):.2f}s"
                else:
                    dur_str = "N/A"
                
            history_table_data.append({
                "Timestamp": t_str,
                "File Name": r.get("file_name") or "Unknown",
                "Status": status_text,
                "Duration": dur_str,
                "Rows Processed": rows_str
            })
        st.dataframe(pd.DataFrame(history_table_data), use_container_width=True)
    else:
        st.info("No runs logged yet.")

elif page == "Delta Lake Transaction Log":
    from delta.tables import DeltaTable
    
    st.title("🛡️ Delta Lake Transaction Log")
    st.write("Dynamic execution history retrieved directly from Delta Lake metadata logs.")
    
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
            st.info("No processing history found in Delta catalog.")
            
    except Exception as e:
        st.error(f"Error loading Delta history: {str(e)}")

elif page == "Data Quality & Observability":
    from delta.tables import DeltaTable
    from pyspark.sql.functions import col
    
    st.title("🛡️ Data Quality & Observability Portal")
    st.write("Comprehensive metrics, lineages, and audit trails generated from your Lakehouse layers.")
    
    # Calculate counts
    bronze_cnt = spark.read.format("delta").load(BRONZE_PATH).count() if os.path.exists(BRONZE_PATH) else 0
    silver_cnt = spark.read.format("delta").load(SILVER_PATH).count() if os.path.exists(SILVER_PATH) else 0
    gold_cnt = spark.read.format("delta").load(GOLD_PATH).count() if os.path.exists(GOLD_PATH) else 0
    
    # Fetch latest validation metrics from dq_metrics path
    latest_dq = None
    try:
        if os.path.exists(DQ_METRICS_PATH):
            dq_df = spark.read.format("delta").load(DQ_METRICS_PATH).orderBy(col("validation_time").desc()).limit(1).toPandas()
            if not dq_df.empty:
                latest_dq = dq_df.iloc[0]
    except Exception as e:
        print(f"Error reading DQ metrics: {e}")
        
    # Calculate passed and failed values based on latest dq run
    if latest_dq is not None:
        total_validated = int(latest_dq["total_rows"])
        null_ids = int(latest_dq["null_ids"])
        invalid_ages = int(latest_dq["invalid_ages"])
        duplicate_ids = int(latest_dq["duplicate_ids"])
        
        failed_records = null_ids + invalid_ages + duplicate_ids
        passed_records = max(0, total_validated - failed_records)
        
        null_rate = (null_ids / total_validated * 100) if total_validated > 0 else 0.0
        dup_rate = (duplicate_ids / total_validated * 100) if total_validated > 0 else 0.0
        inv_age_rate = (invalid_ages / total_validated * 100) if total_validated > 0 else 0.0
        
        dq_score = (passed_records / total_validated * 100) if total_validated > 0 else 100.0
    else:
        total_validated = bronze_cnt
        passed_records = silver_cnt
        failed_records = max(0, bronze_cnt - silver_cnt)
        null_ids = 0
        invalid_ages = 0
        duplicate_ids = 0
        null_rate = 0.0
        dup_rate = 0.0
        inv_age_rate = 0.0
        dq_score = (passed_records / total_validated * 100) if total_validated > 0 else 100.0

    # ----------------- SECTION 1: SYSTEM HEALTH & INCIDENT REPORTING -----------------
    st.divider()
    col_sys_health, col_incident_panel = st.columns([2, 3])
    
    with col_sys_health:
        with st.container(border=True):
            st.subheader("🏥 Operational Monitoring")
            
            # Connection statuses
            airflow_status = "🔴 Disconnected"
            if st.session_state.use_airflow_api:
                from airflow_client import test_airflow_connection
                success, _ = test_airflow_connection(
                    st.session_state.airflow_api_url,
                    st.session_state.airflow_username,
                    st.session_state.airflow_password
                )
                if success:
                    airflow_status = "🟢 Connected"
                    
            spark_status = "🟢 Available" if spark is not None else "🔴 Unavailable"
            
            # Check Data Lake directories readability
            dl_status = "🟢 Accessible"
            for p in [BRONZE_PATH, SILVER_PATH, GOLD_PATH]:
                if os.path.exists(p):
                    if not os.access(p, os.R_OK):
                        dl_status = "🔴 Inaccessible"
                        break
                else:
                    parent = os.path.dirname(p)
                    if os.path.exists(parent) and not os.access(parent, os.W_OK):
                        dl_status = "🔴 Inaccessible"
                        break
            
            dur_val = f"{duration:.2f}s" if isinstance(duration, (int, float)) else (f"{float(duration):.2f}s" if (duration != "N/A" and duration is not None and str(duration).replace('.', '', 1).isdigit()) else "N/A")
            
            st.markdown(f"**Airflow Scheduler:** {airflow_status}")
            st.markdown(f"**Spark SQL Engine:** {spark_status}")
            st.markdown(f"**Data Lake Catalog:** {dl_status}")
            st.markdown(f"**Last Run Duration:** `{dur_val}`")
            st.markdown(f"**Current Pipeline State:** `{status_indicator}`")

    with col_incident_panel:
        with st.container(border=True):
            st.subheader("🚨 DQ Incident Reporting")
            
            if status == "Pipeline failed":
                st.error("❌ Critical Data Quality Failure")
                st.markdown(f"**Reason:** {error_msg or 'Validation threshold of 50.0% null IDs exceeded.'}")
                st.markdown("**Threshold Limit:** Null ID rate ≥ 50.0%")
                st.markdown("**Recommended Action:** The input file has been rejected. Inspect the source dataset file and ensure that the IDs are formatted correctly (i.e. integers or valid numeric representations). Check for malformed strings.")
            elif failed_records > 0:
                st.warning("⚠️ Data Quality Warning")
                st.markdown(f"**Reason:** Data contains {failed_records} anomalous records ({null_ids} Null/Malformed IDs, {invalid_ages} Invalid Ages, and {duplicate_ids} Duplicate IDs).")
                st.markdown("**Recommended Action:** The pipeline completed successfully by automatically cleansing and resolving these anomalies (Silver layer). However, verify upstream systems to improve source data completeness.")
            else:
                st.success("🟢 Data Quality Healthy")
                st.markdown("**Reason:** 100% of processed records successfully conformed to data quality standards.")
                st.markdown("**Audit Details:** 0 nulls, 0 invalid ages, and 0 duplicate IDs detected in the active ingestion batch.")

    # ----------------- SECTION 2: LINEAGE FLOW VIEW -----------------
    st.divider()
    st.subheader("🔗 Data Lineage View")
    with st.container(border=True):
        lin_file, lin_arr1, lin_bronze, lin_arr2, lin_val, lin_arr3, lin_silver, lin_arr4, lin_gold = st.columns([2, 1, 2, 1, 2, 1, 2, 1, 2])
        
        with lin_file:
            st.metric("1. Input File", last_file[:15] if last_file else "None")
            st.caption("Landing Zone")
            
        with lin_arr1:
            st.markdown("<h3 style='text-align: center; margin-top: 15px;'>➔</h3>", unsafe_allow_html=True)
            
        with lin_bronze:
            st.metric("2. Bronze (Raw)", f"{bronze_cnt:,}")
            st.caption("Raw schemas (StringType)")
            
        with lin_arr2:
            st.markdown("<h3 style='text-align: center; margin-top: 15px;'>➔</h3>", unsafe_allow_html=True)
            
        with lin_val:
            val_status = "Passed ✅" if failed_records / max(1, total_validated) < 0.5 else "Failed ❌"
            st.metric("3. Validation", val_status)
            st.caption(f"{failed_records} violations flagged")
            
        with lin_arr3:
            st.markdown("<h3 style='text-align: center; margin-top: 15px;'>➔</h3>", unsafe_allow_html=True)
            
        with lin_silver:
            st.metric("4. Silver (Cleaned)", f"{silver_cnt:,}")
            st.caption(f"{failed_records} rows rejected")
            
        with lin_arr4:
            st.markdown("<h3 style='text-align: center; margin-top: 15px;'>➔</h3>", unsafe_allow_html=True)
            
        with lin_gold:
            st.metric("5. Gold (Curated)", f"{gold_cnt:,}")
            st.caption("Aggregation complete")

    # ----------------- SECTION 3: DATA QUALITY DASHBOARD (KPI CARDS) -----------------
    st.divider()
    st.subheader("🎯 Data Quality Scorecard")
    with st.container(border=True):
        kpi_score, kpi_null, kpi_dupe, kpi_age, kpi_pass, kpi_fail = st.columns(6)
        kpi_score.metric("Overall DQ Score", f"{dq_score:.2f}%")
        kpi_null.metric("Null ID Rate", f"{null_rate:.2f}%")
        kpi_dupe.metric("Duplicate ID Rate", f"{dup_rate:.2f}%")
        kpi_age.metric("Invalid Age Rate", f"{inv_age_rate:.2f}%")
        kpi_pass.metric("Records Passed", f"{passed_records:,}")
        kpi_fail.metric("Records Failed", f"{failed_records:,}")

    # ----------------- SECTION 4: DQ AUDIT PANEL -----------------
    st.divider()
    st.subheader("🔍 Data Quality Audit Panel")
    
    audit_details = get_dq_audit_details(spark)
    col_audit_ids, col_audit_ages, col_audit_dupes = st.columns(3)
    
    with col_audit_ids:
        with st.container(border=True):
            st.markdown(f"##### 🔤 Malformed IDs ({audit_details['malformed_ids']['count']})")
            if audit_details['malformed_ids']['samples']:
                st.dataframe(pd.DataFrame(audit_details['malformed_ids']['samples']), hide_index=True, use_container_width=True)
            else:
                st.success("No malformed IDs found in active raw data.")
                
    with col_audit_ages:
        with st.container(border=True):
            st.markdown(f"##### 🔢 Malformed Ages ({audit_details['malformed_ages']['count']})")
            if audit_details['malformed_ages']['samples']:
                st.dataframe(pd.DataFrame(audit_details['malformed_ages']['samples']), hide_index=True, use_container_width=True)
            else:
                st.success("No malformed ages found in active raw data.")
                
    with col_audit_dupes:
        with st.container(border=True):
            st.markdown(f"##### 👥 Duplicate Keys ({audit_details['duplicate_ids']['count']})")
            if audit_details['duplicate_ids']['samples']:
                st.dataframe(pd.DataFrame(audit_details['duplicate_ids']['samples']), hide_index=True, use_container_width=True)
            else:
                st.success("No duplicate keys found in active raw data.")

    # ----------------- SECTION 5: DQ TREND ANALYSIS & LAYER STATISTICS -----------------
    st.divider()
    col_trend, col_lineage_funnel = st.columns([3, 2])
    
    with col_trend:
        with st.container(border=True):
            dq_trend_df = get_dq_trends(spark)
            fig_trends = plot_dq_trends(dq_trend_df)
            if fig_trends:
                st.plotly_chart(fig_trends, use_container_width=True)
            else:
                st.info("Insufficient DQ history to compile trends chart.")
                
    with col_lineage_funnel:
        with st.container(border=True):
            st.plotly_chart(plot_data_funnel(bronze_cnt, silver_cnt, gold_cnt), use_container_width=True)

    # ----------------- SECTION 6: PIPELINE RUN REPORT -----------------
    st.divider()
    st.subheader("📋 Pipeline Run Report")
    
    if history_records:
        history_table_data = []
        for r in history_records:
            t_str = datetime.fromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
            status_text = "🟢 Completed" if r.get("status") == "completed" else "🔴 Failed"
            
            rows_val = r.get("rows", 0)
            rows_str = f"{rows_val:,}" if (rows_val is not None and isinstance(rows_val, (int, float))) else "0"
            
            dur_val = r.get("duration", "N/A")
            if isinstance(dur_val, (int, float)):
                dur_str = f"{dur_val:.2f}s"
            else:
                dur_str = str(dur_val)
                if dur_str.replace('.', '', 1).isdigit():
                    dur_str = f"{float(dur_str):.2f}s"
                else:
                    dur_str = "N/A"
                    
            history_table_data.append({
                "Run ID": r.get("run_id") or "N/A",
                "Timestamp": t_str,
                "File Name": r.get("file_name") or "Unknown",
                "Status": status_text,
                "Duration": dur_str,
                "Cleaned Rows Processed": rows_str,
                "Error Details": r.get("error") or "None"
            })
        st.dataframe(pd.DataFrame(history_table_data), use_container_width=True, hide_index=True)
    else:
        st.info("No runs logged yet.")

# ----------------- AUTO-REFRESH TRIGGER -----------------
if status == "Pipeline running":
    time.sleep(10)
    st.rerun()

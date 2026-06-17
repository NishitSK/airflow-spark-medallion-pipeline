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
    get_incidents,
    get_gold_report_data,
    generate_txt_report,
    generate_pdf_report,
    get_quarantine_data,
    get_profile_data,
    get_dq_run_report,
    get_schema_mapping_log,
    get_latest_successful_run_id,
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

def log_operation(event_type, message):
    from pipeline.config import BASE_DATA_PATH
    import os
    from datetime import datetime
    log_dir = os.path.join(BASE_DATA_PATH, "output")
    log_file = os.path.join(log_dir, "operations.log")
    
    # Diagnostics check before writing
    try:
        from pathlib import Path
        uid = None
        gid = None
        try:
            uid = os.getuid()
            gid = os.getgid()
        except AttributeError:
            pass
        path = Path(log_file)
        parent_dir = path.parent
        path_exists = path.exists()
        target_to_check = str(path) if path_exists else str(parent_dir)
        is_writable = os.access(target_to_check, os.W_OK)
        print(f"[DIAGNOSTICS] Writing to: {log_file} | Parent exists: {parent_dir.exists()} | File exists: {path_exists} | UID: {uid} | GID: {gid} | Writable: {is_writable}")
    except Exception as diag_err:
        print(f"Diagnostics logging failed: {str(diag_err)}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{event_type.upper()}] {message}\n"
    try:
        from pathlib import Path
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        tmp_file = f"{log_file}.tmp"
        existing_content = ""
        if os.path.exists(log_file):
            try:
                with open(log_file, "r") as f:
                    existing_content = f.read()
            except:
                pass
        with open(tmp_file, "w") as f:
            f.write(existing_content + log_line)
        os.replace(tmp_file, log_file)
    except Exception as e:
        print(f"Failed to write operations log: {str(e)}")

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

# Helper to map connection diagnostics results to friendly system errors
def get_friendly_connection_error(results):
    if not results.get("reachable", False):
        return "System Connection Error: The platform was unable to connect to the background pipeline scheduler. Please ensure that the services are online."
    if not results.get("authenticated", False):
        return "System Configuration Error: The platform was unable to authenticate with the background pipeline scheduler. Please verify that the system environment credentials are configured correctly."
    if not results.get("dag_found", False):
        return "System Initialization Error: The required data engineering pipeline DAG was not found on the scheduler. Please ensure the pipeline is deployed."
    return results.get("error_message") or "An unexpected system error occurred while connecting to the pipeline scheduler."

# Startup connection validation state
if "connection_verified" not in st.session_state:
    st.session_state.connection_verified = None
if "connection_error" not in st.session_state:
    st.session_state.connection_error = None

if st.session_state.connection_verified is None:
    try:
        import airflow_client
        health = airflow_client.get_airflow_health()
        if health == "UNAVAILABLE":
            st.session_state.connection_verified = False
            st.session_state.connection_error = "Pipeline service unavailable."
        else:
            st.session_state.connection_verified = True
            st.session_state.connection_error = None
    except Exception as e:
        st.session_state.connection_verified = False
        st.session_state.connection_error = "Pipeline service unavailable."

# Sidebar Design
with st.sidebar:
    st.title("🛡️ Pipeline Control")
    st.caption("Lakehouse Scheduler")
    st.divider()
    
    page = st.radio(
        "Navigation", 
        ["Pipeline Dashboard", "Delta Lake Transaction Log", "Data Quality & Observability"],
        index=0
    )
    
    st.caption("Production Build: v3.0")
    st.markdown("<div style='text-align: center; margin-top: 50px; opacity: 0.6;'><span style='font-size: 10px; color: #888888; letter-spacing: 1px;'>🔒 MEDALLION SECURE | made by nishit</span></div>", unsafe_allow_html=True)

# Retrieve status from consolidated pipeline states
status, last_success, last_file, error_msg, status_src, stage, duration = get_consolidated_status()

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

# Retrieve the latest run state to determine the indicators
run_status_text = "N/A"
try:
    import airflow_client
    details_ind, err_ind = airflow_client.get_latest_run()
    if not err_ind and details_ind:
        run_status_text = details_ind.get("state", "N/A").upper()
except:
    pass

# Convert status to display state
status_indicator = "🟢 Pipeline Ready"
if run_status_text == "RUNNING":
    status_indicator = "🟡 Pipeline Running"
elif run_status_text == "QUEUED":
    status_indicator = "🔵 Pipeline Queued"
elif run_status_text == "FAILED":
    status_indicator = "🔴 Pipeline Failed"



# ----------------- ERROR HANDLING PANEL -----------------
def get_user_friendly_error(err):
    if not err:
        return "An unknown error occurred.", "Verify that the pipeline is deployed and folders are accessible."
        
    err_str = str(err)
    err_lower = err_str.lower()
    
    # Parse structured error details if present (e.g. from Airflow logs)
    # Format: "Exception: {exc_type}: {exc_msg} | Stage: {failed_stage}"
    exc_details = ""
    stage_details = ""
    clean_err = err_str
    
    if " | Stage: " in err_str:
        parts = err_str.split(" | Stage: ")
        stage_details = f"\n\n**Failed Stage:** `{parts[1]}`"
        clean_err = parts[0]
        
    if clean_err.startswith("Exception: "):
        exc_details = f"\n\n**Exception Details:**\n`{clean_err[11:]}`"
        clean_err = clean_err[11:]
        
    # Determine friendly explanation and suggested fix
    if "permission denied" in err_lower or "permissionerror" in err_lower:
        explanation = "A storage path or file write operation failed due to directory permission restrictions."
        suggestion = "Verify write permissions on the data directories under `/data/` and ensure the container user (UID 50000) has write access to the mounted volumes."
    elif "unsupported dataset schema" in err_lower or "schema_validation" in err_lower or "missing required column" in err_lower:
        explanation = "Unsupported dataset schema."
        suggestion = "The uploaded file does not match the expected schema. Please ensure all required columns are present and mapped correctly."
    elif "not found" in err_lower or "does not exist" in err_lower or "no such file" in err_lower or "missing" in err_lower:
        explanation = "A required storage path or Delta table directory is unavailable."
        suggestion = "Verify that your local storage directories under data/ are mounted and writable. Ensure a dataset has been ingested first."
    elif "halted due to critical data quality" in err_lower or "critical dq failure" in err_lower or "duplicate" in err_lower or "data quality" in err_lower:
        explanation = "Halted due to critical Data Quality violations in raw data."
        suggestion = "Verify that your input file does not contain duplicate IDs, null keys, or negative ages. Run the 'Medium Sample' to test clean execution."
    elif "connection" in err_lower or "refused" in err_lower or "unreachable" in err_lower:
        explanation = "Unable to connect to the scheduling system or Spark cluster."
        suggestion = "Check that the scheduler services and local cluster configurations are running."
    elif "modulenotfound" in err_lower or "no module named" in err_lower:
        explanation = "A Python module import dependency is missing in the execution environment."
        suggestion = "Ensure the module is installed in the container or check the PYTHONPATH configuration in docker-compose."
    else:
        explanation = "An unexpected PySpark cluster or Delta Lake table write error occurred."
        suggestion = "Check that the input format matches the expected columns (id, name, age) and that Spark cluster memory is healthy."

    reason = f"{explanation}{exc_details}{stage_details}"
    return reason, suggestion

# Failure UI logic moved inside page scope.

# ----------------- RENDER NAVIGATION PAGES -----------------
if page == "Pipeline Dashboard":
    st.title("📊 Self-Service Medallion Data Platform")
    st.divider()
    
    from queries import get_latest_dataset_type
    dataset_type = get_latest_dataset_type()
    if dataset_type == "CUSTOMER":
        st.info("🔹 **Active Pipeline Mode:** `CUSTOMER` (Strict Schema Mode)")
    elif dataset_type == "ORDERS":
        st.info("🔸 **Active Pipeline Mode:** `ORDERS` (Strict Schema Mode)")
    else:
        st.info("⚙️ **Active Pipeline Mode:** `GENERIC` (Flexible Generic Mode)")
        
    # Display friendly system connection error banner at the top if validation failed
    if st.session_state.connection_verified is False:
        st.error(f"⚠️ {st.session_state.connection_error or 'Pipeline scheduler is currently offline.'}")
    
    def clear_input_folder():
        if os.path.exists(INPUT_PATH):
            for f in os.listdir(INPUT_PATH):
                fp = os.path.join(INPUT_PATH, f)
                if os.path.isfile(fp) and f.endswith(('.csv', '.json')):
                    try:
                        os.remove(fp)
                    except Exception as e:
                        print(f"Error removing staged file {fp}: {e}")
    
    # Ingestion Tabs in Main Body
    st.subheader("📥 Data Ingestion Control")
    tab_upload, tab_sample = st.tabs(["Upload Dataset", "Use Sample Dataset"])
    
    with tab_upload:
        uploaded_file = st.file_uploader("Drop CSV or JSON format file here", type=['csv', 'json'])
        if uploaded_file is not None:
            # Determine writable application temp directory
            import tempfile
            temp_dir = "/tmp/uploads"
            if os.name == "nt":
                temp_dir = os.path.join(tempfile.gettempdir(), "uploads")
                
            # Ensure the directory exists
            try:
                os.makedirs(temp_dir, exist_ok=True)
            except Exception as e:
                # Fallback to python standard tempdir on error
                temp_dir = os.path.join(tempfile.gettempdir(), "uploads")
                try:
                    os.makedirs(temp_dir, exist_ok=True)
                except Exception as fallback_err:
                    st.error(f"⚠️ Failed to create temporary upload directory: {fallback_err}")
                    temp_dir = None
            
            # Validate write access before opening the file
            is_writable = False
            if temp_dir:
                is_writable = os.access(temp_dir, os.W_OK)
                
            if not is_writable:
                st.error("⚠️ Streamlit does not have write access to the temporary directory. Please check file permissions.")
            else:
                temp_path = os.path.join(temp_dir, uploaded_file.name)
                
                # Write file safely
                write_success = False
                try:
                    with open(temp_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    write_success = True
                except Exception as e:
                    st.error(f"⚠️ Failed to write uploaded file to temporary directory: {str(e)}")
                    
                if write_success:
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
                        # Validate landing zone write access
                        if not os.access(INPUT_PATH, os.W_OK):
                            st.error(f"⚠️ Streamlit does not have write access to the staging directory: `{INPUT_PATH}`")
                        else:
                            try:
                                # Clear old input files
                                clear_input_folder()
                                # Copy file from temp to final input folder (landing zone)
                                final_path = os.path.join(INPUT_PATH, uploaded_file.name)
                                from pathlib import Path
                                Path(final_path).parent.mkdir(parents=True, exist_ok=True)
                                
                                # Separate staging copy from temporary upload
                                shutil.copyfile(temp_path, final_path)
                                
                                # S3 raw upload (non-blocking)
                                try:
                                    from utils.s3_client import upload_file
                                    s3_key = f"raw/uploads/{uploaded_file.name}"
                                    upload_file(final_path, s3_key)
                                    st.caption(f"☁️ Archived to S3: `{s3_key}`")
                                except Exception as s3_err:
                                    print(f"[S3] Warning: raw upload failed (non-fatal): {s3_err}")

                                # Cleanup the temp file
                                try:
                                    os.remove(temp_path)
                                except:
                                    pass
                                    
                                st.session_state.last_uploaded_file = uploaded_file.name
                                st.success(f"File '{uploaded_file.name}' uploaded and staged successfully. Use the Pipeline Execution Panel below to run the pipeline.")
                                time.sleep(1.5)
                                st.rerun()
                            except Exception as stage_err:
                                st.error(f"⚠️ Failed to copy dataset to staging folder: {str(stage_err)}")
                
    with tab_sample:
        st.write("Load one of the project's pre-packaged datasets directly into the pipeline:")
        
        # Loader trigger helper
        def run_sample(label, file):
            src = os.path.join(SAMPLES_PATH, file)
            dest = os.path.join(INPUT_PATH, file)
            try:
                # Clear old input files
                clear_input_folder()
                
                from pathlib import Path
                Path(dest).parent.mkdir(parents=True, exist_ok=True)
                
                # Resilient deletion of existing file if any
                if os.path.exists(dest):
                    try:
                        os.remove(dest)
                    except Exception as remove_err:
                        print(f"Failed to remove existing file {dest}: {remove_err}")
                        
                # Copy without preserving metadata/permissions, with fallback
                try:
                    shutil.copyfile(src, dest)
                except Exception as copy_err:
                    print(f"shutil.copyfile failed, trying manual read/write: {copy_err}")
                    with open(src, "rb") as f_in:
                        content = f_in.read()
                    with open(dest, "wb") as f_out:
                        f_out.write(content)

                # S3 raw upload (non-blocking)
                try:
                    from utils.s3_client import upload_file
                    s3_key = f"raw/uploads/{file}"
                    upload_file(dest, s3_key)
                    st.caption(f"☁️ Archived to S3: `{s3_key}`")
                except Exception as s3_err:
                    print(f"[S3] Warning: raw upload failed (non-fatal): {s3_err}")
                        
                st.session_state.last_uploaded_file = file
                
                st.toast(f"{label} dataset staged in input directory.", icon="📥")
                time.sleep(1.5)
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
                if st.button("Load Small CSV", type="secondary", use_container_width=True):
                    run_sample("Small CSV", "small_sample.csv")
                if st.button("Load Small JSON", type="secondary", use_container_width=True):
                    run_sample("Small JSON", "small_sample.json")
                
        with col_s2:
            with st.container(border=True):
                st.markdown("##### 📊 Medium Sample (10K rows)")
                st.write("Clean records demonstrating Spark execution metrics and cleansing workflows.")
                st.write("**Est. Ingestion Time:** `~5 seconds`")
                st.write("**Processing Cost:** `$0.10 USD`")
                st.divider()
                if st.button("Load Medium CSV", type="secondary", use_container_width=True):
                    run_sample("Medium CSV", "medium_sample.csv")
                if st.button("Load Medium JSON", type="secondary", use_container_width=True):
                    run_sample("Medium JSON", "medium_sample.json")
                
        with col_s3:
            with st.container(border=True):
                st.markdown("##### ⚡ Large Sample (100K rows)")
                st.write("Large data block designed to evaluate PySpark Delta Lake scaling limits.")
                st.write("**Est. Ingestion Time:** `~15 seconds`")
                st.write("**Processing Cost:** `$1.00 USD`")
                st.divider()
                if st.button("Load Large CSV", type="secondary", use_container_width=True):
                    run_sample("Large CSV", "large_sample.csv")

    # ----------------- PIPELINE EXECUTION PANEL -----------------
    st.divider()
    st.subheader("🚀 Pipeline Execution Panel")
    with st.container(border=True):
        # Load local status details if available
        local_status = None
        local_stage = "Waiting"
        local_duration = "N/A"
        local_run_id = "N/A"
        if os.path.exists(STATUS_FILE):
            try:
                import json
                with open(STATUS_FILE, "r") as f:
                    state_data = json.load(f)
                    local_status = state_data.get("status")
                    local_stage = state_data.get("stage", "Waiting")
                    local_duration = state_data.get("duration") or "N/A"
                    local_run_id = state_data.get("run_id") or "N/A"
            except:
                pass

        # Retrieve latest execution details and runs status counts
        run_details = None
        queued_cnt = 0
        running_cnt = 0
        success_cnt = 0
        failed_cnt = 0
        run_status_text = "N/A"
        
        try:
            import airflow_client
            details, err = airflow_client.get_latest_run()
            if not err and details:
                run_details = details
                run_status_text = details.get("state", "N/A").upper()
                
            counts = airflow_client.get_run_counts()
            queued_cnt = counts.get("queued", 0)
            running_cnt = counts.get("running", 0)
            success_cnt = counts.get("success", 0)
            failed_cnt = counts.get("failed", 0)
        except Exception:
            # Fallback to local status
            if local_status:
                run_status_text = local_status.upper()
                if local_status == "running":
                    running_cnt = 1
                elif local_status == "failed":
                    failed_cnt = 1
                elif local_status == "completed":
                    success_cnt = 1
                    
        # Metrics layout
        col_cur, col_q, col_r, col_s, col_f = st.columns(5)
        with col_cur:
            status_emoji = "🟢"
            if run_status_text in ["RUNNING", "QUEUED"]:
                status_emoji = "🟡"
            elif run_status_text == "FAILED":
                status_emoji = "🔴"
            st.metric("Current Status", f"{status_emoji} {run_status_text}")
        with col_q:
            st.metric("Queued", queued_cnt)
        with col_r:
            st.metric("Running", running_cnt)
        with col_s:
            st.metric("Success", success_cnt)
        with col_f:
            st.metric("Failed", failed_cnt)
            
        st.divider()
        
        # Details & Trigger Columns
        col_details, col_action = st.columns([3, 1])
        with col_details:
            if run_details:
                st.markdown(f"**DAG Run ID:** `{run_details.get('run_id')}`")
                st.markdown(f"**Start Time:** `{run_details.get('start_time')}`")
                st.markdown(f"**End Time:** `{run_details.get('end_time')}`")
                st.markdown(f"**Duration:** `{run_details.get('duration')}`")
            elif local_status:
                st.markdown(f"**DAG Run ID:** `{local_run_id}`")
                st.markdown(f"**Active Stage:** `{local_stage}`")
                st.markdown(f"**Start Time:** `N/A`")
                st.markdown(f"**End Time:** `N/A`")
                st.markdown(f"**Duration:** `{local_duration}`")
            else:
                st.info("No run details available. Trigger a run to begin.")
                
        with col_action:
            st.write("") # Spacer
            st.write("")
            
            # Check if there is an input file staged (i.e. file exists in INPUT_PATH)
            has_input = False
            staged_filename = "None"
            if os.path.exists(INPUT_PATH):
                try:
                    files = [f for f in os.listdir(INPUT_PATH) if f.endswith(('.csv', '.json'))]
                    if files:
                        has_input = True
                        files.sort(key=lambda x: os.path.getmtime(os.path.join(INPUT_PATH, x)), reverse=True)
                        staged_filename = files[0]
                except:
                    pass
            
            conn_ok = st.session_state.connection_verified
            run_btn_disabled = not has_input or (run_status_text in ["RUNNING", "QUEUED"]) or not conn_ok
            
            if not has_input:
                st.caption("⚠️ Upload/load a file first")
            elif not conn_ok:
                st.caption(f"❌ {st.session_state.connection_error or 'Pipeline service unavailable.'}")
            else:
                st.caption(f"Staged: `{staged_filename[:20]}`")
                
            if run_status_text in ["RUNNING", "QUEUED"]:
                st.warning("⚠️ Pipeline already running.")
                
            trigger_pipeline = st.button(
                "🚀 Run Pipeline", 
                type="primary", 
                use_container_width=True,
                disabled=run_btn_disabled
            )
            
            if trigger_pipeline:
                # Prevent duplicate triggers check
                if run_status_text in ["RUNNING", "QUEUED"]:
                    st.error("❌ Pipeline is already running or queued.")
                    log_operation("duplicate_prevention", "Trigger blocked: Pipeline is already running/queued.")
                else:
                    import airflow_client
                    triggered, msg = airflow_client.trigger_pipeline()
                    if triggered:
                        log_operation("trigger", f"Pipeline trigger accepted for file: {staged_filename}")
                        st.success("Pipeline triggered successfully.")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        log_operation("trigger_failure", f"Unable to trigger pipeline: {msg}")
                        st.error(f"❌ Unable to trigger pipeline: {msg}")

    # ----------------- LIVE PROCESSING PROGRESS -----------------
    if status == "Pipeline running":
        st.divider()
        st.info("🟢 **Live Monitoring**\nRefreshing every 3 seconds")
        st.markdown("### 🔄 Pipeline Running...")
        
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
    if status == "Pipeline failed" and error_msg:
        reason, fix = get_user_friendly_error(error_msg)
        with st.container(border=True):
            st.error("❌ Pipeline Failed")
            st.markdown(f"**Reason:**\n{reason}")
            st.markdown(f"**Suggested Fix:**\n{fix}")

    # ----------------- PIPELINE STATUS METRICS -----------------
    st.divider()
    st.subheader("📊 Pipeline Status Metrics")
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

    # ----------------- LAST RUN SUMMARY CARD -----------------
    try:
        from queries import get_last_run_summary
        summary_data = get_last_run_summary(spark)
        st.subheader("📋 Last Run Summary")
        with st.container(border=True):
            col_s_status, col_s_start, col_s_end, col_s_dur, col_s_rows, col_s_dq = st.columns(6)
            with col_s_status:
                st.metric("Last Status", summary_data["status"])
            with col_s_start:
                st.metric("Start Time", summary_data["start_time"])
            with col_s_end:
                st.metric("End Time", summary_data["end_time"])
            with col_s_dur:
                st.metric("Duration", summary_data["duration"])
            with col_s_rows:
                st.metric("Rows Processed", summary_data["rows"])
            with col_s_dq:
                st.metric("DQ Score", summary_data["dq_score"])
    except Exception as e:
        print(f"Error rendering Last Run Summary: {e}")

    # System Health Panel & Dataset Information
    st.divider()
    col_system, col_info = st.columns(2)
    with col_system:
        with st.container(border=True):
            st.subheader("🏥 System Health Monitor")
            
            # Health check variables
            airflow_status = "🔴 Unavailable"
            import airflow_client
            health = airflow_client.get_airflow_health()
            if health == "AVAILABLE":
                airflow_status = "🟢 Available"
            
            spark_status = "🟢 Available" if spark is not None else "🔴 Unavailable"
            input_writable = "🟢 Accessible" if os.access(INPUT_PATH, os.W_OK) else "🔴 Inaccessible"
            
            output_dir = os.path.dirname(STATUS_FILE)
            output_writable = "🟢 Accessible" if os.access(output_dir, os.W_OK) else "🔴 Inaccessible"
            
            st.markdown(f"**Pipeline Scheduler:** {airflow_status}")
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

    # ----------------- PIPELINE OUTPUTS SECTION -----------------
    st.divider()
    st.subheader("Pipeline Outputs")
    
    # Check if latest run was successful and if report data is available
    latest_run_is_success = False
    latest_run_id = None
    if os.path.exists(HISTORY_FILE):
        try:
            import json
            with open(HISTORY_FILE, "r") as f:
                lines = [json.loads(line.strip()) for line in f if line.strip()]
                if lines:
                    latest_run = lines[-1]
                    latest_run_id = latest_run.get("run_id")
                    if latest_run.get("status") == "completed":
                        latest_run_is_success = True
        except Exception as e:
            print(f"Error checking history for output status: {e}")

    # If the current status is running or failed, do not expose the outputs
    if status in ["Pipeline running", "Pipeline failed"]:
        latest_run_is_success = False

    report_data = None
    if latest_run_is_success:
        try:
            report_data = get_gold_report_data(spark)
        except Exception as e:
            print(f"Error getting report data: {e}")

    if report_data is None:
        st.warning("⚠️ No successful pipeline execution outputs are currently available. Stage a dataset and run the pipeline to generate outputs.")
    else:
        run_id = report_data.get("run_id", "Unknown")
        
        # Load Silver Delta table for this run
        silver_df = load_layer_data(spark, SILVER_PATH)
        csv_bytes = b""
        if silver_df is not None:
            try:
                silver_pdf = silver_df.toPandas()
                csv_bytes = convert_df_to_csv(silver_pdf)
            except Exception as e:
                print(f"Error converting Silver dataset to CSV: {str(e)}")
                
        # Generate TXT and PDF reports
        txt_report = generate_txt_report(report_data)
        pdf_bytes = generate_pdf_report(report_data)
        
        # Get S3 Metadata and direct downloads
        from queries import get_s3_export_metadata
        from utils.s3_client import object_exists, download_object_bytes
        
        s3_meta = get_s3_export_metadata(run_id)
        
        # Check S3 presence and fetch bytes
        s3_active = object_exists(f"exports/{run_id}/cleaned_dataset.csv")
        s3_csv_bytes = None
        s3_report_bytes = None
        s3_rejected_bytes = None
        
        if s3_active:
            s3_csv_bytes = download_object_bytes(f"exports/{run_id}/cleaned_dataset.csv")
            s3_report_bytes = download_object_bytes(f"reports/{run_id}/gold_report.txt")
            s3_rejected_bytes = download_object_bytes(f"quarantine/{run_id}/rejected_records.csv")
        
        col_actions, col_report = st.columns([1, 2])
        
        with col_actions:
            st.markdown("##### ☁️ S3 Storage")
            if s3_active:
                st.success("S3 Integration Active")
                
                # 📥 Download Cleaned Dataset
                if s3_csv_bytes:
                    st.download_button(
                        label="📥 S3 Download: Cleaned Dataset",
                        data=s3_csv_bytes,
                        file_name=f"cleaned_dataset_{run_id}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key=f"s3_down_csv_{run_id}"
                    )
                elif csv_bytes:
                    st.download_button(
                        label="📥 Local Download: Cleaned Dataset",
                        data=csv_bytes,
                        file_name=f"cleaned_dataset_{run_id}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key=f"local_down_csv_{run_id}"
                    )
                else:
                    st.button("📥 Download Cleaned Dataset", disabled=True, use_container_width=True)
                
                # 📄 Download Report (TXT)
                if s3_report_bytes:
                    st.download_button(
                        label="📄 S3 Download: Report (TXT)",
                        data=s3_report_bytes,
                        file_name=f"gold_analytics_report_{run_id}.txt",
                        mime="text/plain",
                        use_container_width=True,
                        key=f"s3_down_txt_{run_id}"
                    )
                else:
                    st.download_button(
                        label="📄 Local Download: Report (TXT)",
                        data=txt_report,
                        file_name=f"gold_analytics_report_{run_id}.txt",
                        mime="text/plain",
                        use_container_width=True,
                        key=f"local_down_txt_{run_id}"
                    )
                
                # ⬇️ Download Rejected Records
                try:
                    q_df_output = get_quarantine_data(spark, run_id=run_id)
                    has_rejects = not q_df_output.empty
                except:
                    has_rejects = False
                    
                if s3_rejected_bytes:
                    st.download_button(
                        label="⬇️ S3 Download: Rejected Records",
                        data=s3_rejected_bytes,
                        file_name=f"rejected_records_{run_id}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key=f"s3_down_rej_{run_id}"
                    )
                elif has_rejects:
                    rejected_bytes = q_df_output.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label=f"⬇️ Local Download: Rejected Records ({len(q_df_output)})",
                        data=rejected_bytes,
                        file_name=f"rejected_records_{run_id}.csv",
                        mime="text/csv",
                        use_container_width=True,
                        key=f"local_down_rej_{run_id}"
                    )
                else:
                    st.success("✅ No rejected records")

                st.markdown("---")
                
                # Metadata panel
                if s3_meta.get("cleaned_dataset"):
                    st.caption(f"`exports/{run_id}/cleaned_dataset.csv` ({s3_meta['cleaned_dataset'].get('size_mb', 0):.2f} MB)")
                if s3_meta.get("rejected_records"):
                    st.caption(f"`quarantine/{run_id}/rejected_records.csv` ({s3_meta['rejected_records'].get('size_mb', 0):.2f} MB)")
                if s3_meta.get("gold_report"):
                    st.caption(f"`reports/{run_id}/gold_report.txt` ({s3_meta['gold_report'].get('size_mb', 0):.2f} MB)")
            else:
                st.warning("S3 Integration Offline")
                # Fallback to local
                if csv_bytes:
                    st.download_button(label="📥 Download Cleaned Dataset", data=csv_bytes, file_name=f"cleaned_dataset_{run_id}.csv", mime="text/csv", use_container_width=True, key=f"offline_down_csv_{run_id}")
                st.download_button(label="📄 Download Report (TXT)", data=txt_report, file_name=f"gold_analytics_report_{run_id}.txt", mime="text/plain", use_container_width=True, key=f"offline_down_txt_{run_id}")
                if pdf_bytes:
                    st.download_button(label="📄 Download Report (PDF)", data=pdf_bytes, file_name=f"gold_analytics_report_{run_id}.pdf", mime="application/pdf", use_container_width=True, key=f"offline_down_pdf_{run_id}")
                try:
                    q_df_output = get_quarantine_data(spark, run_id=run_id)
                    if not q_df_output.empty:
                        rejected_bytes = q_df_output.to_csv(index=False).encode("utf-8")
                        st.download_button(label=f"⬇️ Download Rejected Records ({len(q_df_output)})", data=rejected_bytes, file_name=f"rejected_records_{run_id}.csv", mime="text/csv", use_container_width=True, key=f"offline_down_rej_{run_id}")
                    else:
                        st.success("✅ No rejected records")
                except:
                    pass

        with col_report:
            # 📊 View Gold Analytics Report
            with st.expander("📊 View Gold Analytics Report", expanded=True):
                st.text(txt_report)

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
                from queries import col
                total_raw = df.count()
                st.metric("Total Ingested Records", f"{total_raw:,}")
                sorted_df = df.orderBy(col("ingestion_time").desc()).limit(100).toPandas()
                st.dataframe(sorted_df, use_container_width=True)
            else:
                st.info("Bronze layer data is currently empty.")
            
    with tabs[1]:
        df = load_layer_data(spark, SILVER_PATH)
        if df is not None:
            from queries import col, get_latest_dataset_type, get_latest_successful_run_id, get_dq_run_report
            
            # Read dataset type
            latest_run_id = get_latest_successful_run_id()
            dq_report = get_dq_run_report(spark, run_id=latest_run_id) if latest_run_id else None
            dataset_type = dq_report.get("dataset_type", "CUSTOMER") if dq_report else get_latest_dataset_type()
            
            sort_cols = [col("processed_date").desc()]
            cols_list = df.df.columns
            if "id" in cols_list:
                sort_cols.append(col("id"))
            elif "order_id" in cols_list:
                sort_cols.append(col("order_id"))
                
            df_sorted = df.orderBy(*sort_cols)
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
                
                if dataset_type == "CUSTOMER" and 'age' in full_pdf.columns:
                    with col_slider:
                        age_range = st.slider("Interactive Filter: Select Age Scope", 0, 120, (18, 65), key="silver_age_slider")
                    
                    filtered_pdf = full_pdf[(full_pdf['age'] >= age_range[0]) & (full_pdf['age'] <= age_range[1])]
                    
                    # Statistics Metrics
                    m_avg, m_min, m_max = st.columns(3)
                    m_avg.metric("Average User Age", f"{filtered_pdf['age'].mean():.1f} yrs" if len(filtered_pdf) > 0 and pd.notnull(filtered_pdf['age'].mean()) else "N/A")
                    m_min.metric("Minimum User Age", f"{int(filtered_pdf['age'].min())} yrs" if len(filtered_pdf) > 0 and pd.notnull(filtered_pdf['age'].min()) else "N/A")
                    m_max.metric("Maximum User Age", f"{int(filtered_pdf['age'].max())} yrs" if len(filtered_pdf) > 0 and pd.notnull(filtered_pdf['age'].max()) else "N/A")
                    
                    display_pdf = filtered_pdf.head(1000)
                    st.plotly_chart(plot_age_distribution(display_pdf), use_container_width=True)
                elif dataset_type == "ORDERS" and 'unit_price' in full_pdf.columns:
                    price_min = float(full_pdf['unit_price'].min()) if pd.notnull(full_pdf['unit_price'].min()) else 0.0
                    price_max = float(full_pdf['unit_price'].max()) if pd.notnull(full_pdf['unit_price'].max()) else 1000.0
                    with col_slider:
                        price_range = st.slider("Interactive Filter: Select Price Scope ($)", price_min, price_max, (price_min, price_max), key="silver_price_slider")
                    filtered_pdf = full_pdf[(full_pdf['unit_price'] >= price_range[0]) & (full_pdf['unit_price'] <= price_range[1])]
                    
                    # Statistics Metrics
                    m_rev, m_ord, m_avg = st.columns(3)
                    total_orders = filtered_pdf['order_id'].nunique() if 'order_id' in filtered_pdf.columns else len(filtered_pdf)
                    if 'quantity' in filtered_pdf.columns and 'unit_price' in filtered_pdf.columns:
                        total_revenue = (filtered_pdf['quantity'] * filtered_pdf['unit_price']).sum()
                        avg_val = total_revenue / total_orders if total_orders > 0 else 0.0
                    else:
                        total_revenue = 0.0
                        avg_val = 0.0
                        
                    m_rev.metric("Total Revenue", f"${total_revenue:,.2f}")
                    m_ord.metric("Unique Orders", f"{total_orders:,}")
                    m_avg.metric("Avg Order Value", f"${avg_val:,.2f}")
                    
                    display_pdf = filtered_pdf.head(1000)
                    st.plotly_chart(plot_product_volume(display_pdf), use_container_width=True)
                else:
                    filtered_pdf = full_pdf
                    display_pdf = filtered_pdf.head(1000)
                    st.caption("Showing preview of Generic Silver dataset. All columns are preserved, whitespaces trimmed, and data types inferred/casted dynamically.")
                
                st.dataframe(display_pdf.head(100), use_container_width=True)
        else:
            st.info("Silver layer data is currently empty.")
            
    with tabs[2]:
        df = load_layer_data(spark, GOLD_PATH)
        if df:
            pdf = df.toPandas()
            from queries import get_latest_dataset_type, get_latest_successful_run_id, get_dq_run_report
            latest_run_id = get_latest_successful_run_id()
            dq_report = get_dq_run_report(spark, run_id=latest_run_id) if latest_run_id else None
            dataset_type = dq_report.get("dataset_type", "CUSTOMER") if dq_report else get_latest_dataset_type()
            
            with st.container(border=True):
                if dataset_type == "CUSTOMER":
                    # Summary Statistics for Gold
                    g1, g2, g3 = st.columns(3)
                    if 'average_age' in pdf.columns:
                        g1.metric("Average Gold Age", f"{pdf['average_age'].mean():.1f} yrs" if not pdf.empty and pd.notnull(pdf['average_age'].mean()) else "N/A")
                    else:
                        g1.metric("Average Gold Age", "N/A")
                    
                    if 'total_users' in pdf.columns:
                        g2.metric("Total Managed Users", f"{pdf['total_users'].sum():,}" if not pdf.empty else "N/A")
                    else:
                        g2.metric("Total Managed Users", "N/A")
                    g3.metric("Aggregated Dates", f"{len(pdf)}" if not pdf.empty else "N/A")
                    
                    if 'average_age' in pdf.columns:
                        st.plotly_chart(plot_gold_trends(pdf), use_container_width=True)
                    st.dataframe(pdf, use_container_width=True)
                elif dataset_type == "ORDERS":
                    # Summary Statistics for Gold
                    g1, g2, g3 = st.columns(3)
                    if 'total_orders' in pdf.columns:
                        g1.metric("Total Managed Orders", f"{pdf['total_orders'].sum():,}" if not pdf.empty else "0")
                    else:
                        g1.metric("Total Managed Orders", "0")
                    
                    if 'total_revenue' in pdf.columns:
                        g2.metric("Total Managed Revenue", f"${pdf['total_revenue'].sum():,.2f}" if not pdf.empty else "$0.00")
                    else:
                        g2.metric("Total Managed Revenue", "$0.00")
                    g3.metric("Aggregated Dates", f"{len(pdf)}" if not pdf.empty else "0")
                    
                    st.plotly_chart(plot_orders_trends(pdf), use_container_width=True)
                    st.dataframe(pdf, use_container_width=True)
                else:
                    g1, g2, g3, g4 = st.columns(4)
                    if 'total_rows' in pdf.columns:
                        g1.metric("Total Ingested Rows", f"{int(pdf['total_rows'].iloc[0]):,}" if not pdf.empty else "0")
                    else:
                        g1.metric("Total Ingested Rows", "0")
                        
                    if 'total_columns' in pdf.columns:
                        g2.metric("Total Ingested Columns", f"{int(pdf['total_columns'].iloc[0])}" if not pdf.empty else "0")
                    else:
                        g2.metric("Total Ingested Columns", "0")
                        
                    if 'completeness_score' in pdf.columns:
                        g3.metric("Dataset Completeness", f"{pdf['completeness_score'].iloc[0]:.2f}%" if not pdf.empty else "N/A")
                    else:
                        g3.metric("Dataset Completeness", "N/A")
                        
                    if 'duplicate_rate' in pdf.columns:
                        g4.metric("Duplicate Rate", f"{pdf['duplicate_rate'].iloc[0]:.2f}%" if not pdf.empty else "N/A")
                    else:
                        g4.metric("Duplicate Rate", "N/A")
                        
                    if not pdf.empty and 'column_metrics' in pdf.columns:
                        col_metrics_str = pdf['column_metrics'].iloc[0]
                        try:
                            column_metrics_dict = json.loads(col_metrics_str) if isinstance(col_metrics_str, str) else col_metrics_str
                            if column_metrics_dict:
                                st.markdown("##### 📁 Column Inventory and Profiling")
                                inventory_data = []
                                for col_name, metrics in column_metrics_dict.items():
                                    inventory_data.append({
                                        "Column Name": col_name,
                                        "Datatype": metrics.get("datatype", "string"),
                                        "Null Count": metrics.get("null_count", 0),
                                        "Null Percentage": f"{metrics.get('null_percentage', 0.0):.2f}%",
                                        "Distinct Count": metrics.get("distinct_count", 0)
                                    })
                                st.dataframe(pd.DataFrame(inventory_data), use_container_width=True, hide_index=True)
                                
                                st.plotly_chart(plot_generic_nulls(column_metrics_dict), use_container_width=True)
                                if 'duplicate_rate' in pdf.columns:
                                    st.plotly_chart(plot_generic_duplicates(float(pdf['duplicate_rate'].iloc[0])), use_container_width=True)
                        except Exception as e:
                            st.caption(f"Could not parse column metrics: {e}")
                    
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

    # ========== ENTERPRISE DATA QUALITY SUMMARY ==========
    st.divider()
    st.subheader("🎯 Enterprise Data Quality Summary")

    latest_run_id = get_latest_successful_run_id()
    dq_report = get_dq_run_report(spark, run_id=latest_run_id) if latest_run_id else None

    if dq_report:
        total_r  = int(float(dq_report.get("total_rows", 0) or 0))
        valid_r  = int(float(dq_report.get("valid_rows", 0) or 0))
        invalid_r = int(float(dq_report.get("invalid_rows", 0) or 0))
        dq_sc   = float(dq_report.get("dq_score", 100.0) or 100.0)
        anomaly_str = str(dq_report.get("anomaly_flags", "None") or "None")
        mapping_str = str(dq_report.get("schema_mappings", "[]") or "[]")

        with st.container(border=True):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("🏆 DQ Score", f"{dq_sc:.1f}%")
            c2.metric("✅ Records Accepted", f"{valid_r:,}")
            c3.metric("🚫 Records Quarantined", f"{invalid_r:,}")
            c4.metric("📊 Total Received", f"{total_r:,}")

            col_anomaly, col_mapping = st.columns(2)
            with col_anomaly:
                with st.container(border=True):
                    st.markdown("**🔍 Anomalies Detected**")
                    if anomaly_str and anomaly_str != "None":
                        for line in anomaly_str.split(";"):
                            if line.strip():
                                st.warning(f"⚠️ {line.strip()}")
                    else:
                        st.success("✅ No anomalies detected in this run.")
            with col_mapping:
                with st.container(border=True):
                    st.markdown("**🔄 Schema Mappings Applied**")
                    try:
                        import json as _json
                        mappings = _json.loads(mapping_str) if mapping_str != "[]" else []
                        if mappings:
                            for m in mappings:
                                st.info(f"↪️ `{m.get('from_col', '?')}` → `{m.get('to_col', '?')}`")
                        else:
                            st.success("✅ Schema matched canonical format — no mapping required.")
                    except Exception:
                        st.caption(mapping_str)
    else:
        st.info("⚠️ No DQ report available yet. Run the pipeline to generate a quality report.")

    # ========== DATA PROFILE SECTION ==========
    st.divider()
    st.subheader("📐 Data Profile")
    profile_df = get_profile_data(spark, run_id=latest_run_id)
    if not profile_df.empty:
        display_cols = [c for c in ["column_name", "total_rows", "null_count", "null_pct",
                                      "distinct_count", "duplicate_pct", "min_value",
                                      "max_value", "mean_value", "stddev_value",
                                      "outlier_count", "top_values"] if c in profile_df.columns]
        with st.container(border=True):
            st.dataframe(profile_df[display_cols], use_container_width=True, hide_index=True)
    else:
        st.info("No data profile available yet. Run the pipeline to generate a column profile.")

    # ========== QUARANTINE VIEWER ==========
    st.divider()
    st.subheader("🚫 Quarantine Viewer — Rejected Records")
    quarantine_df = get_quarantine_data(spark, run_id=latest_run_id)
    if not quarantine_df.empty:
        q_count = len(quarantine_df)
        st.warning(f"**{q_count}** record(s) quarantined in the latest run. These records were rejected due to data quality violations and were NOT written to Silver.")

        # Download rejected records
        try:
            rejected_csv = quarantine_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download Rejected Records CSV",
                data=rejected_csv,
                file_name=f"rejected_records_{latest_run_id}.csv",
                mime="text/csv",
                use_container_width=True,
                key="dl_quarantine_btn"
            )
        except Exception:
            pass

        # Show quarantine table with key columns first
        priority_cols = [c for c in ["id", "name", "age", "quarantine_reason", "rule_violated",
                                       "run_id", "dq_source_file", "quarantine_time"]
                         if c in quarantine_df.columns]
        other_cols = [c for c in quarantine_df.columns if c not in priority_cols]
        with st.container(border=True):
            st.dataframe(quarantine_df[priority_cols + other_cols].head(200),
                         use_container_width=True, hide_index=True)
    else:
        st.success("✅ No records were quarantined in the latest successful run.")

elif page == "Delta Lake Transaction Log":
    st.title("🛡️ Delta Lake Transaction Log")
    st.write("Dynamic execution history retrieved directly from Delta Lake metadata logs.")
    
    try:
        from queries import get_delta_history_pandas
        history_df = get_delta_history_pandas(SILVER_PATH)
        
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
    except Exception as e:
        st.error(f"Failed to load Delta Lake transaction log: {e}")

elif page == "Data Quality & Observability":
    from queries import col, read_delta_pandas, get_latest_dataset_type, get_latest_successful_run_id, get_dq_run_report
    
    st.title("🛡️ Data Quality & Observability Portal")
    st.write("Comprehensive metrics, lineages, and audit trails generated from your Lakehouse layers.")
    
    # Calculate counts safely
    def get_count_safely(path):
        try:
            if not os.path.exists(path):
                return 0
            df = read_delta_pandas(path)
            return len(df)
        except:
            return 0
            
    bronze_cnt = get_count_safely(BRONZE_PATH)
    silver_cnt = get_count_safely(SILVER_PATH)
    gold_cnt = get_count_safely(GOLD_PATH)
    
    latest_run_id = get_latest_successful_run_id()
    latest_dq_report = get_dq_run_report(spark, run_id=latest_run_id) if latest_run_id else None
    
    if latest_dq_report:
        dataset_type = latest_dq_report.get("dataset_type", "GENERIC")
        total_validated = int(float(latest_dq_report.get("total_rows", 0) or 0))
        failed_records = int(float(latest_dq_report.get("invalid_rows", 0) or 0))
        passed_records = int(float(latest_dq_report.get("valid_rows", 0) or 0))
        dq_score = float(latest_dq_report.get("dq_score", 100.0) or 100.0)
        
        null_rate = 0.0
        dup_rate = 0.0
        inv_age_rate = 0.0
        
        if dataset_type == "CUSTOMER":
            null_ids = int(float(latest_dq_report.get("null_ids", 0) or 0))
            duplicate_ids = int(float(latest_dq_report.get("duplicate_ids", 0) or 0))
            invalid_ages = int(float(latest_dq_report.get("invalid_ages", 0) or 0))
            
            null_rate = (null_ids / total_validated * 100) if total_validated > 0 else 0.0
            dup_rate = (duplicate_ids / total_validated * 100) if total_validated > 0 else 0.0
            inv_age_rate = (invalid_ages / total_validated * 100) if total_validated > 0 else 0.0
        elif dataset_type == "ORDERS":
            null_order_ids = int(float(latest_dq_report.get("null_order_ids", 0) or 0))
            duplicate_ids = int(float(latest_dq_report.get("duplicate_ids", 0) or 0))
            
            null_rate = (null_order_ids / total_validated * 100) if total_validated > 0 else 0.0
            dup_rate = (duplicate_ids / total_validated * 100) if total_validated > 0 else 0.0
        else:
            null_rate = 100.0 - float(latest_dq_report.get("completeness_score", 100.0) or 100.0)
            dup_rate = float(latest_dq_report.get("duplicate_rate", 0.0) or 0.0)
    else:
        dataset_type = get_latest_dataset_type()
        total_validated = bronze_cnt
        passed_records = silver_cnt
        failed_records = max(0, bronze_cnt - silver_cnt)
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
            
            airflow_status = "🔴 Unavailable"
            import airflow_client
            health = airflow_client.get_airflow_health()
            if health == "AVAILABLE":
                airflow_status = "🟢 Available"
                    
            spark_status = "🟢 Available" if spark is not None else "🔴 Unavailable"
            
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
            
            st.markdown(f"**Pipeline Scheduler:** {airflow_status}")
            st.markdown(f"**Spark SQL Engine:** {spark_status}")
            st.markdown(f"**Data Lake Catalog:** {dl_status}")
            st.markdown(f"**Last Run Duration:** `{dur_val}`")
            st.markdown(f"**Current Pipeline State:** `{status_indicator}`")

    with col_incident_panel:
        with st.container(border=True):
            st.subheader("🚨 DQ Incident Reporting")
            
            if status == "Pipeline failed":
                st.error("❌ Critical Data Quality Failure")
                st.markdown(f"**Reason:** {error_msg or 'Validation threshold exceeded.'}")
                if dataset_type == "CUSTOMER":
                    st.markdown("**Threshold Limit:** Null ID rate ≥ 50.0% or Min Rows < 1")
                elif dataset_type == "ORDERS":
                    st.markdown("**Threshold Limit:** Conformance checks or Min Rows < 1")
                else:
                    st.markdown("**Threshold Limit:** Min Rows threshold failure.")
                st.markdown("**Recommended Action:** The input file has been rejected. Inspect the source dataset file and verify fields format.")
            elif failed_records > 0:
                st.warning("⚠️ Data Quality Warning")
                if dataset_type == "CUSTOMER":
                    st.markdown(f"**Reason:** Data contains {failed_records} anomalous records ({null_ids} Null/Malformed IDs, {invalid_ages} Invalid Ages, and {duplicate_ids} Duplicate IDs).")
                elif dataset_type == "ORDERS":
                    st.markdown(f"**Reason:** Data contains {failed_records} anomalous records.")
                else:
                    st.markdown(f"**Reason:** Data contains duplicate rows.")
                st.markdown("**Recommended Action:** The pipeline completed successfully by automatically cleansing and resolving these anomalies (Silver layer). However, verify upstream systems to improve source data completeness.")
            else:
                st.success("🟢 Data Quality Healthy")
                st.markdown("**Reason:** 100% of processed records conformed to data quality standards.")
                if dataset_type == "CUSTOMER":
                    st.markdown("**Audit Details:** 0 nulls, 0 invalid ages, and 0 duplicate IDs detected.")
                elif dataset_type == "ORDERS":
                    st.markdown("**Audit Details:** 0 nulls, 0 invalid quantities/prices, and 0 duplicate orders detected.")
                else:
                    st.markdown("**Audit Details:** 0 missing cells and 0 duplicate rows detected in the active batch.")

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
        if dataset_type == "CUSTOMER":
            kpi_score, kpi_null, kpi_dupe, kpi_age, kpi_pass, kpi_fail = st.columns(6)
            kpi_score.metric("Overall DQ Score", f"{dq_score:.2f}%")
            kpi_null.metric("Null ID Rate", f"{null_rate:.2f}%")
            kpi_dupe.metric("Duplicate ID Rate", f"{dup_rate:.2f}%")
            kpi_age.metric("Invalid Age Rate", f"{inv_age_rate:.2f}%")
            kpi_pass.metric("Records Passed", f"{passed_records:,}")
            kpi_fail.metric("Records Failed", f"{failed_records:,}")
        elif dataset_type == "ORDERS":
            kpi_score, kpi_null, kpi_dupe, kpi_qty, kpi_price, kpi_pass, kpi_fail = st.columns(7)
            kpi_score.metric("Overall DQ Score", f"{dq_score:.2f}%")
            
            tot_val = max(1, total_validated)
            qty_rate = (int(latest_dq_report.get("invalid_qty", 0)) / tot_val * 100.0) if latest_dq_report else 0.0
            price_rate = (int(latest_dq_report.get("invalid_price", 0)) / tot_val * 100.0) if latest_dq_report else 0.0
            
            kpi_null.metric("Null ID Rate", f"{null_rate:.2f}%")
            kpi_dupe.metric("Duplicate ID Rate", f"{dup_rate:.2f}%")
            kpi_qty.metric("Invalid Qty Rate", f"{qty_rate:.2f}%")
            kpi_price.metric("Invalid Price Rate", f"{price_rate:.2f}%")
            kpi_pass.metric("Records Passed", f"{passed_records:,}")
            kpi_fail.metric("Records Failed", f"{failed_records:,}")
        else:
            kpi_score, kpi_comp, kpi_dupe, kpi_cols, kpi_pass = st.columns(5)
            comp_score = float(latest_dq_report.get("completeness_score", 100.0)) if latest_dq_report else 100.0
            total_cols = int(latest_dq_report.get("total_columns", 0)) if latest_dq_report else 0
            
            kpi_score.metric("Overall DQ Score", f"{dq_score:.2f}%")
            kpi_comp.metric("Completeness Score", f"{comp_score:.2f}%")
            kpi_dupe.metric("Duplicate Row Rate", f"{dup_rate:.2f}%")
            kpi_cols.metric("Total Columns", f"{total_cols}")
            kpi_pass.metric("Records Processed", f"{passed_records:,}")

    # ----------------- SECTION 4: DQ AUDIT PANEL -----------------
    st.divider()
    st.subheader("🔍 Data Quality Audit Panel")
    
    audit_details = get_dq_audit_details(spark)
    
    if dataset_type == "CUSTOMER":
        col_audit_ids, col_audit_ages, col_audit_dupes = st.columns(3)
        with col_audit_ids:
            with st.container(border=True):
                st.markdown(f"##### 🔤 Malformed IDs ({audit_details.get('malformed_ids', {}).get('count', 0)})")
                if audit_details.get('malformed_ids', {}).get('samples'):
                    st.dataframe(pd.DataFrame(audit_details['malformed_ids']['samples']), hide_index=True, use_container_width=True)
                else:
                    st.success("No malformed IDs found in active raw data.")
                    
        with col_audit_ages:
            with st.container(border=True):
                st.markdown(f"##### 🔢 Malformed Ages ({audit_details.get('malformed_ages', {}).get('count', 0)})")
                if audit_details.get('malformed_ages', {}).get('samples'):
                    st.dataframe(pd.DataFrame(audit_details['malformed_ages']['samples']), hide_index=True, use_container_width=True)
                else:
                    st.success("No malformed ages found in active raw data.")
                    
        with col_audit_dupes:
            with st.container(border=True):
                st.markdown(f"##### 👥 Duplicate Keys ({audit_details.get('duplicate_ids', {}).get('count', 0)})")
                if audit_details.get('duplicate_ids', {}).get('samples'):
                    st.dataframe(pd.DataFrame(audit_details['duplicate_ids']['samples']), hide_index=True, use_container_width=True)
                else:
                    st.success("No duplicate keys found in active raw data.")
    elif dataset_type == "ORDERS":
        col_audit_ids, col_audit_qty, col_audit_price, col_audit_dupes = st.columns(4)
        with col_audit_ids:
            with st.container(border=True):
                st.markdown(f"##### 🔤 Missing Order IDs ({audit_details.get('malformed_order_ids', {}).get('count', 0)})")
                if audit_details.get('malformed_order_ids', {}).get('samples'):
                    st.dataframe(pd.DataFrame(audit_details['malformed_order_ids']['samples']), hide_index=True, use_container_width=True)
                else:
                    st.success("No missing order IDs.")
        with col_audit_qty:
            with st.container(border=True):
                st.markdown(f"##### 🔢 Invalid Quantities ({audit_details.get('invalid_qty', {}).get('count', 0)})")
                if audit_details.get('invalid_qty', {}).get('samples'):
                    st.dataframe(pd.DataFrame(audit_details['invalid_qty']['samples']), hide_index=True, use_container_width=True)
                else:
                    st.success("No invalid quantities.")
        with col_audit_price:
            with st.container(border=True):
                st.markdown(f"##### 💲 Invalid Prices ({audit_details.get('invalid_price', {}).get('count', 0)})")
                if audit_details.get('invalid_price', {}).get('samples'):
                    st.dataframe(pd.DataFrame(audit_details['invalid_price']['samples']), hide_index=True, use_container_width=True)
                else:
                    st.success("No invalid prices.")
        with col_audit_dupes:
            with st.container(border=True):
                st.markdown(f"##### 👥 Duplicate Orders ({audit_details.get('duplicate_orders', {}).get('count', 0)})")
                if audit_details.get('duplicate_orders', {}).get('samples'):
                    st.dataframe(pd.DataFrame(audit_details['duplicate_orders']['samples']), hide_index=True, use_container_width=True)
                else:
                    st.success("No duplicate order IDs.")
    else:
        with st.container(border=True):
            st.markdown(f"##### 📊 Null Analysis Per Column ({audit_details.get('column_nulls', {}).get('count', 0)} Columns with Nulls)")
            if audit_details.get('column_nulls', {}).get('samples'):
                st.dataframe(pd.DataFrame(audit_details['column_nulls']['samples']), hide_index=True, use_container_width=True)
            else:
                st.success("No null values found in any columns!")

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
    time.sleep(3)
    st.rerun()

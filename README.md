# Medallion Data Engineering Pipeline

An end-to-end, Dockerized Data Engineering pipeline demonstrating a unified Medallion architecture using **Apache Spark 4.0** for data processing, **Apache Airflow 3.x** for scheduling and orchestration, and **Streamlit** for real-time dashboard analytics and ingestion control.

---

## Architecture Diagram

```mermaid
graph TD
    User([User]) -->|Upload CSV/JSON or Click Load Sample| Streamlit[Streamlit Dashboard]
    Streamlit -->|Write File| InputLake[(/data/input/)]
    
    Airflow[Airflow Scheduler] -->|FileSensor scan every 60s| InputLake
    InputLake -->|Detect File| Trigger[file_trigger_pipeline DAG]
    Trigger -->|Execute BashOperator| UnifiedJob[unified_pipeline.py]
    
    subgraph Spark Engine [PySpark 4.0 & Delta Lake 4.1]
        UnifiedJob -->|1. Ingest Raw| Bronze[(Bronze Lakehouse)]
        UnifiedJob -->|2. Data Quality Checks| Validate[validate_data.py]
        Validate -->|Fail/Stop if Null ID > 50%| ErrorInc[(Incidents Log)]
        Validate -->|Pass/Continue| Silver[(Silver Lakehouse)]
        UnifiedJob -->|3. Clean & Deduplicate| Silver
        UnifiedJob -->|4. Aggregate Analytics| Gold[(Gold Lakehouse)]
    end
    
    UnifiedJob -->|Log Run Status & Metrics| Trace[(pipeline_status.json)]
    UnifiedJob -->|Archive Original| Archive[(/data/archive/)]
    
    Streamlit -->|Poll DAG status| AirflowAPI[Airflow 3.x REST API]
    Streamlit -.->|Fallback status sync| Trace
    
    Streamlit -->|Visualize Insights| Gold
    Streamlit -->|Interactive Filters & KPIs| Silver
```

---

## Enhanced Streamlit Dashboard Features

The Streamlit dashboard has been heavily optimized for production UX. It includes:

1. **Auto-Refresh Trigger**: Automatically refreshes the UI every 5 seconds *only* when the pipeline is actively processing to prevent manual page reloads (F5).
2. **Visual Progress Timeline**: Highlights the active execution stage (`Upload → Bronze Ingestion → Silver Transformation → Gold Aggregation`) in real time with dynamic CSS indicators.
3. **Quick Start Sample Loader**: Includes three built-in sample datasets (Small, Medium, and Large) that automatically copy to the input directory and trigger the Airflow DAG via API.
4. **Dataset Information Panel**: Shows the active file name, row counts, size on disk, and directory placement dynamically.
5. **System Health Panel**: Monitors connection states for Airflow (via REST API), Spark (local runtime), and Data Lake availability.
6. **Interactive Visualizations**: Includes interactive age range filters and summary metrics on PySpark-processed Silver and Gold datasets.
7. **Success Notifications**: Alerts users with browser-native notification toasts (`st.toast`) once background pipeline runs successfully complete.

---

## Quick Start & Sample Datasets

The dashboard includes multiple built-in sample datasets stored inside `/data/samples/`. You can trigger them directly from the dashboard:

* **Small Dataset (`small_sample.csv`)**: 100 rows. Fast run time. Contains intentionally dirty data (nulls, duplicates, invalid ages) to demonstrate Data Quality (DQ) validation.
* **Medium Dataset (`medium_sample.csv`)**: 10,000 rows. Ideal for analyzing PySpark execution speeds and transformation rules.
* **Large Dataset (`large_sample.csv`)**: 100,000 rows. Designed to test Spark's partitioning and scalability limits.

---

## Deployment & Setup

### Prerequisites
* Docker and Docker Compose installed.

### Execution
1. Clone the repository and navigate to the project directory:
   ```bash
   cd airflow-spark-medallion-pipeline
   ```
2. Launch the services:
   ```bash
   docker-compose -f docker/docker-compose.yml up --build -d
   ```
3. Access the dashboard:
   * **Streamlit Dashboard**: `http://localhost:8501`
   * **Airflow UI**: `http://localhost:8081` (Default: `admin` / `admin`)
   * **Spark Master UI**: `http://localhost:8080`

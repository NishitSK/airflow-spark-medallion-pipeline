from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta
import pendulum
import os

# ---------------------------
# Failure Callback
# ---------------------------
def on_dag_failure(context):
    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "DAG"
    exception = context.get("exception", "Unknown Error")

    message = f"FAILURE in task '{task_id}': {str(exception)[:200]}..."

    log_cmd = (
        f"python3 /opt/airflow/monitoring/log_incident.py "
        f"'{dag_id}' '{run_id}' '{task_id}' '{message}'"
    )

    os.system(log_cmd)


# ---------------------------
# Default DAG arguments
# ---------------------------
default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_dag_failure,
}


# ---------------------------
# DAG Definition
# ---------------------------
with DAG(
    dag_id="file_trigger_pipeline",
    default_args=default_args,
    description="Modular Medallion Pipeline",
    schedule=None,   # run only when explicitly triggered
    catchup=False,
    max_active_runs=1,
) as dag:

    # Run Spark unified medallion pipeline
    unified_pipeline = BashOperator(
        task_id="unified_medallion_pipeline",
        bash_command="python /opt/airflow/spark_jobs/unified_pipeline.py --airflow-start-time '{{ dag_run.start_date.timestamp() if dag_run else 0 }}'",
    )

    # Archive processed files on success
    archive_file = BashOperator(
        task_id="archive_file",
        bash_command="python /opt/airflow/monitoring/archive_file.py",
    )

    # Task order
    unified_pipeline >> archive_file


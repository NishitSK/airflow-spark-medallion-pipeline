from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta
import pendulum

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="spark_daily_5am_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args
) as dag:

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command="python /opt/airflow/spark_jobs/process_data.py"
    )

    archive_file = BashOperator(
        task_id="archive_file",
        bash_command="""
        mkdir -p /data/archive
        mv /data/input/*.csv /data/archive/ 2>/dev/null || true
        """
    )

    run_spark >> archive_file

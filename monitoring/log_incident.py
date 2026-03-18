from datetime import datetime
from pipeline.config import TRACE_PATH
from pipeline.delta_utils import get_spark_session, write_delta
from pyspark.sql.functions import lit

def log_incident(dag_id, run_id, task_id, message, status="ERROR"):
    """
    Utility to log incidents or trace steps to a Delta table.
    """
    spark = get_spark_session("IncidentLogger")
    
    try:
        incident_data = [(
            dag_id,
            run_id,
            task_id,
            message,
            status,
            datetime.now()
        )]
        
        columns = ["dag_id", "run_id", "task_id", "message", "status", "timestamp"]
        df = spark.createDataFrame(incident_data, columns)
        
        write_delta(df, TRACE_PATH)
        print(f"Incident logged for task {task_id}")
    finally:
        spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 5:
        log_incident(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

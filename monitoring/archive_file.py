import os
import sys
import shutil
from datetime import datetime

# Add /opt/airflow to PYTHONPATH for container execution environment
sys.path.append("/opt/airflow")

try:
    from pipeline.config import INPUT_PATH, ARCHIVE_PATH, BASE_DATA_PATH
except ImportError:
    # Failback default path configurations in docker containers
    INPUT_PATH = "/data/input"
    ARCHIVE_PATH = "/data/archive"
    BASE_DATA_PATH = "/data"

def log_operation(event_type, message):
    log_dir = os.path.join(BASE_DATA_PATH, "output")
    log_file = os.path.join(log_dir, "operations.log")
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

def archive_files():
    if not os.path.exists(INPUT_PATH):
        print(f"Input path {INPUT_PATH} does not exist.")
        return
        
    os.makedirs(ARCHIVE_PATH, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    files_processed = 0
    
    for filename in os.listdir(INPUT_PATH):
        if filename.endswith(('.csv', '.json')):
            src = os.path.join(INPUT_PATH, filename)
            base, ext = os.path.splitext(filename)
            # Handle possible compound extensions or simple split
            new_filename = f"{base}_{timestamp}{ext}"
            dest = os.path.join(ARCHIVE_PATH, new_filename)
            
            try:
                shutil.move(src, dest)
                print(f"Archived: {filename} -> {new_filename}")
                log_operation("archive", f"Archived input file: {filename} -> {new_filename}")
                files_processed += 1
            except Exception as e:
                err_msg = f"Failed to archive file {filename}: {str(e)}"
                print(err_msg)
                log_operation("archive_error", err_msg)
                
    if files_processed == 0:
        print("No CSV or JSON files found in input path to archive.")

if __name__ == "__main__":
    archive_files()

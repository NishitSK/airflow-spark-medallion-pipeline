import os
import csv
import subprocess
import shutil

def main():
    input_dir = r"c:\Users\Acer\Desktop\Newfolder\protothon1 - Copy\airflow-spark-medallion-pipeline\data\input"
    test_file = os.path.join(input_dir, "robustness_test.csv")
    
    # 1. Clean input directory of existing files first
    if os.path.exists(input_dir):
        for f in os.listdir(input_dir):
            if f.endswith(('.csv', '.json')):
                os.remove(os.path.join(input_dir, f))
    else:
        os.makedirs(input_dir, exist_ok=True)
        
    # 2. Write the dirty dataset
    # Schema: id, name, age
    # Row 1: normal whole numbers
    # Row 2: whole numbers formatted as floats (1002.0, 40.0)
    # Row 3: whole numbers formatted as floats with trailing zeros (1003.000, 50.00)
    # Row 4: empty string values
    # Row 5: null/None values
    # Row 6: malformed string values ("abc", "xyz")
    # Row 7: duplicate id after normalization (1001.0)
    data = [
        ["1001", "Alice", "30"],
        ["1002.0", "Bob", "40.0"],
        ["1003.000", "Charlie", "50.00"],
        ["", "David", ""],
        [None, "Eve", None],
        ["abc", "Frank", "xyz"],
        ["1001.0", "Alice Duplicate", "30"]
    ]
    
    with open(test_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age"])
        for row in data:
            writer.writerow(row)
            
    print(f"Generated test dataset at {test_file}")
    
    # 3. Execute the pipeline
    cmd = [
        r"C:\Users\Acer\AppData\Local\Programs\Python\Python311\python.exe",
        r"c:\Users\Acer\Desktop\Newfolder\protothon1 - Copy\airflow-spark-medallion-pipeline\spark_jobs\unified_pipeline.py"
    ]
    
    env = os.environ.copy()
    env["PYTHONPATH"] = r"c:\Users\Acer\Desktop\Newfolder\protothon1 - Copy\airflow-spark-medallion-pipeline"
    
    print("Running unified medallion pipeline...")
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    
    print("\n--- Pipeline Execution STDOUT ---")
    print(result.stdout)
    
    print("\n--- Pipeline Execution STDERR ---")
    print(result.stderr)
    
    if result.returncode == 0:
        print("\nSUCCESS: Pipeline ran to completion successfully!")
    else:
        print(f"\nFAILURE: Pipeline failed with exit code {result.returncode}")

if __name__ == "__main__":
    main()

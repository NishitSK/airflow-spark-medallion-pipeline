import os
import sys
import csv
import shutil
import subprocess
import time
import json

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(ROOT_DIR, "data", "input")
OUTPUT_DIR = os.path.join(ROOT_DIR, "data", "output")
STATUS_FILE = os.path.join(OUTPUT_DIR, "pipeline_status.json")

def clean_input_and_output():
    if os.path.exists(INPUT_DIR):
        for f in os.listdir(INPUT_DIR):
            if f.endswith(('.csv', '.json')):
                os.remove(os.path.join(INPUT_DIR, f))
    else:
        os.makedirs(INPUT_DIR, exist_ok=True)
        
    # Remove status file if exists
    if os.path.exists(STATUS_FILE):
        try:
            os.remove(STATUS_FILE)
        except Exception:
            pass

def run_pipeline():
    cmd = [sys.executable, os.path.join(ROOT_DIR, "spark_jobs", "unified_pipeline.py")]
    env = os.environ.copy()
    env["PYTHONPATH"] = ROOT_DIR
    
    t0 = time.time()
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    dur = time.time() - t0
    
    # Read status from pipeline_status.json
    status_data = {}
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r") as f:
                status_data = json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load status file: {e}")
            
    return result.returncode, result.stdout, result.stderr, status_data, dur

def write_csv(filename, headers, rows):
    filepath = os.path.join(INPUT_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    return filepath

def run_test_case(name, headers, rows_generator, expected_pass, expected_err_substring=None):
    print(f"\n==================================================")
    print(f" RUNNING {name}")
    print(f"==================================================")
    
    clean_input_and_output()
    
    # Generate CSV
    filepath = os.path.join(INPUT_DIR, f"{name.lower().replace(' ', '_')}.csv")
    t_gen_0 = time.time()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        rows_generator(writer)
    print(f"Generated test dataset in {time.time() - t_gen_0:.2f} seconds")
    
    # Run
    code, stdout, stderr, status, dur = run_pipeline()
    
    print(f"Exit Code: {code}")
    print(f"Duration: {dur:.2f} seconds")
    print(f"Status in JSON: {status.get('status', 'N/A')}")
    print(f"Error in JSON: {status.get('error', 'None')}")
    
    # Verify
    success = True
    if expected_pass:
        if code != 0 or status.get("status") != "completed":
            print(f"[FAIL] Expected PASS, but got exit code {code} and status '{status.get('status')}'")
            print("Pipeline Stderr:\n", stderr)
            print("Pipeline Stdout:\n", stdout)
            success = False
        else:
            print(f"[PASS] Pipeline processed successfully as expected.")
    else:
        if code == 0 or status.get("status") == "completed":
            print(f"[FAIL] Expected FAIL CLEANLY, but pipeline succeeded with exit code {code}")
            success = False
        else:
            err_msg = status.get("error", "") or stderr or stdout
            if expected_err_substring and expected_err_substring.lower() not in err_msg.lower():
                print(f"[FAIL] Expected error substring '{expected_err_substring}', but got: '{err_msg}'")
                print("Pipeline Stderr:\n", stderr)
                success = False
            else:
                print(f"[PASS] Pipeline failed cleanly as expected. Error matches: '{err_msg.strip()}'")
                
    return success

def main():
    test_cases = [
        # TEST 1
        (
            "TEST 1 (id,name,age)",
            ["id", "name", "age"],
            lambda w: w.writerow(["1001", "John Smith", "35"]),
            True,
            None
        ),
        # TEST 2
        (
            "TEST 2 (age,id,name)",
            ["age", "id", "name"],
            lambda w: w.writerow(["35", "1001", "John Smith"]),
            True,
            None
        ),
        # TEST 3
        (
            "TEST 3 (name,age,id,email,phone)",
            ["name", "age", "id", "email", "phone"],
            lambda w: w.writerow(["John Smith", "35", "1001", "john@example.com", "555-0199"]),
            True,
            None
        ),
        # TEST 4
        (
            "TEST 4 (id,name)",
            ["id", "name"],
            lambda w: w.writerow(["1001", "John Smith"]),
            False,
            "Missing required column: age"
        ),
        # TEST 5
        (
            "TEST 5 (order_id,customer_id,product_name,price)",
            ["order_id", "customer_id", "product_name", "price"],
            lambda w: w.writerow(["9001", "2001", "Widget", "19.99"]),
            False,
            "Unsupported dataset schema"
        ),
        # TEST 6
        (
            "TEST 6 (id,name,age,gender,date,month,year)",
            ["id", "name", "age", "gender", "date", "month", "year"],
            lambda w: w.writerow(["1001", "John Smith", "35", "Male", "17", "06", "2026"]),
            True,
            None
        ),
    ]
    
    # Run tests 1 to 6
    overall_success = True
    for tc in test_cases:
        overall_success &= run_test_case(tc[0], tc[1], tc[2], tc[3], tc[4])
        
    # TEST 7: 1 million row dataset with supported schema
    def test_7_rows(writer):
        for i in range(1, 1000001):
            writer.writerow([str(i), f"User_{i}", "30"])
    overall_success &= run_test_case(
        "TEST 7 (1M Rows Supported)",
        ["id", "name", "age"],
        test_7_rows,
        True,
        None
    )
    
    # TEST 8: 1 million row dataset with unsupported schema
    def test_8_rows(writer):
        for i in range(1, 1000001):
            writer.writerow([str(i), f"Cust_{i}", f"Product_{i}", "19.99"])
    overall_success &= run_test_case(
        "TEST 8 (1M Rows Unsupported)",
        ["order_id", "customer_id", "product_name", "price"],
        test_8_rows,
        False,
        "Unsupported dataset schema"
    )
    
    print("\n" + "="*50)
    if overall_success:
        print("[SUCCESS] ALL TESTS PASSED SUCCESSFULLY!")
    else:
        print("[FAIL] SOME TEST CASES FAILED!")
    print("="*50)
    
    # Return code for bash pipeline integration
    sys.exit(0 if overall_success else 1)

if __name__ == "__main__":
    main()

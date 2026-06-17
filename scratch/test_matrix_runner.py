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
                try:
                    os.remove(os.path.join(INPUT_DIR, f))
                except Exception:
                    pass
    else:
        os.makedirs(INPUT_DIR, exist_ok=True)
        
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
    
    status_data = {}
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r") as f:
                status_data = json.load(f)
        except Exception as e:
            print(f"Warning: Failed to load status file: {e}")
            
    return result.returncode, result.stdout, result.stderr, status_data, dur

def run_test_case(name, headers, rows_generator, expected_pass, expected_dataset_type=None, expected_err_substring=None):
    print(f"\n==================================================")
    print(f" RUNNING {name}")
    print(f"==================================================")
    
    clean_input_and_output()
    
    # Generate CSV with a clean file name
    sanitized_name = name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace(",", "_")
    filepath = os.path.join(INPUT_DIR, f"{sanitized_name}.csv")
    t_gen_0 = time.time()
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        rows_generator(writer)
    print(f"Generated test dataset in {time.time() - t_gen_0:.2f} seconds")
    
    # Run the pipeline script
    code, stdout, stderr, status, dur = run_pipeline()
    
    print(f"Exit Code: {code}")
    print(f"Duration: {dur:.2f} seconds")
    print(f"Status in JSON: {status.get('status', 'N/A')}")
    print(f"Dataset Type in JSON: {status.get('dataset_type', 'N/A')}")
    print(f"Error in JSON: {status.get('error', 'None')}")
    
    # Verify results
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
            print(f"[FAIL] Expected FAIL, but pipeline succeeded.")
            success = False
        else:
            err_msg = status.get("error", "") or stderr or stdout
            if expected_err_substring and expected_err_substring.lower() not in err_msg.lower():
                print(f"[FAIL] Expected error substring '{expected_err_substring}', but got: '{err_msg}'")
                print("Pipeline Stderr:\n", stderr)
                success = False
            else:
                print(f"[PASS] Pipeline failed as expected. Error matches: '{err_msg.strip()}'")
                
    if expected_dataset_type:
        actual_type = status.get("dataset_type", "N/A")
        if actual_type != expected_dataset_type:
            print(f"[FAIL] Expected Dataset Type '{expected_dataset_type}', but got '{actual_type}'")
            success = False
        else:
            print(f"[PASS] Dataset Type matches expected: '{actual_type}'")
                
    return success, dur

def main():
    results = []
    overall_success = True
    
    # TEST 1: id,name,age -> CUSTOMER
    success, dur = run_test_case(
        "TEST 1 (id,name,age)",
        ["id", "name", "age"],
        lambda w: w.writerow(["1001", "John Smith", "35"]),
        expected_pass=True,
        expected_dataset_type="CUSTOMER"
    )
    results.append(("TEST 1", success, dur))
    overall_success &= success
    
    # TEST 2: age,id,name -> CUSTOMER
    success, dur = run_test_case(
        "TEST 2 (age,id,name)",
        ["age", "id", "name"],
        lambda w: w.writerow(["35", "1001", "John Smith"]),
        expected_pass=True,
        expected_dataset_type="CUSTOMER"
    )
    results.append(("TEST 2", success, dur))
    overall_success &= success
    
    # TEST 3: id,name,age,email,phone -> CUSTOMER
    success, dur = run_test_case(
        "TEST 3 (id,name,age,email,phone)",
        ["id", "name", "age", "email", "phone"],
        lambda w: w.writerow(["1001", "John Smith", "35", "john@example.com", "555-0101"]),
        expected_pass=True,
        expected_dataset_type="CUSTOMER"
    )
    results.append(("TEST 3", success, dur))
    overall_success &= success
    
    # TEST 4: order_id,product_name,quantity,unit_price -> ORDERS
    success, dur = run_test_case(
        "TEST 4 (order_id,product_name,quantity,unit_price)",
        ["order_id", "product_name", "quantity", "unit_price"],
        lambda w: w.writerow(["9001", "Widget A", "5", "19.99"]),
        expected_pass=True,
        expected_dataset_type="ORDERS"
    )
    results.append(("TEST 4", success, dur))
    overall_success &= success
    
    # TEST 5: order_id,product_name,quantity,unit_price,order_date -> ORDERS
    success, dur = run_test_case(
        "TEST 5 (order_id,product_name,quantity,unit_price,order_date)",
        ["order_id", "product_name", "quantity", "unit_price", "order_date"],
        lambda w: w.writerow(["9001", "Widget A", "5", "19.99", "2026-06-17"]),
        expected_pass=True,
        expected_dataset_type="ORDERS"
    )
    results.append(("TEST 5", success, dur))
    overall_success &= success
    
    # TEST 6: employee_id,department,salary -> GENERIC
    success, dur = run_test_case(
        "TEST 6 (employee_id,department,salary)",
        ["employee_id", "department", "salary"],
        lambda w: w.writerow(["E101", "Sales", "65000.00"]),
        expected_pass=True,
        expected_dataset_type="GENERIC"
    )
    results.append(("TEST 6", success, dur))
    overall_success &= success
    
    # TEST 7: random columns -> GENERIC
    success, dur = run_test_case(
        "TEST 7 (col_a,col_b,col_c)",
        ["col_a", "col_b", "col_c"],
        lambda w: w.writerow(["ValA", "ValB", "ValC"]),
        expected_pass=True,
        expected_dataset_type="GENERIC"
    )
    results.append(("TEST 7", success, dur))
    overall_success &= success
    
    # TEST 8: 1 million rows customer schema -> CUSTOMER
    def test_8_rows(writer):
        for i in range(1, 1000001):
            writer.writerow([str(i), f"User_{i}", "30"])
    success, dur = run_test_case(
        "TEST 8 (1M Rows Customer Schema)",
        ["id", "name", "age"],
        test_8_rows,
        expected_pass=True,
        expected_dataset_type="CUSTOMER"
    )
    results.append(("TEST 8", success, dur))
    overall_success &= success
    
    # TEST 9: 1 million rows orders schema -> ORDERS
    def test_9_rows(writer):
        for i in range(1, 1000001):
            writer.writerow([f"O_{i}", f"Product_{i}", "2", "9.99"])
    success, dur = run_test_case(
        "TEST 9 (1M Rows Orders Schema)",
        ["order_id", "product_name", "quantity", "unit_price"],
        test_9_rows,
        expected_pass=True,
        expected_dataset_type="ORDERS"
    )
    results.append(("TEST 9", success, dur))
    overall_success &= success
    
    # TEST 10: 1 million rows unknown schema -> GENERIC
    def test_10_rows(writer):
        for i in range(1, 1000001):
            writer.writerow([f"E_{i}", "Engineering", "95000"])
    success, dur = run_test_case(
        "TEST 10 (1M Rows Unknown Schema)",
        ["employee_id", "department", "salary"],
        test_10_rows,
        expected_pass=True,
        expected_dataset_type="GENERIC"
    )
    results.append(("TEST 10", success, dur))
    overall_success &= success
    
    # Print summary table
    print("\n" + "="*60)
    print("                TEST MATRIX RESULT SUMMARY")
    print("="*60)
    print(f"{'Test Case':<10} | {'Status':<10} | {'Duration (s)':<15}")
    print("-"*60)
    for name, success, dur in results:
        status_str = "SUCCESS" if success else "FAILED"
        print(f"{name:<10} | {status_str:<10} | {dur:.2f}")
    print("="*60)
    
    if overall_success:
        print("[SUCCESS] ALL 10 TESTS PASSED SUCCESSFULLY!")
    else:
        print("[FAIL] SOME TEST CASES FAILED!")
    print("="*60)
    
    sys.exit(0 if overall_success else 1)

if __name__ == "__main__":
    main()

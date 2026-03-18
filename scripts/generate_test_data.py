import csv
import random
import os

def generate_data(file_path, num_records=1000):
    header = ['id', 'name', 'age']
    names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy']
    
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        
        for i in range(1, num_records + 1):
            name = random.choice(names)
            # 10% chance of missing age
            age = random.randint(18, 80) if random.random() > 0.1 else ""
            writer.writerow([i, name, age])
            
            # 5% chance of creating a duplicate record
            if random.random() < 0.05:
                writer.writerow([i, name, age])

    print(f"Generated {num_records} records (plus duplicates) in {file_path}")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_file = os.path.join(project_root, "data", "input", "raw_data.csv")
    generate_data(output_file, num_records=1500)

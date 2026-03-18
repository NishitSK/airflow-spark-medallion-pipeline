import csv
import random
import os

names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"]

# Target path OUTSIDE the current project directory
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
parent_dir = os.path.dirname(project_root)
output_dir = os.path.join(parent_dir, 'ultimate_large_data')
output_file = os.path.join(output_dir, 'ultimate_raw_data.csv')

os.makedirs(output_dir, exist_ok=True)

print(f"Generating 1,100,000 rows in {output_file}...")

# Writing in chunks to be memory efficient
with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "age"])
    
    # Generate 1,000,000 unique records
    for i in range(1, 1000001):
        name = random.choice(names)
        age = random.randint(18, 80) if random.random() > 0.05 else ""
        writer.writerow([i, name, age])
        
        if i % 100000 == 0:
            print(f"Progress: {i} rows written...")

    # Append 100,000 duplicate records (sampled from the first 100k for simplicity)
    print("Adding 100,000 duplicates...")
    for i in range(1, 100001):
        name = random.choice(names)
        age = random.randint(18, 80)
        writer.writerow([i, name, age]) # Reuse IDs 1-100,000

print(f"Successfully generated 1,100,000 rows.")

import csv
import random
import os

names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"]

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
output_file = os.path.join(project_root, 'data', 'input', 'raw_data.csv')

# Step 1: Generate 100 unique rows
rows = []
for i in range(1, 101):
    name = random.choice(names)
    # 20% chance of null (empty string) age
    age = random.randint(18, 80) if random.random() > 0.2 else ""
    rows.append([i, name, age])

# Step 2: Add ~20 duplicate rows by sampling from existing rows
duplicates = random.sample(rows, 20)
rows.extend(duplicates)

# Step 3: Shuffle to mix duplicates throughout the file
random.shuffle(rows)

# Step 4: Write to CSV
os.makedirs(os.path.dirname(output_file), exist_ok=True)
with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "age"])
    writer.writerows(rows)

print(f"Generated {len(rows)} rows with duplicates and nulls in {output_file}")

import csv
import random
import os

names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"]

# Target path OUTSIDE the current project directory (one level up from project root)
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
parent_dir = os.path.dirname(project_root)
output_dir = os.path.join(parent_dir, 'extra_large_data')
output_file = os.path.join(output_dir, 'extra_large_raw_data.csv')

print(f"Target Directory: {output_dir}")
os.makedirs(output_dir, exist_ok=True)

# Generate 50,000 unique records
rows = []
for i in range(1, 50001):
    name = random.choice(names)
    # 10% chance of null (empty string) age
    age = random.randint(18, 80) if random.random() > 0.1 else ""
    rows.append([i, name, age])

# Add 5,000 duplicate rows
duplicates = random.sample(rows, 5000)
rows.extend(duplicates)

# Shuffle to mix duplicates throughout the file
random.shuffle(rows)

with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "age"])
    writer.writerows(rows)

print(f"Successfully generated {len(rows)} rows in {output_file}")

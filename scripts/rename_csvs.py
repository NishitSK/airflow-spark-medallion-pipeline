import os
import glob
import csv

files = glob.glob('c:/Users/Acer/Desktop/New folder/protothon1/Data-Engineering-Pipeline/data/**/*.csv', recursive=True)

for f in files:
    try:
        size = os.path.getsize(f)
        
        dupes = 0
        nulls = 0
        seen_rows = set()
        
        with open(f, 'r', encoding='utf-8', errors='ignore') as file:
            reader = csv.reader(file)
            try:
                headers = next(reader, None)
            except Exception:
                pass
            
            for row in reader:
                nulls += sum(1 for val in row if not val or val.strip() == "")
                row_tuple = tuple(row)
                if row_tuple in seen_rows:
                    dupes += 1
                else:
                    seen_rows.add(row_tuple)
        
        basename = os.path.basename(f)
        if "bytes_" in basename:
            basename = basename.split("bytes_", 1)[1]
            
        new_name = f"s{size}d{dupes}n{nulls}_{basename}"
        new_path = os.path.join(os.path.dirname(f), new_name)
        
        print(f"Renaming {f} to {new_path}")
        os.rename(f, new_path)
    except Exception as e:
        print(f"Failed to process {f}: {e}")

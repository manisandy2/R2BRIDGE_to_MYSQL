import csv
import json

csv_file = "json_backups/sample_serial_records_with_random_names.csv"
json_file = "json_backups/sample_serial.json"

def auto_convert(value):
    # Try parsing JSON
    try:
        return json.loads(value)
    except (ValueError, TypeError):
        pass
    # Try numbers
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value  # keep string

data = []
with open(csv_file, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        converted_row = {k: auto_convert(v) for k, v in row.items()}
        data.append(converted_row)

with open(json_file, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4, ensure_ascii=False)

print(f"✅ CSV converted to JSON and saved as {json_file}")
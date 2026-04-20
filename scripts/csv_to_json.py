import csv
import json
import requests
import os

URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Folder setup
os.makedirs("data", exist_ok=True)

csv_path = "data/api-scrip-master.csv"

# 🔥 CONFIG
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB

# Download CSV
response = requests.get(URL)
with open(csv_path, "wb") as f:
    f.write(response.content)

print("✅ CSV downloaded")

# Convert CSV → JSON (batched + filtered)
file_index = 1
current_size = 0
current_data = []

def save_batch(data, index):
    file_path = f"data/api-scrip-master-{index}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    print(f"✅ Saved {file_path} ({len(data)} records)")


with open(csv_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        segment = row.get("SEM_SEGMENT", "")
        instrument = row.get("SEM_INSTRUMENT_NAME", "")

        # 🔥 MAIN FILTER → only D (derivatives) + I (index)
        if segment not in ["D", "I"]:
            continue

        # 🔥 OPTIONAL: keep only OPTIONS inside D (skip futures)
        if segment == "D" and instrument not in ["OPTIDX", "OPTSTK"]:
            continue

        # 🔥 OPTIONAL: reduce fields (recommended for size)
        filtered_row = {
            "id": row.get("SEM_SMST_SECURITY_ID"),
            "symbol": row.get("SEM_TRADING_SYMBOL"),
            "segment": segment,
            "instrument": instrument,
            "expiry": row.get("SEM_EXPIRY_DATE"),
            "strike": row.get("SEM_STRIKE_PRICE"),
            "optionType": row.get("SEM_OPTION_TYPE"),
        }

        row_json = json.dumps(filtered_row)
        row_size = len(row_json.encode("utf-8"))

        # Split into batches (~50MB)
        if current_size + row_size > MAX_FILE_SIZE and current_data:
            save_batch(current_data, file_index)

            file_index += 1
            current_data = []
            current_size = 0

        current_data.append(filtered_row)
        current_size += row_size


# Save last batch
if current_data:
    save_batch(current_data, file_index)

print("🚀 All batches created successfully")

# 🔥 Remove CSV (to avoid pushing large file)
os.remove(csv_path)
print("🗑️ CSV file removed (only batch JSON kept)")
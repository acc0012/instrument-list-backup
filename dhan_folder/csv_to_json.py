import csv
import json
import requests
import os

URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

os.makedirs("data", exist_ok=True)
csv_path = "data/api-scrip-master.csv"

# Download CSV
response = requests.get(URL)
with open(csv_path, "wb") as f:
    f.write(response.content)

print("✅ CSV downloaded")

# Separate containers
nifty_data = []
sensex_data = []

# 🔥 NEW: batching setup
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
file_index = 1
current_size = 0
current_data = []

def save_batch(data, index):
    path = f"data/api-scrip-master-{index}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    print(f"✅ Saved batch {index} ({len(data)} records)")

with open(csv_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        segment = row.get("SEM_SEGMENT", "")
        instrument = row.get("SEM_INSTRUMENT_NAME", "")
        symbol = row.get("SEM_TRADING_SYMBOL", "")

        # 🔥 FILTER
        if segment not in ["D", "I"]:
            continue

        if segment == "D" and instrument not in ["OPTIDX", "OPTSTK"]:
            continue

        filtered_row = {
            "id": row.get("SEM_SMST_SECURITY_ID"),
            "symbol": symbol,
            "segment": segment,
            "instrument": instrument,
            "expiry": row.get("SEM_EXPIRY_DATE"),
            "strike": row.get("SEM_STRIKE_PRICE"),
            "optionType": row.get("SEM_OPTION_TYPE"),
        }

        # 🔥 EXISTING SPLIT LOGIC (unchanged)
        if symbol.startswith("NIFTY"):
            nifty_data.append(filtered_row)

        elif symbol.startswith("SENSEX"):
            sensex_data.append(filtered_row)

        # 🔥 NEW: batching logic
        row_json = json.dumps(filtered_row)
        row_size = len(row_json.encode("utf-8"))

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

# Save split files (unchanged)
with open("data/nifty.json", "w", encoding="utf-8") as f:
    json.dump(nifty_data, f, indent=4)

with open("data/sensex.json", "w", encoding="utf-8") as f:
    json.dump(sensex_data, f, indent=4)

print(f"✅ Saved nifty.json ({len(nifty_data)} records)")
print(f"✅ Saved sensex.json ({len(sensex_data)} records)")

# Remove CSV
os.remove(csv_path)
print("🗑️ CSV removed")

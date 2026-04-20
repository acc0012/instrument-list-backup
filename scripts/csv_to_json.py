import csv
import json
import requests
import os

URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Ensure folder exists
os.makedirs("data", exist_ok=True)
 
csv_path = "data/api-scrip-master.csv"
json_path = "data/api-scrip-master.json"

# Download CSV
response = requests.get(URL)
with open(csv_path, "wb") as f:
    f.write(response.content)

# Convert CSV → JSON
data = []
with open(csv_path, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        data.append(row)

# Save JSON
with open(json_path, "w", encoding='utf-8') as jsonfile:
    json.dump(data, jsonfile, indent=2)

print("✅ CSV downloaded and converted to JSON")
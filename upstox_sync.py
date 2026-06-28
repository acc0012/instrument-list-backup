import os
import json
import gzip
import shutil
import requests
from datetime import datetime
from pymongo import MongoClient
from zoneinfo import ZoneInfo

DOWNLOAD_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

DOWNLOAD_FILE = "NSE.json.gz"
EXTRACTED_FILE = "NSE.json"


def cleanup():
    """Remove temporary downloaded files."""

    print("Cleaning up temporary files...")

    for file in [DOWNLOAD_FILE, EXTRACTED_FILE]:
        if os.path.exists(file):
            os.remove(file)
            print(f"Deleted: {file}")

    print("Cleanup completed.")


def get_mongo_collection():
    """Create MongoDB collection from GitHub Actions environment variables."""

    mongo_uri = os.environ["AUTH_MONGO_URI"]
    mongo_db = "UPSTOX_INSTRUMENTS"
    mongo_collection = "latest_instruments"

    client = MongoClient(mongo_uri)

    db = client[mongo_db]

    return db[mongo_collection]


def download_file():
    print("Downloading NSE instruments...")

    response = requests.get(DOWNLOAD_URL, stream=True)
    response.raise_for_status()

    with open(DOWNLOAD_FILE, "wb") as f:
        for chunk in response.iter_content(8192):
            if chunk:
                f.write(chunk)

    print("Download completed.")


def extract_file():
    print("Extracting...")

    with gzip.open(DOWNLOAD_FILE, "rb") as gz:
        with open(EXTRACTED_FILE, "wb") as out:
            shutil.copyfileobj(gz, out)

    print("Extraction completed.")


def load_json():
    with open(EXTRACTED_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_data(data):
    """
    Filters the downloaded Upstox instrument master to only the
    nearest-expiry NIFTY option contracts and restructures them
    into a strike-wise lookup dictionary.

    Final Structure:

    {
        "23850": {
            "ce": {...},
            "pe": {...}
        },
        "23900": {
            "ce": {...},
            "pe": {...}
        }
    }
    """

    # ------------------------------------------------------
    # Filter only NIFTY F&O contracts
    # ------------------------------------------------------

    filtered = [
        item
        for item in data
        if item.get("segment") == "NSE_FO" and item.get("underlying_symbol") == "NIFTY"
    ]

    if not filtered:
        raise Exception("No NIFTY contracts found.")

    # ------------------------------------------------------
    # Pick nearest expiry
    # ------------------------------------------------------

    nearest_expiry = min(item["expiry"] for item in filtered)

    nearest = [item for item in filtered if item["expiry"] == nearest_expiry]

    expiry = datetime.fromtimestamp(nearest_expiry / 1000).strftime("%Y-%m-%d")

    # ------------------------------------------------------
    # Build strike-wise lookup
    # ------------------------------------------------------

    grouped = {}

    for item in nearest:

        strike = str(int(item["strike_price"]))

        option_type = item["instrument_type"].lower()

        if strike not in grouped:
            grouped[strike] = {
                "ce": None,
                "pe": None,
            }

        grouped[strike][option_type] = item

    # ------------------------------------------------------
    # Remove incomplete strikes (safety)
    # ------------------------------------------------------

    cleaned = {}

    for strike, contracts in grouped.items():

        if contracts["ce"] and contracts["pe"]:
            cleaned[strike] = contracts

    # ------------------------------------------------------
    # Final Mongo document
    # ------------------------------------------------------

    return {
        "expiry": expiry,
        "uploaded_at": datetime.now(ZoneInfo("Asia/Kolkata")).strftime(
            "%Y-%m-%d %I:%M:%S %p IST"
        ),
        "total_records": len(nearest),
        "total_strikes": len(cleaned),
        "data": cleaned,
    }
 

def upload_to_mongodb(document):
    collection = get_mongo_collection()

    print("Deleting existing documents...")

    collection.delete_many({})

    print("Uploading latest document...")

    collection.insert_one(document)

    print("MongoDB upload completed.")


def main():
    try:
        download_file()

        extract_file()

        data = load_json()

        document = filter_data(data)

        upload_to_mongodb(document)

        print("--------------------------------")
        print("Completed Successfully")
        print(f"Expiry        : {document['expiry']}")
        print(f"Total Records : {document['total_records']}")
        print("--------------------------------")

    finally:
        cleanup()


if __name__ == "__main__":
    main()

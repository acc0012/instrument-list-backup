import os
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
# ----------------------------
# Mongo (Auth DB)
# ----------------------------
_client = None
DB_NAME = os.getenv("AUTH_MONGO_DB", "trading")
COLLECTION_NAME = os.getenv("AUTH_MONGO_COLLECTION", "auth")


def _get_collection():
    global _client

    if _client is None:
        uri = os.getenv("AUTH_MONGO_URI")
        if not uri:
            raise ValueError("AUTH_MONGO_URI missing")

        _client = MongoClient(uri)

    db = _client[DB_NAME]
    return db[COLLECTION_NAME]


# ----------------------------
# Fetch token
# ----------------------------
def fetch_token_from_mongo():
    collection = _get_collection()

    data = collection.find_one({"_id": "dhan_token"})
    if not data:
        return None

    data.pop("_id", None)
    return data
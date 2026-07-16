import os
from pymongo import MongoClient

_market_client = None


def get_market_data_collection():
    global _market_client

    if _market_client is None:
        uri = os.getenv("AUTH_MONGO_URI")
        if not uri:
            raise ValueError("AUTH_MONGO_URI missing")

        _market_client = MongoClient(uri)

    db = _market_client[os.getenv("MARKET_DB", "marketdata")]
    return db[os.getenv("OPTION_CHAIN_COLLECTION", "index_option_chain")]

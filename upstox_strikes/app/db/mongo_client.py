from pymongo import MongoClient

from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger("mongo_client")


# Initialize MongoDB client
try:
    client = MongoClient(settings.MONGO_URI)

    # Verify MongoDB connection
    client.server_info()

    logger.info("Successfully connected to MongoDB.")

except Exception as exc:
    logger.error("Could not connect to MongoDB: %s", exc)
    raise


# Access database
db = client[settings.UPSTOX_DB]


# Define collections for easy access
instrument_collection = db[settings.UPSTOX_INST_COLLECTION]
ema_signal_collection = db[settings.EMA_COLLECTION]
ema_signal_testing_collection = db[settings.TEST_EMA_COLLECTION]


logger.info("Connected to DB=%s", settings.UPSTOX_DB)

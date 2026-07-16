from pymongo import MongoClient
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger("mongo_client")

# Initialize the MongoClient
try:
    client = MongoClient(settings.MONGO_URI)

    # Verify connection
    client.server_info()
    logger.info("Successfully connected to MongoDB.")
except Exception as e:
    logger.error(f"Could not connect to MongoDB: {e}")
    raise

# Access the database
db = client[settings.UPSTOX_DB]

# Define collections for easy access
instrument_collection = db[settings.UPSTOX_INST_COLLECTION]
token_collection = db[settings.UPSTOX_TOKEN_COLLECTION]
ema_signal_collection = db[settings.EMA_COLLECTION]

# Added testing collection definition
ema_signal_testing_collection = db[settings.TEST_EMA_COLLECTION]

logger.info(f"Connected to DB={settings.UPSTOX_DB}")

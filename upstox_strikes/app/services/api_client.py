from pymongo import MongoClient

from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger("instrument_loader")

client = MongoClient(settings.MONGO_URL)
db = client[settings.MONGO_DB]
collection = db["latest_instruments"]


def fetch_instrument_range(
    min_strike: int = 23400,
    max_strike: int = 24600,
):
    """
    Fetch instruments directly from MongoDB.
    """

    try:
        document = collection.find_one()

        if not document:
            logger.error("No instrument document found.")
            return {}

        strikes = document.get("data", {})

        filtered = {
            strike: value
            for strike, value in strikes.items()
            if min_strike <= int(strike) <= max_strike
        }

        logger.info(
            f"Loaded {len(filtered)} strikes from MongoDB "
            f"(expiry={document.get('expiry')})"
        )

        return filtered

    except Exception as e:
        logger.exception(f"Failed to load instruments: {e}")
        raise

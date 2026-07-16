import app.core.token_cache as token_cache
from app.db.mongo_client import (
    token_collection,
)  # Correct import from your mongo_client.py
from app.core.logger import get_logger

logger = get_logger("upstox_token")


def mask_token(token: str) -> str:
    """
    Show only first few and last few characters
    of the token for logging purposes.
    """
    if not token:
        return "EMPTY"

    if len(token) <= 15:
        return f"{token[:5]}*****"

    return f"{token[:10]}...{token[-5:]}"


def get_access_token() -> str | None:
    """
    Retrieve the access token from the memory cache.
    """
    return token_cache.upstox_access_token


def load_access_token() -> None:
    """
    Load Upstox access token from MongoDB into the memory cache.
    """
    try:
        logger.info("Loading Upstox access token from MongoDB...")

        # Use the token_collection imported from app.db.mongo_client
        doc = token_collection.find_one({"_id": "upstox_access_token"})

        if not doc:
            logger.warning("Upstox access token document not found in database.")
            return

        access_token = doc.get("access_token")

        if not access_token:
            logger.warning("Access token field is empty in database.")
            return

        # Update the in-memory cache
        token_cache.upstox_access_token = access_token

        logger.info(
            "Upstox access token fetched successfully. Token: %s | Updated at: %s",
            mask_token(access_token),
            doc.get("updated_at"),
        )

    except Exception as exc:
        logger.exception(
            "Failed to load Upstox access token: %s",
            str(exc),
        )
        raise

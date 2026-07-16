from dotenv import load_dotenv
import os
from zoneinfo import ZoneInfo

load_dotenv()


class Settings:
    # Database Configuration
    MONGO_URI = os.getenv("MONGO_URI")

    UPSTOX_DB = os.getenv("UPSTOX_DB") or "UPSTOX_APP"

    UPSTOX_INST_COLLECTION = (
        os.getenv("UPSTOX_INST_COLLECTION") or "latest_instruments"
    )

    UPSTOX_TOKEN_COLLECTION = (
        os.getenv("UPSTOX_TOKEN_COLLECTION") or "upstox_tokens"
    )

    EMA_COLLECTION = (
        os.getenv("EMA_COLLECTION") or "market_analysis"
    )

    # Test Collection
    TEST_EMA_COLLECTION = (
        os.getenv("TEST_EMA_COLLECTION") or "market_analysis_testing"
    )

    # Telegram Configuration
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

    # API Configuration
    API_BASE_URL = os.getenv("API_BASE_URL")

    TEST_RUN = False
    SAVE_RES = False

    # Timezone
    IST = ZoneInfo("Asia/Kolkata")


settings = Settings()
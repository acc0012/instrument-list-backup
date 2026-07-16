import requests
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger("api_client")


def fetch_instrument_range(min_strike: int = 23400, max_strike: int = 24600):
    """
    Fetch the list of instruments from the existing FastAPI service.
    """
    url = f"{settings.API_BASE_URL}/api/v1/instruments/range"
    params = {"min_strike": min_strike, "max_strike": max_strike}

    try:
        logger.info(f"Fetching instruments from {url} with params {params}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        if data.get("success"):
            logger.info(f"Successfully fetched {data.get('count')} instruments.")
            return data.get("data", {})
        else:
            logger.error("API returned success=False")
            return {}

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching instruments from API: {e}")
        raise

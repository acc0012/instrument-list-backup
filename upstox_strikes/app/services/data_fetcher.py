import json
from pathlib import Path
from datetime import datetime, timedelta

import upstox_client
from upstox_client.rest import ApiException
from app.core.config import settings
from app.core.logger import get_logger
from app.services.token_service import get_access_token

logger = get_logger("data_fetcher")

api_version = "2.0"

RESPONSE_DIR = Path("responses")
RESPONSE_DIR.mkdir(exist_ok=True)


def get_last_market_days(days=2, start_from_today=True):
    """
    Calculate last N market days.
    If start_from_today is True, includes current date.
    If False, starts from yesterday (the last completed market day).
    """
    market_days = []
    # Start checking from today, or yesterday if we want completed days only
    current_date = datetime.now()
    if not start_from_today:
        current_date -= timedelta(days=1)

    while len(market_days) < days:
        # Check if weekday (Mon=0, Sun=6)
        if current_date.weekday() < 5:
            market_days.append(current_date.strftime("%Y-%m-%d"))
        current_date -= timedelta(days=1)

    return market_days


def fetch_candles(
    instrument_key,
    interval="1minute",
    include_intraday=False,
    save_response=settings.SAVE_RES,
):
    token = get_access_token()
    if not token:
        msg = f"Access token not found. {len(token)}" if token else "Access token not found or None."
        logger.error(msg)
        return None

    configuration = upstox_client.Configuration()
    configuration.access_token = token
    api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))

    intraday_response = None
    historical_responses = {}
    intraday_candles = []
    historical_candles = []

    # ======================================================
    # Logic:
    # If include_intraday=True: Fetch Today + 2 Prev Days (Total 3)
    # If include_intraday=False: Fetch 3 Completed Prev Days
    # ======================================================
    historical_days_count = 6 if include_intraday else 7
    
    if include_intraday:
        try:
            logger.info("Fetching intraday data for %s", instrument_key)
            response = api_instance.get_intra_day_candle_data(
                instrument_key, interval, api_version
            )
            intraday_response = response.to_dict()
            if response.data and response.data.candles:
                intraday_candles.extend(response.data.candles)
        except ApiException as exc:
            logger.error("Intraday API Error: %s", exc)

    # Fetch historical based on whether we include today or not
    market_days = get_last_market_days(
        days=historical_days_count, start_from_today=include_intraday
    )

    for day in market_days:
        try:
            logger.info("Fetching historical data for %s (%s)", instrument_key, day)
            response = api_instance.get_historical_candle_data1(
                instrument_key, interval, day, day, api_version
            )
            historical_responses[day] = response.to_dict()
            if response.data and response.data.candles:
                historical_candles.extend(response.data.candles)
        except ApiException as exc:
            logger.error("Historical API Error (%s): %s", day, exc)

    merged_candles = intraday_candles + historical_candles
    if merged_candles:
        merged_candles.sort(key=lambda x: x[0])

    trading_dates = sorted({candle[0][:10] for candle in merged_candles})
    summary = {
        "instrument_key": instrument_key,
        "interval": interval,
        "include_intraday": include_intraday,
        "generated_at": datetime.now().isoformat(),
        "total_candles": len(merged_candles),
        "trading_dates": trading_dates,
        "start_candle": (merged_candles[0][0] if merged_candles else None),
        "end_candle": (merged_candles[-1][0] if merged_candles else None),
    }

    result = {"summary": summary, "merged_candles": merged_candles}

    if save_response:
        # Use suffix to differentiate between "live" files and "historical" files
        suffix = "intraday" if include_intraday else "historical"
        clean_key = instrument_key.replace("|", "_")
        filename = RESPONSE_DIR / f"{clean_key}_{suffix}.json"

        with open(filename, "w", encoding="utf-8") as fp:
            json.dump(result, fp, indent=4, ensure_ascii=False)
        logger.info("Saved result to %s", filename)

    return result

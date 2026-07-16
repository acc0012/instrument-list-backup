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

# ==========================================================
# Response Folder
# ==========================================================

RESPONSE_DIR = Path("responses")
RESPONSE_DIR.mkdir(exist_ok=True)


def get_last_market_days(days=2):
    """
    Calculate last N market days excluding weekends.
    """
    market_days = []

    current_date = datetime.now()

    while len(market_days) < days:
        current_date -= timedelta(days=1)

        if current_date.weekday() < 5:
            market_days.append(current_date.strftime("%Y-%m-%d"))

    return market_days


def fetch_historical_candles(
    instrument_key,
    interval="1minute",
    save_response=settings.SAVE_RES,
):
    """
    Fetch:
        - Today's intraday candles
        - Previous market day candles

    Returns:
        {
            summary,
            merged_candles,
            raw_responses
        }
    """

    token = get_access_token()

    if not token:
        logger.error("Access token not found.")
        return None

    configuration = upstox_client.Configuration()
    configuration.access_token = token

    api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))

    intraday_response = None
    historical_responses = {}

    intraday_candles = []
    historical_candles = []

    # ======================================================
    # Intraday
    # ======================================================

    try:

        logger.info(
            "Fetching intraday data for %s",
            instrument_key,
        )

        response = api_instance.get_intra_day_candle_data(
            instrument_key,
            interval,
            api_version,
        )

        intraday_response = response.to_dict()

        if response.data and response.data.candles:
            intraday_candles.extend(response.data.candles)

    except ApiException as exc:

        logger.error(
            "Intraday API Error : %s",
            exc,
        )

    # ======================================================
    # Historical
    # ======================================================

    market_days = get_last_market_days(2)

    for day in market_days:

        try:

            logger.info(
                "Fetching historical data for %s (%s)",
                instrument_key,
                day,
            )

            response = api_instance.get_historical_candle_data1(
                instrument_key,
                interval,
                day,
                day,
                api_version,
            )

            historical_responses[day] = response.to_dict()

            if response.data and response.data.candles:

                historical_candles.extend(response.data.candles)

        except ApiException as exc:

            logger.error(
                "Historical API Error (%s): %s",
                day,
                exc,
            )

    # ======================================================
    # Merge
    # ======================================================

    merged_candles = intraday_candles + historical_candles

    # oldest -> newest
    merged_candles.sort(key=lambda x: x[0])

    # ======================================================
    # Summary
    # ======================================================

    trading_dates = sorted({candle[0][:10] for candle in merged_candles})

    summary = {
        "instrument_key": instrument_key,
        "interval": interval,
        "generated_at": datetime.now().isoformat(),
        "historical_days_requested": len(market_days),
        "total_api_calls": 1 + len(market_days),
        "intraday_candles": len(intraday_candles),
        "historical_candles": len(historical_candles),
        "total_candles": len(merged_candles),
        "trading_days": len(trading_dates),
        "trading_dates": trading_dates,
        "start_candle": (merged_candles[0][0] if merged_candles else None),
        "end_candle": (merged_candles[-1][0] if merged_candles else None),
    }

    result = {
        "summary": summary,
        "merged_candles": merged_candles,
        "raw_responses": {
            "intraday": intraday_response,
            "historical": historical_responses,
        },
    }

    # ======================================================
    # Save
    # ======================================================

    if save_response:

        filename = RESPONSE_DIR / f"{instrument_key.replace('|','_')}.json"

        with open(
            filename,
            "w",
            encoding="utf-8",
        ) as fp:

            json.dump(
                result,
                fp,
                indent=4,
                ensure_ascii=False,
            )

        logger.info(
            "Saved merged response -> %s",
            filename,
        )

    logger.info(
        "Fetched %s candles | %s -> %s",
        summary["total_candles"],
        summary["start_candle"],
        summary["end_candle"],
    )

    return result

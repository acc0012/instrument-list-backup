import json
from pathlib import Path
from datetime import datetime, timedelta

import upstox_client
from upstox_client.rest import ApiException

from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger("data_fetcher")

api_version = "2.0"

RESPONSE_DIR = Path("responses")
RESPONSE_DIR.mkdir(exist_ok=True)


def get_last_market_days(days=2, start_from_today=True):
    """
    Calculate last N market days.

    If start_from_today is True:
        Includes current date if it is a weekday.

    If start_from_today is False:
        Starts from yesterday, meaning only completed market days are considered.

    Note:
        This only excludes Saturday/Sunday.
        It does not exclude exchange holidays.
    """
    market_days = []

    current_date = datetime.now()

    if not start_from_today:
        current_date -= timedelta(days=1)

    while len(market_days) < days:
        # Monday = 0, Sunday = 6
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
    """
    Fetch candle data from Upstox Historical Candle API.

    Token is intentionally not used in this project flow.

    Project requirement:
        Fetch historical candles,
        calculate EMA,
        save latest trading date EMA cross result to MongoDB.

    Default behavior:
        include_intraday=False
        Fetches 7 completed previous weekday candles.

    If include_intraday=True:
        Fetches intraday candles for current day plus historical candles.
    """

    api_instance = upstox_client.HistoryApi()

    intraday_response = None
    historical_responses = {}
    intraday_candles = []
    historical_candles = []

    # ======================================================
    # Logic:
    # If include_intraday=True:
    #     Fetch intraday data for today + 6 historical days
    #
    # If include_intraday=False:
    #     Fetch 7 completed previous market days
    # ======================================================
    historical_days_count = 6 if include_intraday else 7

    if include_intraday:
        try:
            logger.info("Fetching intraday data for %s", instrument_key)

            response = api_instance.get_intra_day_candle_data(
                instrument_key,
                interval,
                api_version,
            )

            intraday_response = response.to_dict()

            if response.data and response.data.candles:
                intraday_candles.extend(response.data.candles)

            logger.info(
                "Fetched %d intraday candles for %s",
                len(intraday_candles),
                instrument_key,
            )

        except ApiException as exc:
            logger.error("Intraday API Error for %s: %s", instrument_key, exc)

        except Exception as exc:
            logger.exception(
                "Unexpected error while fetching intraday data for %s: %s",
                instrument_key,
                exc,
            )

    # Fetch historical candles
    market_days = get_last_market_days(
        days=historical_days_count,
        start_from_today=include_intraday,
    )

    logger.info(
        "Historical market days selected for %s: %s",
        instrument_key,
        ", ".join(market_days),
    )

    for day in market_days:
        try:
            logger.info(
                "Fetching historical data for %s on %s",
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

                logger.info(
                    "Fetched %d candles for %s on %s",
                    len(response.data.candles),
                    instrument_key,
                    day,
                )
            else:
                logger.warning(
                    "No candles returned for %s on %s",
                    instrument_key,
                    day,
                )

        except ApiException as exc:
            logger.error(
                "Historical API Error for %s on %s: %s",
                instrument_key,
                day,
                exc,
            )

        except Exception as exc:
            logger.exception(
                "Unexpected error while fetching historical data for %s on %s: %s",
                instrument_key,
                day,
                exc,
            )

    merged_candles = intraday_candles + historical_candles

    if merged_candles:
        merged_candles.sort(key=lambda x: x[0])

    trading_dates = sorted({candle[0][:10] for candle in merged_candles})

    summary = {
        "instrument_key": instrument_key,
        "interval": interval,
        "include_intraday": include_intraday,
        "generated_at": datetime.now().isoformat(),
        "requested_market_days": market_days,
        "total_candles": len(merged_candles),
        "trading_days": len(trading_dates),
        "trading_dates": trading_dates,
        "start_candle": merged_candles[0][0] if merged_candles else None,
        "end_candle": merged_candles[-1][0] if merged_candles else None,
    }

    result = {
        "summary": summary,
        "merged_candles": merged_candles,
    }

    if save_response:
        suffix = "intraday" if include_intraday else "historical"
        clean_key = instrument_key.replace("|", "_")
        filename = RESPONSE_DIR / f"{clean_key}_{suffix}.json"

        with open(filename, "w", encoding="utf-8") as fp:
            json.dump(result, fp, indent=4, ensure_ascii=False)

        logger.info("Saved candle response to %s", filename)

    logger.info(
        "Completed candle fetch for %s | Total candles=%d | Trading dates=%s",
        instrument_key,
        len(merged_candles),
        trading_dates,
    )

    return result
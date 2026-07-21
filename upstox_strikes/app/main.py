import sys
import os
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

# Fix for "ModuleNotFoundError"
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.logger import get_logger
from app.db.mongo_client import (
    ema_signal_collection,
    ema_signal_testing_collection,
)
from app.services.api_client import fetch_instrument_range
from app.services.data_fetcher import fetch_candles
from app.services.ema_calculator import calculate_ema_cross
from app.services.telegram_service import (
    send_project_start_message,
    send_project_completed_message,
    send_project_failed_message,
    send_high_error_alert,
)
from app.core.config import settings

logger = get_logger("main")

IST = timezone(timedelta(hours=5, minutes=30))
SLEEP_BETWEEN_REQUESTS = 1  # seconds
ERROR_THRESHOLD = 5  # send alert if errors exceed this
DAYS_TO_KEEP = 4  # today + 3 previous trading days


def parse_candle_timestamp(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


def normalize_candles(candles: List[Any]) -> List[Dict[str, Any]]:
    """
    Convert raw candles to list of dictionaries.
    """
    if not candles:
        return []

    if isinstance(candles[0], dict):
        return candles

    normalized = []

    for c in candles:
        if len(c) >= 6:
            normalized.append(
                {
                    "timestamp": c[0],
                    "open": c[1],
                    "high": c[2],
                    "low": c[3],
                    "close": c[4],
                    "volume": c[5],
                    "oi": c[6] if len(c) > 6 else 0,
                }
            )
        else:
            logger.warning("Skipping malformed candle: %s", c)

    return normalized


def compute_ema_cross_series(
    candles: List[Dict[str, Any]],
    short_window: int = 9,
    long_window: int = 21,
) -> List[Dict[str, Any]]:
    """
    Compute all EMA crossover events in the candle series.
    """
    if len(candles) < long_window:
        return []

    closes = [c["close"] for c in candles]
    timestamps = [c["timestamp"] for c in candles]

    def ema(values, window):
        if len(values) < window:
            return []

        ema_vals = [sum(values[:window]) / window]
        multiplier = 2 / (window + 1)

        for price in values[window:]:
            ema_vals.append((price - ema_vals[-1]) * multiplier + ema_vals[-1])

        return ema_vals

    short_ema = ema(closes, short_window)
    long_ema = ema(closes, long_window)

    events = []
    prev_short = None
    prev_long = None

    for i in range(long_window - 1, len(candles)):
        short_idx = i - (short_window - 1)
        long_idx = i - (long_window - 1)

        if short_idx < 0 or long_idx < 0:
            continue

        cur_short = short_ema[short_idx]
        cur_long = long_ema[long_idx]

        if prev_short is not None and prev_long is not None:
            prev_gap = prev_short - prev_long
            cur_gap = cur_short - cur_long

            if prev_gap <= 0 and cur_gap > 0:
                events.append(
                    {
                        "timestamp": timestamps[i],
                        "signal": "BULLISH",
                        "short_ema": cur_short,
                        "long_ema": cur_long,
                        "price": closes[i],
                    }
                )

            elif prev_gap >= 0 and cur_gap < 0:
                events.append(
                    {
                        "timestamp": timestamps[i],
                        "signal": "BEARISH",
                        "short_ema": cur_short,
                        "long_ema": cur_long,
                        "price": closes[i],
                    }
                )

        prev_short = cur_short
        prev_long = cur_long

    return events


def filter_crosses_by_date(
    events: List[Dict[str, Any]],
    target_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Filter EMA crossover events that occurred on the given target date in IST.
    """
    target_date_only = target_date.date()
    filtered = []

    for ev in events:
        ts = ev["timestamp"]

        if isinstance(ts, str):
            dt = parse_candle_timestamp(ts)
        else:
            dt = ts

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        else:
            dt = dt.astimezone(IST)

        if dt.date() == target_date_only:
            ev_copy = ev.copy()
            ev_copy["timestamp"] = dt.isoformat()
            filtered.append(ev_copy)

    return filtered


def filter_candles_by_date(
    candles: List[Dict[str, Any]],
    target_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Extract candles that belong to the given date in IST.

    Note:
        This function is currently not used for storing data.
        It is kept for possible future use.
    """
    target_date_only = target_date.date()
    result = []

    for c in candles:
        ts = c["timestamp"]

        if isinstance(ts, str):
            dt = parse_candle_timestamp(ts)
        else:
            dt = ts

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        else:
            dt = dt.astimezone(IST)

        if dt.date() == target_date_only:
            c_copy = c.copy()
            c_copy["timestamp"] = dt.isoformat()
            result.append(c_copy)

    return result


def prune_daily_entries(collection, filter_query, keep_count=DAYS_TO_KEEP):
    """
    Remove daily entries older than the most recent keep_count days.
    """
    doc = collection.find_one(filter_query, {"daily": 1})

    if not doc or "daily" not in doc:
        return

    daily_keys = list(doc["daily"].keys())

    if len(daily_keys) <= keep_count:
        return

    # Sort descending because YYYY-MM-DD is lexicographically sortable.
    sorted_keys = sorted(daily_keys, reverse=True)
    keys_to_remove = sorted_keys[keep_count:]

    if keys_to_remove:
        unset_dict = {f"daily.{k}": "" for k in keys_to_remove}

        collection.update_one(
            filter_query,
            {"$unset": unset_dict},
        )

        logger.info(
            "Pruned %d old daily entries for %s",
            len(keys_to_remove),
            filter_query,
        )


def get_target_date(
    summary: Dict,
    include_today: bool,
    default_today: datetime,
) -> datetime:
    """
    Determine the target date for filtering and storing daily data.

    If include_today is True:
        Return today's IST date.

    If include_today is False:
        Use the last available date from trading_dates.
    """
    if include_today:
        return default_today

    trading_dates = summary.get("trading_dates")

    if trading_dates and isinstance(trading_dates, list) and len(trading_dates) > 0:
        last_date_str = trading_dates[-1]

        try:
            target_date = datetime.strptime(last_date_str, "%Y-%m-%d").replace(
                tzinfo=IST
            )
            return target_date

        except ValueError:
            logger.warning(
                "Could not parse trading date '%s', falling back to today.",
                last_date_str,
            )
            return default_today

    logger.warning("No trading_dates found in summary, falling back to today.")
    return default_today


def run_daily_ema_analysis(
    test_run: bool = False,
    test_strikes: list[str] | None = None,
    include_today: bool = False,
) -> tuple[int, int]:
    """
    Run EMA analysis and store daily results.

    Returns:
        processed_count, error_count
    """
    logger.info("Starting EMA analysis task... (include_today=%s)", include_today)

    error_count = 0
    processed_count = 0

    # Fetch instruments
    instruments_data = fetch_instrument_range(
        min_strike=23400,
        max_strike=24600,
    )

    if not instruments_data:
        logger.warning("No instruments retrieved.")
        return processed_count, error_count

    # Filter for test mode
    if test_run:
        logger.info("Running in TEST MODE")

        if test_strikes:
            instruments_data = {
                strike: value
                for strike, value in instruments_data.items()
                if strike in test_strikes
            }

            logger.info("Filtered test strikes: %s", ", ".join(test_strikes))

        target_collection = ema_signal_testing_collection

    else:
        logger.info("Running in PRODUCTION MODE")
        target_collection = ema_signal_collection

    now_utc = datetime.now(timezone.utc)
    today_ist = now_utc.astimezone(IST)

    # Process each strike and option type
    for strike, data in instruments_data.items():
        for instrument_type in ["ce", "pe"]:
            inst_details = data.get(instrument_type)

            if not inst_details:
                continue

            instrument_key = inst_details.get("instrument_key")
            trading_symbol = inst_details.get("trading_symbol")

            logger.info("Processing %s (%s)", trading_symbol, instrument_key)

            try:
                # Fetch historical candles without token validation
                response = fetch_candles(
                    instrument_key=instrument_key,
                    interval="1minute",
                    include_intraday=include_today,
                    save_response=settings.SAVE_RES,
                )

                time.sleep(SLEEP_BETWEEN_REQUESTS)

                if not response:
                    logger.warning("No response for %s", trading_symbol)
                    error_count += 1
                    continue

                raw_candles = response.get("merged_candles", [])

                if not raw_candles:
                    logger.warning("No candle data for %s", trading_symbol)
                    error_count += 1
                    continue

                candles = normalize_candles(raw_candles)

                if not candles:
                    logger.warning("Failed to normalize candles for %s", trading_symbol)
                    error_count += 1
                    continue

                summary = response.get("summary", {})

                logger.info(
                    "Loaded %d candles | %s -> %s",
                    summary.get("total_candles", len(candles)),
                    summary.get("start_candle"),
                    summary.get("end_candle"),
                )

                # Determine target date
                target_date = get_target_date(
                    summary=summary,
                    include_today=include_today,
                    default_today=today_ist,
                )

                target_date_str = target_date.strftime("%Y-%m-%d")

                # Compute latest EMA cross
                result = calculate_ema_cross(
                    candles=candles,
                    short_window=9,
                    long_window=21,
                )

                if result is None:
                    logger.warning("EMA calculation failed for %s", trading_symbol)
                    error_count += 1
                    continue

                # Compute all crossovers and filter by target date
                all_crosses = compute_ema_cross_series(
                    candles=candles,
                    short_window=9,
                    long_window=21,
                )

                target_crosses = filter_crosses_by_date(
                    events=all_crosses,
                    target_date=target_date,
                )

                if result["signal"]:
                    logger.info("Latest EMA Signal: %s", result["signal"])
                else:
                    logger.info("No latest EMA crossover for %s", trading_symbol)

                if target_crosses:
                    logger.info(
                        "Found %d EMA crossover(s) on %s for %s",
                        len(target_crosses),
                        target_date_str,
                        trading_symbol,
                    )

                # Build daily entry - minimal data only
                daily_entry = {
                    "instrument_key": instrument_key,
                    "trading_symbol": trading_symbol,
                    "last_price": result["last_price"],
                    "total_candles": summary.get("total_candles"),
                    "start_candle": summary.get("start_candle"),
                    "end_candle": summary.get("end_candle"),
                    "trading_days": summary.get("trading_days"),
                    "trading_dates": summary.get("trading_dates"),
                    "crosses_today": target_crosses,
                }

                filter_query = {
                    "strike": strike,
                    "type": instrument_type.upper(),
                }

                update_data = {
                    "$set": {
                        "instrument_key": instrument_key,
                        "trading_symbol": trading_symbol,
                        f"daily.{target_date_str}": daily_entry,
                        "last_updated": datetime.now(timezone.utc),
                        "latest_crosses": target_crosses,
                        "latest_crosses_date": target_date_str,
                    },
                    "$unset": {
                        "candles": "",
                    },
                }

                target_collection.update_one(
                    filter_query,
                    update_data,
                    upsert=True,
                )

                prune_daily_entries(
                    collection=target_collection,
                    filter_query=filter_query,
                )

                processed_count += 1

                logger.info(
                    "Updated document for %s %s (daily key: %s)",
                    strike,
                    instrument_type.upper(),
                    target_date_str,
                )

            except Exception as exc:
                logger.exception(
                    "Error processing %s %s: %s",
                    strike,
                    instrument_type.upper(),
                    exc,
                )
                error_count += 1

    # Send high error alert if threshold exceeded
    if error_count > ERROR_THRESHOLD:
        send_high_error_alert(
            error_count=error_count,
            error_threshold=ERROR_THRESHOLD,
            processed_count=processed_count,
        )

        logger.warning("High error alert triggered.")
    else:
        logger.info("EMA analysis completed with %d errors.", error_count)

    logger.info(
        "EMA analysis task completed. Processed=%d Errors=%d",
        processed_count,
        error_count,
    )

    return processed_count, error_count


if __name__ == "__main__":
    try:
        TEST_RUN = settings.TEST_RUN
        TEST_STRIKES = ["23800", "23950"]
        INCLUDE_TODAY = False

        send_project_start_message()

        processed_count, error_count = run_daily_ema_analysis(
            test_run=TEST_RUN,
            test_strikes=TEST_STRIKES,
            include_today=INCLUDE_TODAY,
        )

        send_project_completed_message(
            processed_count=processed_count,
            error_count=error_count,
        )

    except Exception as exc:
        logger.exception("Critical error during EMA task: %s", exc)

        send_project_failed_message(
            error_message=str(exc),
        )

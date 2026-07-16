import pandas as pd

from app.core.logger import get_logger

logger = get_logger("ema_calculator")


def calculate_ema_cross(
    candles: list,
    short_window: int = 9,
    long_window: int = 21,
):
    """
    Calculate EMA crossover.

    Expected candle format:
    [
        [
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            oi
        ],
        ...
    ]

    Candles must be in chronological order
    (oldest -> newest).
    """

    # =====================================================
    # Validation
    # =====================================================

    if candles is None:
        logger.warning("Received None instead of candle data.")
        return None

    if not isinstance(candles, list):
        logger.error(
            "Expected list of candles, got %s",
            type(candles).__name__,
        )
        return None

    logger.info("Received %d candles for EMA calculation.", len(candles))

    if len(candles) < long_window:
        logger.warning(
            "Insufficient candles. Required=%d Received=%d",
            long_window,
            len(candles),
        )
        return None

    # =====================================================
    # DataFrame
    # =====================================================

    try:

        df = pd.DataFrame(
            candles,
            columns=[
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "oi",
            ],
        )

    except Exception as exc:

        logger.exception(
            "Failed to create DataFrame: %s",
            exc,
        )

        return None

    # =====================================================
    # Convert Types
    # =====================================================

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "oi",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    if df["close"].isna().all():
        logger.error("All close prices are invalid.")
        return None

    # =====================================================
    # EMA Calculation
    # =====================================================

    df["ema_short"] = (
        df["close"]
        .ewm(
            span=short_window,
            adjust=False,
        )
        .mean()
    )

    df["ema_long"] = (
        df["close"]
        .ewm(
            span=long_window,
            adjust=False,
        )
        .mean()
    )

    # =====================================================
    # Signal Detection
    # =====================================================

    df["signal"] = 0

    df.loc[
        df["ema_short"] > df["ema_long"],
        "signal",
    ] = 1

    df.loc[
        df["ema_short"] < df["ema_long"],
        "signal",
    ] = -1

    df["cross"] = df["signal"].diff()

    last_row = df.iloc[-1]

    signal_detected = None

    if last_row["cross"] == 2:
        signal_detected = "BULLISH_CROSS"

    elif last_row["cross"] == -2:
        signal_detected = "BEARISH_CROSS"

    # =====================================================
    # Logging
    # =====================================================

    logger.info(
        "Last Candle | Time=%s Close=%.2f EMA%d=%.2f EMA%d=%.2f Cross=%s Signal=%s",
        last_row["timestamp"],
        last_row["close"],
        short_window,
        last_row["ema_short"],
        long_window,
        last_row["ema_long"],
        last_row["cross"],
        signal_detected,
    )

    # =====================================================
    # Result
    # =====================================================

    return {
        "signal": signal_detected,
        "last_price": float(last_row["close"]),
        "ema_short": float(last_row["ema_short"]),
        "ema_long": float(last_row["ema_long"]),
        "timestamp": last_row["timestamp"],
        "total_candles": len(df),
    }

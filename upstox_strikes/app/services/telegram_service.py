import requests
from datetime import datetime
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger("telegram_service")

IST = ZoneInfo("Asia/Kolkata")


def is_telegram_enabled() -> bool:
    """
    Check whether Telegram notification is enabled.
    """
    return bool(getattr(settings, "TELE_FLAG", False))


def send_telegram_message(message: str) -> bool:
    """
    Send a Telegram message if TELE_FLAG is enabled.

    Returns:
        True  -> message sent successfully
        False -> message not sent or failed
    """

    if not is_telegram_enabled():
        logger.info("Telegram notification skipped because TELE_FLAG=False.")
        return False

    bot_token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        logger.warning(
            "Telegram notification enabled, but TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing."
        )
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(
            url,
            json=payload,
            timeout=10,
        )
        response.raise_for_status()

        logger.info("Telegram message sent successfully.")
        return True

    except requests.exceptions.RequestException as exc:
        logger.error("Failed to send Telegram message: %s", exc)
        return False

    except Exception as exc:
        logger.exception("Unexpected Telegram error: %s", exc)
        return False


def send_project_start_message() -> bool:
    """
    Send Telegram message when EMA analysis project starts.
    """

    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    message = (
        "🚀 <b>EMA Analysis Started</b>\n\n"
        f"Start Time: <b>{now_ist} IST</b>\n"
        "Status: Running\n"
        "Mode: Historical candle EMA processing"
    )

    return send_telegram_message(message)


def send_project_completed_message(
    processed_count: int = 0,
    error_count: int = 0,
) -> bool:
    """
    Send Telegram message when EMA analysis project completes.
    """

    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    message = (
        "✅ <b>EMA Analysis Completed</b>\n\n"
        f"End Time: <b>{now_ist} IST</b>\n"
        f"Processed Instruments: <b>{processed_count}</b>\n"
        f"Errors: <b>{error_count}</b>\n"
        "Status: Completed"
    )

    return send_telegram_message(message)


def send_project_failed_message(error_message: str) -> bool:
    """
    Send Telegram message when EMA analysis project fails critically.
    """

    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    message = (
        "❌ <b>EMA Analysis Failed</b>\n\n"
        f"Failure Time: <b>{now_ist} IST</b>\n"
        "Status: Critical Error\n\n"
        f"<b>Error:</b>\n{error_message}"
    )

    return send_telegram_message(message)


def send_high_error_alert(
    error_count: int,
    error_threshold: int,
    processed_count: int,
) -> bool:
    """
    Send Telegram alert when error count exceeds threshold.
    """

    now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    message = (
        "⚠️ <b>EMA Analysis Alert</b>\n\n"
        f"Alert Time: <b>{now_ist} IST</b>\n"
        f"Error Count: <b>{error_count}</b>\n"
        f"Threshold: <b>{error_threshold}</b>\n"
        f"Processed Instruments: <b>{processed_count}</b>\n\n"
        "Please investigate manually."
    )

    return send_telegram_message(message)
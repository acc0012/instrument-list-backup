import os
import json
import logging
from datetime import datetime, timezone

from dhanhq import dhanhq, DhanContext
from dhan_auth import load_valid_dhan_credentials
from mongo_clients import get_market_data_collection


# ------------------------------------
# CONFIG
# ------------------------------------
INDEX_SECURITY_IDS = [13, 51]
EXCHANGE_SEGMENT = "IDX_I"


# ------------------------------------
# LOGGING SETUP
# ------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ------------------------------------
# HELPER
# ------------------------------------
def parse_response(resp, label="response"):
    """
    Convert Dhan SDK response into dict safely.
    Also print logs for debugging.
    """

    logger.info(f"📦 {label} TYPE => {type(resp)}")

    # Print small sample only
    try:
        sample = str(resp)[:1000]
        logger.info(f"📦 {label} SAMPLE => {sample}")
    except Exception:
        logger.warning(f"⚠️ Failed printing {label} sample")

    # --------------------------------
    # STRING RESPONSE
    # --------------------------------
    if isinstance(resp, str):
        try:
            resp = json.loads(resp)

            logger.info(
                f"✅ Converted {label} string response to JSON dict"
            )

        except Exception:
            logger.exception(
                f"❌ Failed parsing {label} JSON string"
            )
            return {}

    # --------------------------------
    # INVALID RESPONSE
    # --------------------------------
    if not isinstance(resp, dict):
        logger.error(
            f"❌ {label} is not dict after parsing. "
            f"TYPE={type(resp)}"
        )
        return {}

    return resp


# ------------------------------------
# MAIN FUNCTION
# ------------------------------------
def fetch_and_store_option_chain():

    logger.info("🚀 Starting index option chain fetch job")

    # --------------------------------
    # LOAD DHAN CREDS
    # --------------------------------
    creds = load_valid_dhan_credentials()

    if not creds:
        logger.error("❌ No valid Dhan credentials found")
        raise RuntimeError("No valid Dhan credentials")

    # --------------------------------
    # INIT DHAN SDK
    # --------------------------------
    try:

        context = DhanContext(
            client_id=creds["client_id"],
            access_token=creds["access_token"]
        )

        dhan = dhanhq(context)

        logger.info("✅ Dhan SDK initialized successfully")

    except Exception:
        logger.exception("❌ Failed to initialize Dhan client")
        raise

    # --------------------------------
    # MONGO
    # --------------------------------
    collection = get_market_data_collection()

    # --------------------------------
    # LOOP INDEXES
    # --------------------------------
    for sec_id in INDEX_SECURITY_IDS:

        try:

            logger.info(
                f"📌 Processing index security_id={sec_id}"
            )

            # --------------------------------
            # FETCH EXPIRY LIST
            # --------------------------------
            expiry_resp = dhan.expiry_list(
                under_security_id=sec_id,
                under_exchange_segment=EXCHANGE_SEGMENT
            )

            expiry_resp = parse_response(
                expiry_resp,
                f"expiry_list_{sec_id}"
            )

            expiries = (
                expiry_resp
                .get("data", {})
                .get("data", [])
            )

            logger.info(
                f"📅 Expiries found count={len(expiries)}"
            )

            if not expiries:
                logger.warning(
                    f"⚠️ No expiries found for index {sec_id}"
                )
                continue

            # --------------------------------
            # SELECT FIRST EXPIRY
            # --------------------------------
            expiry = expiries[0]

            logger.info(
                f"📅 Selected expiry={expiry}"
            )

            # --------------------------------
            # FETCH OPTION CHAIN
            # --------------------------------
            oc = dhan.option_chain(
                under_security_id=sec_id,
                under_exchange_segment=EXCHANGE_SEGMENT,
                expiry=expiry
            )

            oc = parse_response(
                oc,
                f"option_chain_{sec_id}"
            )

            oc_data = (
                oc
                .get("data", {})
                .get("data", {})
            )

            logger.info(
                f"📊 option_chain keys count="
                f"{len(oc_data.keys()) if isinstance(oc_data, dict) else 0}"
            )

            if not oc_data:
                logger.warning(
                    f"⚠️ Empty option chain for index {sec_id}"
                )
                continue

            # --------------------------------
            # DELETE OLD SNAPSHOT
            # --------------------------------
            delete_result = collection.delete_many({
                "index_security_id": sec_id,
                "expiry": expiry
            })

            logger.info(
                f"🗑️ Deleted "
                f"{delete_result.deleted_count} "
                f"old records "
                f"(index={sec_id}, expiry={expiry})"
            )

            # --------------------------------
            # INSERT NEW SNAPSHOT
            # --------------------------------
            payload = {
                "index_security_id": sec_id,
                "expiry": expiry,
                "fetched_at": datetime.now(timezone.utc),
                "option_chain": oc_data
            }

            result = collection.insert_one(payload)

            logger.info(
                f"✅ Stored option chain snapshot "
                f"(index={sec_id}, expiry={expiry}) "
                f"_id={result.inserted_id}"
            )

        except Exception:
            logger.exception(
                f"❌ Failed processing index {sec_id}"
            )
            continue

    logger.info("✅ Index option chain fetch job completed")


# ------------------------------------
# ENTRY POINT
# ------------------------------------
if __name__ == "__main__":
    fetch_and_store_option_chain()
import os
import json
import logging
from datetime import datetime, timezone

from dhanhq import dhanhq
from dhan_folder.dhan_auth import load_valid_dhan_credentials
from dhan_folder.mongo_clients import get_market_data_collection


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

    # --------------------------------
    # PRINT SAMPLE RESPONSE
    # --------------------------------
    try:

        sample = str(resp)[:2000]

        logger.info(
            f"📦 {label} SAMPLE => {sample}"
        )

    except Exception:

        logger.warning(
            f"⚠️ Failed printing {label} sample"
        )

    # --------------------------------
    # HANDLE STRING RESPONSE
    # --------------------------------
    if isinstance(resp, str):

        try:

            resp = json.loads(resp)

            logger.info(
                f"✅ Converted {label} string -> dict"
            )

        except Exception:

            logger.exception(
                f"❌ Failed parsing {label} JSON string"
            )

            return {}

    # --------------------------------
    # VALIDATE TYPE
    # --------------------------------
    if not isinstance(resp, dict):

        logger.error(
            f"❌ {label} invalid type after parsing "
            f"=> {type(resp)}"
        )

        return {}

    return resp


# ------------------------------------
# MAIN FUNCTION
# ------------------------------------
def fetch_and_store_option_chain():

    logger.info(
        "🚀 Starting index option chain fetch job"
    )

    # --------------------------------
    # LOAD DHAN CREDS
    # --------------------------------
    creds = load_valid_dhan_credentials()

    if not creds:

        logger.error(
            "❌ No valid Dhan credentials found"
        )

        raise RuntimeError(
            "No valid Dhan credentials"
        )

    logger.info(
        f"✅ Loaded Dhan credentials "
        f"client_id={creds.get('client_id')}"
    )

    # --------------------------------
    # INIT DHAN SDK
    # --------------------------------
    try:

        dhan = dhanhq(
            creds["client_id"],
            creds["access_token"]
        )

        logger.info(
            "✅ Dhan SDK initialized successfully"
        )

    except Exception:

        logger.exception(
            "❌ Failed to initialize Dhan client"
        )

        raise

    # --------------------------------
    # MONGO COLLECTION
    # --------------------------------
    collection = get_market_data_collection()

    logger.info(
        "✅ Mongo collection initialized"
    )

    # --------------------------------
    # CLEAR OLD COLLECTION DATA
    # --------------------------------
    logger.info(
        "🗑️ Clearing full option chain collection"
    )

    delete_result = collection.delete_many({})

    logger.info(
        f"🗑️ Total deleted documents => "
        f"{delete_result.deleted_count}"
    )

    # --------------------------------
    # PROCESS EACH INDEX
    # --------------------------------
    for sec_id in INDEX_SECURITY_IDS:

        try:

            logger.info(
                f"📌 Processing index security_id={sec_id}"
            )

            # --------------------------------
            # FETCH EXPIRY LIST
            # --------------------------------
            logger.info(
                f"📡 Fetching expiry list for {sec_id}"
            )

            expiry_resp = dhan.expiry_list(
                under_security_id=sec_id,
                under_exchange_segment=EXCHANGE_SEGMENT
            )

            expiry_resp = parse_response(
                expiry_resp,
                f"expiry_list_{sec_id}"
            )

            logger.info(
                f"📦 expiry_resp final TYPE => "
                f"{type(expiry_resp)}"
            )

            # --------------------------------
            # EXTRACT EXPIRIES
            # --------------------------------
            expiries = (
                expiry_resp
                .get("data", {})
                .get("data", [])
            )

            logger.info(
                f"📅 Expiries count => "
                f"{len(expiries)}"
            )

            logger.info(
                f"📅 Expiries => {expiries}"
            )

            if not expiries:

                logger.warning(
                    f"⚠️ No expiries found for {sec_id}"
                )

                continue

            # --------------------------------
            # SELECT FIRST EXPIRY
            # --------------------------------
            expiry = expiries[0]

            logger.info(
                f"📅 Selected expiry => {expiry}"
            )

            # --------------------------------
            # FETCH OPTION CHAIN
            # --------------------------------
            logger.info(
                f"📡 Fetching option chain "
                f"for {sec_id}"
            )

            oc = dhan.option_chain(
                under_security_id=sec_id,
                under_exchange_segment=EXCHANGE_SEGMENT,
                expiry=expiry
            )

            oc = parse_response(
                oc,
                f"option_chain_{sec_id}"
            )

            logger.info(
                f"📦 option_chain final TYPE => "
                f"{type(oc)}"
            )

            # --------------------------------
            # EXTRACT OPTION CHAIN
            # --------------------------------
            oc_data = (
                oc
                .get("data", {})
                .get("data", {})
            )

            logger.info(
                f"📊 option_chain strikes count => "
                f"{len(oc_data.keys()) if isinstance(oc_data, dict) else 0}"
            )

            # --------------------------------
            # PRINT SAMPLE STRIKES
            # --------------------------------
            try:

                strike_keys = list(
                    oc_data.keys()
                )[:10]

                logger.info(
                    f"📊 Sample strikes => "
                    f"{strike_keys}"
                )

            except Exception:

                logger.warning(
                    "⚠️ Failed printing strike sample"
                )

            if not oc_data:

                logger.warning(
                    f"⚠️ Empty option chain for {sec_id}"
                )

                continue

            # --------------------------------
            # CREATE PAYLOAD
            # --------------------------------
            payload = {
                "index_security_id": sec_id,
                "expiry": expiry,
                "fetched_at": datetime.now(
                    timezone.utc
                ),
                "option_chain": oc_data
            }

            logger.info(
                "📦 Preparing Mongo payload"
            )

            # --------------------------------
            # INSERT SNAPSHOT
            # --------------------------------
            result = collection.insert_one(
                payload
            )

            logger.info(
                f"✅ Stored option chain snapshot "
                f"index={sec_id} "
                f"expiry={expiry} "
                f"_id={result.inserted_id}"
            )

        except Exception:

            logger.exception(
                f"❌ Failed processing index "
                f"{sec_id}"
            )

            continue

    logger.info(
        "✅ Index option chain fetch job completed"
    )


# ------------------------------------
# ENTRY POINT
# ------------------------------------
if __name__ == "__main__":
    fetch_and_store_option_chain()

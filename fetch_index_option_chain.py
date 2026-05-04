import os
import logging
from datetime import datetime, timezone
from dhanhq import dhanhq

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
# MAIN FUNCTION
# ------------------------------------
def fetch_and_store_option_chain():
    logger.info("🚀 Starting index option chain fetch job")

    creds = load_valid_dhan_credentials()
    if not creds:
        logger.error("❌ No valid Dhan credentials found")
        raise RuntimeError("No valid Dhan credentials")

    dhan = dhanhq(creds["client_id"], creds["access_token"])
    collection = get_market_data_collection()

    for sec_id in INDEX_SECURITY_IDS:
        logger.info(f"📌 Processing index security_id={sec_id}")

        # ----------------------------
        # Fetch expiry list
        # ----------------------------
        expiry_resp = dhan.expiry_list(
            under_security_id=sec_id,
            under_exchange_segment=EXCHANGE_SEGMENT
        )

        expiries = expiry_resp.get("data", {}).get("data", [])
        if not expiries:
            logger.warning(f"⚠️ No expiries found for index {sec_id}")
            continue

        expiry = expiries[0]  # ✅ This week expiry
        logger.info(f"📅 Selected expiry={expiry}")

        # ----------------------------
        # Fetch option chain
        # ----------------------------
        oc = dhan.option_chain(
            under_security_id=sec_id,
            under_exchange_segment=EXCHANGE_SEGMENT,
            expiry=expiry
        )

        oc_data = oc.get("data", {}).get("data", {})
        if not oc_data:
            logger.warning(f"⚠️ Empty option chain for index {sec_id}")
            continue

        # ----------------------------
        # DELETE previous snapshot
        # ----------------------------
        delete_result = collection.delete_many({
            "index_security_id": sec_id,
            "expiry": expiry
        })

        logger.info(
            f"🗑️ Deleted {delete_result.deleted_count} old records "
            f"(index={sec_id}, expiry={expiry})"
        )

        # ----------------------------
        # INSERT new snapshot
        # ----------------------------
        payload = {
            "index_security_id": sec_id,
            "expiry": expiry,
            "fetched_at": datetime.now(timezone.utc),
            "option_chain": oc_data
        }

        collection.insert_one(payload)

        logger.info(
            f"✅ Stored option chain snapshot "
            f"(index={sec_id}, expiry={expiry})"
        )

    logger.info("✅ Index option chain fetch job completed")


# ------------------------------------
# ENTRY POINT
# ------------------------------------
if __name__ == "__main__":
    fetch_and_store_option_chain()
    
    
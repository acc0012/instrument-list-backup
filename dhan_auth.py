from datetime import datetime, timezone
from auth_store import fetch_token_from_mongo


def load_valid_dhan_credentials():
    data = fetch_token_from_mongo()
    if not data:
        return None

    expiry_str = data["expiryTime"]

    # Normalize Z → +00:00
    if expiry_str.endswith("Z"):
        expiry_str = expiry_str.replace("Z", "+00:00")

    expiry = datetime.fromisoformat(expiry_str)

    # ✅ FORCE timezone awareness
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)

    # ✅ NOW SAFE
    if datetime.now(timezone.utc) >= expiry:
        return None

    return {
        "client_id": data["dhanClientId"],
        "access_token": data["accessToken"]
    }

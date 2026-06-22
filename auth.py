"""Token generation and validation for web client authentication."""
import hashlib
import hmac
import json
import time
import base64

import config


def generate_token(account_id: int, phone: str) -> str:
    """Generate a simple HMAC-based token."""
    payload = {
        "account_id": account_id,
        "phone": phone,
        "exp": int(time.time()) + config.JWT_EXPIRE_HOURS * 3600,
    }
    payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    sig = hmac.new(config.JWT_SECRET.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{sig}"


def validate_token(token: str) -> dict:
    """Validate token, return payload or None if invalid."""
    if not token or "." not in token:
        return None
    try:
        parts = token.split(".", 1)
        payload_b64 = parts[0]
        sig = parts[1]
        expected_sig = hmac.new(config.JWT_SECRET.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None

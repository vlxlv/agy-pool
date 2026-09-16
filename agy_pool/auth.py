"""
Authentication, OAuth credential decoding, JWT payload parsing,
and token refresh management for agy-pool.
"""

import os
import sys
import time
import json
import base64
import hashlib
import urllib.request
import urllib.parse
import urllib.error

from agy_pool import config
from agy_pool import storage


def _decode_cred(h, k=0x5A):
    return bytes([b ^ k for b in bytes.fromhex(h)]).decode()


_DEFAULT_CLIENT_ID = _decode_cred("6b6a6d6b6a6a6c6a6c6a6f636b772e3732292933346832686b3639283f68696f2c2e35363530326e3d6e6a693f2a743b2a2a29743d35353d363f2f293f283935342e3f342e74393537")
_DEFAULT_CLIENT_SECRET = _decode_cred("1d1519090a0277116f621c0d086e626c163e16106b371618622902196e206c2b1e1b3c")
CLIENT_ID = os.environ.get("AGY_CLIENT_ID", _DEFAULT_CLIENT_ID)
CLIENT_SECRET = os.environ.get("AGY_CLIENT_SECRET", _DEFAULT_CLIENT_SECRET)
OAUTH_SCOPES = (
    "openid email profile "
    "https://www.googleapis.com/auth/userinfo.email "
    "https://www.googleapis.com/auth/userinfo.profile "
    "https://www.googleapis.com/auth/cloud-platform "
    "https://www.googleapis.com/auth/cclog "
    "https://www.googleapis.com/auth/experimentsandconfigs "
    "https://www.googleapis.com/auth/aicode"
)

TOKEN_FIELDS = ("access_token", "refresh_token", "token_expiry", "updated_at", "id_token")
STATUS_FIELDS = ("status", "validation_url", "rate_limited_until")
REFRESH_PERSIST_FIELDS = TOKEN_FIELDS + STATUS_FIELDS + ("last_quota",)


def _persist_account_fields(account, fields):
    """Persist specified fields of account into the storage pool."""
    values = {key: account[key] for key in fields if key in account}
    removed = [key for key in fields if key not in account]

    def update(pool):
        stored = storage._find_account(pool, account)
        if stored:
            stored.update(values)
            for k in removed:
                stored.pop(k, None)

    storage.pool_transaction(update)


def decode_jwt_payload(jwt_str):
    """Decodes claims payload from a JWT string without verification."""
    try:
        parts = jwt_str.split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
            return json.loads(base64.urlsafe_b64decode(payload_b64.encode()))
    except Exception:
        pass
    return {}


def refresh_token(account):
    """Refreshes the access token using refresh_token if close to expiry."""
    now = time.time()
    exp = account.get("token_expiry", 0)
    # If valid for at least another 2 minutes, return existing access_token
    if account.get("access_token") and (exp - now > 120):
        return account["access_token"]

    lock_key = account.get("id") or account.get("email") or account.get("refresh_token", "")
    lock_name = hashlib.sha256(lock_key.encode()).hexdigest()[:24]
    with storage._file_lock(os.path.join(config.GEMINI_DIR, f"agy-pool-refresh-{lock_name}.lock")):
        # Another thread/process may have refreshed while this caller waited.
        stored = storage._find_account(storage.load_pool(), account)
        if stored:
            for key in TOKEN_FIELDS:
                if key in stored:
                    account[key] = stored[key]
        now = time.time()
        if account.get("access_token") and account.get("token_expiry", 0) - now > 120:
            return account["access_token"]

        rf = account.get("refresh_token")
        if not rf:
            raise ValueError("Missing refresh_token")

        params = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": rf,
            "grant_type": "refresh_token"
        }
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")

        with urllib.request.urlopen(req, timeout=10) as resp:
            res = json.loads(resp.read().decode())

        new_at = res.get("access_token")
        new_rf = res.get("refresh_token")
        expires_in = res.get("expires_in", 3600)
        if not new_at:
            raise ValueError("Token refresh failed: No access_token returned")
        account["access_token"] = new_at
        if new_rf:
            account["refresh_token"] = new_rf
        account["token_expiry"] = now + expires_in
        account["updated_at"] = int(now)
        _persist_account_fields(account, TOKEN_FIELDS)
        return new_at


def _is_validation_error(status, body):
    if status != 403:
        return False
    text = body.decode("utf-8", errors="ignore").lower() if isinstance(body, (bytes, bytearray)) else str(body).lower()
    return any(marker in text for marker in (
        "validation_required", "verify your account", "verify your account to continue"
    ))


def _extract_validation_url(body):
    try:
        raw = body.decode("utf-8", errors="ignore") if isinstance(body, (bytes, bytearray)) else str(body)
        data = json.loads(raw)
        for item in data.get("error", {}).get("details", []):
            meta = item.get("metadata", {})
            if "validation_url" in meta:
                return meta["validation_url"]
            for link in item.get("links", []):
                if "verify" in link.get("description", "").lower() or "verify" in link.get("url", "").lower():
                    return link.get("url")
    except Exception:
        pass
    return None


def _is_auth_error(status, body):
    if status == 401:
        return True
    if status == 403:
        text = body.decode("utf-8", errors="ignore").lower() if isinstance(body, (bytes, bytearray)) else str(body).lower()
        return any(marker in text for marker in (
            "unauthenticated", "invalid_grant", "access_token_expired",
            "account is disabled", "user disabled"
        ))
    return False

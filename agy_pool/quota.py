"""
Quota tracking, probing, formatting, freshness, and refresh management for agy-pool.
"""

import email.utils
import json
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from agy_pool import config, storage, auth

WINDOW_5H_SECS = 18000.0   # 5 hours
WINDOW_7D_SECS = 604800.0  # 7 days

QUOTA_FRESH_MAX_AGE = config.QUOTA_FRESH_MAX_AGE
QUOTA_AGING_MAX_AGE = config.QUOTA_AGING_MAX_AGE
_QUOTA_REFRESH_BACKOFF = config._QUOTA_REFRESH_BACKOFF

_QUOTA_REFRESH_LOCK = threading.Lock()
_QUOTA_REFRESH_IN_FLIGHT = set()
_QUOTA_REFRESH_RETRY = {}

BACKEND_HOST = "daily-cloudcode-pa.googleapis.com"
BACKEND_URL_BASE = f"https://{BACKEND_HOST}"
DEFAULT_UA = "antigravity/cli"

# Dynamic dependency hooks to support runtime mocking and entrypoint overrides
_token_refresher = None
_quota_prober = None
_safe_quota_fn = None
_backend_url_base_provider = None
_user_agent_provider = None


def set_token_refresher(refresher):
    global _token_refresher
    _token_refresher = refresher


def _get_token_refresher():
    return _token_refresher or auth.refresh_token


def set_quota_prober(prober):
    global _quota_prober
    _quota_prober = prober


def _get_quota_prober():
    return _quota_prober or query_quota


def set_safe_quota_fn(fn):
    global _safe_quota_fn
    _safe_quota_fn = fn


def _get_safe_quota_fn():
    return _safe_quota_fn or _safe_quota


def set_backend_url_provider(provider):
    global _backend_url_base_provider
    _backend_url_base_provider = provider


def _get_backend_url_base():
    if _backend_url_base_provider is not None:
        return _backend_url_base_provider()
    return BACKEND_URL_BASE


def set_user_agent_provider(provider):
    global _user_agent_provider
    _user_agent_provider = provider


def _get_user_agent():
    if _user_agent_provider is not None:
        return _user_agent_provider()
    return DEFAULT_UA


def _is_quota_error(status, body):
    if status == 429:
        return True
    if status != 403:
        return False
    text = body.decode("utf-8", errors="ignore").lower() if isinstance(body, (bytes, bytearray)) else str(body).lower()
    return any(marker in text for marker in (
        "resource_exhausted", "quota_exceeded", "rate_limit_exceeded",
        "quota exceeded", "quota exhausted", "exceeded your current quota",
        "insufficient quota"
    ))


def _parse_retry_after(headers, default=300):
    """
    Parses Retry-After header from an HTTP response, supporting:
    - Integer seconds (e.g., '120')
    - HTTP-date format (RFC 2822 / RFC 7231, e.g. 'Wed, 21 Oct 2026 07:28:00 GMT')
    Returns delay in seconds clamped between 5s and 86400s (24h).
    """
    if not headers:
        return default
    raw = None
    if hasattr(headers, "get"):
        raw = headers.get("Retry-After")
    if not raw:
        return default
    raw = str(raw).strip()
    if not raw:
        return default
    try:
        seconds = int(raw)
        return max(5, min(86400, seconds))
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        now = datetime.now(timezone.utc)
        seconds = int((dt - now).total_seconds())
        return max(5, min(86400, seconds))
    except Exception:
        pass
    return default


def _quota_fraction(value):
    """Return a bounded quota fraction, or None when the value is unknown/invalid."""
    try:
        value = float(value)
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return max(0.0, min(1.0, value))
    except (TypeError, ValueError):
        return None


def _cached_quota_state(last_quota):
    """Copy known cached windows without manufacturing unknown capacity."""
    old = last_quota if isinstance(last_quota, dict) else {}
    result = {}
    for key in ("gemini_5h", "gemini_weekly", "third_party_5h", "third_party_weekly"):
        window = old.get(key)
        fraction = _quota_fraction(window.get("fraction")) if isinstance(window, dict) else None
        if fraction is not None:
            result[key] = {"fraction": fraction, "reset_time": window.get("reset_time")}

    # Legacy pools stored only the combined fraction. Preserve their old usable
    # scheduling behavior when a later refresh supplies just one explicit window.
    if "gemini_5h" not in result and "gemini_weekly" not in result:
        legacy = _quota_fraction(old.get("remaining_fraction"))
        if legacy is not None:
            reset = old.get("reset_time")
            result["gemini_5h"] = {"fraction": legacy, "reset_time": reset}
            result["gemini_weekly"] = {"fraction": legacy, "reset_time": reset}
    return result


def _recompute_compat_quota(quota_data):
    """Derive legacy fields from known Gemini windows only."""
    known = []
    for key in ("gemini_5h", "gemini_weekly"):
        window = quota_data.get(key)
        fraction = _quota_fraction(window.get("fraction")) if isinstance(window, dict) else None
        if fraction is not None:
            window["fraction"] = fraction
            known.append((fraction, window.get("reset_time")))
    if known:
        fraction, reset = min(known, key=lambda item: item[0])
        quota_data["remaining_fraction"] = fraction
        quota_data["reset_time"] = reset
    else:
        quota_data.pop("remaining_fraction", None)
        quota_data.pop("reset_time", None)


def _fetch_available_models_quota(headers, quota_data):
    req = urllib.request.Request(
        f"{_get_backend_url_base()}/v1internal:fetchAvailableModels",
        data=b"{}",
        headers=headers,
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        res = json.loads(resp.read().decode())
    models = res.get("models", {})
    default_model = res.get("defaultAgentModelId")
    candidates = ([default_model] if default_model else []) + [
        "gemini-3.8-flash-high", "gemini-3.6-flash-high", "gemini-3.5-flash-medium"
    ]
    candidates += [name for name in models if name not in candidates]
    for name in candidates:
        model = models.get(name)
        qinfo = model.get("quotaInfo") if isinstance(model, dict) else None
        fraction = _quota_fraction(qinfo.get("remainingFraction")) if isinstance(qinfo, dict) else None
        if fraction is not None:
            quota_data["gemini_5h"] = {"fraction": fraction, "reset_time": qinfo.get("resetTime")}
            return
    raise ValueError("model response contained no quota information")


def query_quota(account):
    """Refresh cached quota, preserving known windows absent from a partial response."""
    refresher = _get_token_refresher()
    at = refresher(account)
    headers = {
        "Authorization": f"Bearer {at}",
        "Content-Type": "application/json",
        "User-Agent": _get_user_agent()
    }
    quota_data = _cached_quota_state(account.get("last_quota"))

    quota_error = None
    try:
        req = urllib.request.Request(
            f"{_get_backend_url_base()}/v1internal:retrieveUserQuotaSummary",
            data=b"{}",
            headers=headers,
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            res = json.loads(resp.read().decode())

        if account.get("status") in ("validation_required", "auth_error"):
            account.pop("status", None)
            account.pop("validation_url", None)
            account.pop("rate_limited_until", None)
            auth._persist_account_fields(account, ("status", "validation_url", "rate_limited_until"))

        found_quota = False
        for group in res.get("groups", []):
            for bucket in group.get("buckets", []):
                bid = bucket.get("bucketId", "")
                fraction = _quota_fraction(bucket.get("remainingFraction"))
                if fraction is None:
                    continue
                reset = bucket.get("resetTime")
                if "gemini" in bid:
                    if "5h" in bid or bucket.get("window") == "5h":
                        quota_data["gemini_5h"] = {"fraction": fraction, "reset_time": reset}
                        found_quota = True
                    elif "weekly" in bid or bucket.get("window") == "weekly":
                        quota_data["gemini_weekly"] = {"fraction": fraction, "reset_time": reset}
                        found_quota = True
                elif "3p" in bid:
                    if "5h" in bid or bucket.get("window") == "5h":
                        quota_data["third_party_5h"] = {"fraction": fraction, "reset_time": reset}
                    elif "weekly" in bid or bucket.get("window") == "weekly":
                        quota_data["third_party_weekly"] = {"fraction": fraction, "reset_time": reset}
        if not found_quota:
            raise ValueError("quota response contained no Gemini quota buckets")

    except urllib.error.HTTPError as e:
        quota_error = e
        try:
            err_body = e.read()
        except Exception:
            err_body = b""
        if auth._is_validation_error(e.code, err_body) or auth._is_auth_error(e.code, err_body):
            validation = auth._is_validation_error(e.code, err_body)
            status = "validation_required" if validation else "auth_error"
            delay = 86400 if validation else 3600
            v_url = auth._extract_validation_url(err_body) if validation else None
            account["status"] = status
            if v_url:
                account["validation_url"] = v_url
            account["rate_limited_until"] = time.time() + delay
            quota_data["gemini_5h"] = {"fraction": 0.0, "reset_time": None}
            quota_data["gemini_weekly"] = {"fraction": 0.0, "reset_time": None}
            _recompute_compat_quota(quota_data)
            quota_data["updated_at"] = int(time.time())
            account["last_quota"] = quota_data

            def update(pool):
                stored = storage._find_account(pool, account)
                if stored:
                    stored["status"] = status
                    if v_url:
                        stored["validation_url"] = v_url
                    stored["rate_limited_until"] = account["rate_limited_until"]
                    stored["error_count"] = stored.get("error_count", 0) + 1
                    stored["last_quota"] = quota_data
            storage.pool_transaction(update)
            return quota_data
        try:
            _fetch_available_models_quota(headers, quota_data)
        except Exception as fallback_error:
            raise fallback_error from quota_error

    except Exception as e:
        quota_error = e
        try:
            _fetch_available_models_quota(headers, quota_data)
        except Exception as fallback_error:
            raise fallback_error from quota_error

    _recompute_compat_quota(quota_data)
    quota_data["updated_at"] = int(time.time())
    account["last_quota"] = quota_data
    return quota_data


def _safe_quota(account):
    try:
        prober = _get_quota_prober()
        prober(account)
        auth._persist_account_fields(account, auth.REFRESH_PERSIST_FIELDS)
        return True
    except Exception:
        return False


def format_remaining_time(iso_reset_time):
    if not iso_reset_time:
        return "N/A"
    try:
        if isinstance(iso_reset_time, (int, float)):
            diff_sec = int(iso_reset_time - time.time())
        else:
            clean_time = str(iso_reset_time).replace("Z", "+00:00")
            target_dt = datetime.fromisoformat(clean_time)
            now_dt = datetime.now(timezone.utc)
            diff_sec = int((target_dt - now_dt).total_seconds())

        if diff_sec <= 0:
            return "Ready"
        days, rem = divmod(diff_sec, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, _ = divmod(rem, 60)
        if days > 0:
            return f"in {days}d {hours}h"
        if hours > 0:
            return f"in {hours}h {minutes}m"
        return f"in {minutes}m"
    except Exception:
        return str(iso_reset_time)[:16]


def render_progress_bar(fraction, width=10):
    if fraction is None:
        return f"{config.CLR_DIM}[{'░' * width}]   N/A{config.CLR_RESET}"
    fraction = max(0.0, min(1.0, float(fraction)))
    filled = int(round(fraction * width))
    bar = "█" * filled + "░" * (width - filled)
    pct = f"{fraction * 100:5.1f}%"
    if fraction > 0.4:
        color = config.CLR_GREEN
    elif fraction > 0.15:
        color = config.CLR_YELLOW
    else:
        color = config.CLR_RED
    return f"{color}[{bar}] {pct}{config.CLR_RESET}"


def _display_quota_fractions(quota):
    """Return explicit window values, with legacy combined quota as a fallback."""
    quota = quota if isinstance(quota, dict) else {}
    explicit = "gemini_5h" in quota or "gemini_weekly" in quota
    g5 = quota.get("gemini_5h")
    weekly = quota.get("gemini_weekly")
    q5 = _quota_fraction(g5.get("fraction")) if isinstance(g5, dict) else None
    q7 = _quota_fraction(weekly.get("fraction")) if isinstance(weekly, dict) else None
    if not explicit:
        legacy = _quota_fraction(quota.get("remaining_fraction"))
        q5 = q7 = legacy
    return q5, q7


def _format_account_quota_summary(q):
    g5 = q.get("gemini_5h", {})
    gw = q.get("gemini_weekly", {})
    g5_frac, gw_frac = _display_quota_fractions(q)
    print(f"  • Gemini 5h    : {render_progress_bar(g5_frac)} (Resets {format_remaining_time(g5.get('reset_time'))})")
    print(f"  • Gemini Weekly: {render_progress_bar(gw_frac)} (Resets {format_remaining_time(gw.get('reset_time'))})")


def _parse_iso_or_timestamp(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        val = val.strip()
        if not val or val.upper() in ("N/A", "NONE", "NULL", "READY"):
            return None
        try:
            return float(val)
        except ValueError:
            pass
        try:
            clean = val.replace("Z", "+00:00")
            return datetime.fromisoformat(clean).timestamp()
        except Exception:
            return None
    return None


def quota_freshness(account, now=None):
    """Return freshness class/rank/age for the persisted quota snapshot."""
    if now is None:
        now = time.time()
    quota = account.get("last_quota")
    updated = _parse_iso_or_timestamp(quota.get("updated_at")) if isinstance(quota, dict) else None
    if updated is None or updated <= 0:
        return {"class": "unknown", "rank": 0, "age": None}
    age = max(0.0, now - updated)
    reset_passed = any(
        (_parse_iso_or_timestamp(window.get("reset_time")) is not None and
         _parse_iso_or_timestamp(window.get("reset_time")) <= now)
        for window in (quota.get("gemini_5h"), quota.get("gemini_weekly"))
        if isinstance(window, dict)
    )
    if reset_passed or age > QUOTA_AGING_MAX_AGE:
        name, rank = "stale", 1
    elif age > QUOTA_FRESH_MAX_AGE:
        name, rank = "aging", 2
    else:
        name, rank = "fresh", 3
    return {"class": name, "rank": rank, "age": age}


def quota_refresh_needed(account, now=None):
    return quota_freshness(account, now=now)["class"] != "fresh"


def quota_freshness_rank(account, now=None):
    """Rank freshness for scheduling; fresh and aging are equally trustworthy."""
    return {"fresh": 2, "aging": 2, "stale": 1, "unknown": 0}[quota_freshness(account, now=now)["class"]]


def format_quota_age(account, now=None):
    state = quota_freshness(account, now=now)
    if state["age"] is None:
        return "unknown"
    age = int(state["age"])
    if state["class"] == "stale":
        return f"stale ({age // 60}m)"
    if age >= 60:
        return f"{age // 60}m{age % 60:02d}s"
    return f"{age}s"


def schedule_quota_refresh(account, now=None):
    """Start one bounded, non-blocking refresh per account."""
    key = account.get("id") or account.get("email")
    if not key:
        return False
    if now is None:
        now = time.time()
    with _QUOTA_REFRESH_LOCK:
        if key in _QUOTA_REFRESH_IN_FLIGHT:
            return False
        retry = _QUOTA_REFRESH_RETRY.get(key)
        if retry and retry["next_at"] > now:
            return False
        _QUOTA_REFRESH_IN_FLIGHT.add(key)

    def refresh():
        try:
            safe_fn = _get_safe_quota_fn()
            success = safe_fn(dict(account))
            with _QUOTA_REFRESH_LOCK:
                if success:
                    _QUOTA_REFRESH_RETRY.pop(key, None)
                else:
                    previous = _QUOTA_REFRESH_RETRY.get(key, {})
                    attempt = previous.get("attempt", 0)
                    delay = _QUOTA_REFRESH_BACKOFF[min(attempt, len(_QUOTA_REFRESH_BACKOFF) - 1)]
                    _QUOTA_REFRESH_RETRY[key] = {
                        "attempt": attempt + 1,
                        "next_at": time.time() + delay,
                    }
        except Exception:
            with _QUOTA_REFRESH_LOCK:
                previous = _QUOTA_REFRESH_RETRY.get(key, {})
                attempt = previous.get("attempt", 0)
                delay = _QUOTA_REFRESH_BACKOFF[min(attempt, len(_QUOTA_REFRESH_BACKOFF) - 1)]
                _QUOTA_REFRESH_RETRY[key] = {"attempt": attempt + 1, "next_at": time.time() + delay}
        finally:
            with _QUOTA_REFRESH_LOCK:
                _QUOTA_REFRESH_IN_FLIGHT.discard(key)

    threading.Thread(target=refresh, daemon=True).start()
    return True

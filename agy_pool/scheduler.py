"""
Scheduler and load balancing policy for agy-pool.

Implements candidate selection strategies (max_quota, least_used, round_robin)
and reset-aware capacity computation across 5-hour and weekly quota windows.
"""

import time

from agy_pool.storage import pool_transaction
from agy_pool.quota import (
    WINDOW_5H_SECS,
    WINDOW_7D_SECS,
    _quota_fraction,
    _parse_iso_or_timestamp,
    quota_freshness_rank,
)

VALID_STRATEGIES = ("max_quota", "least_used", "round_robin")


def compute_capacity_state(account, now=None):
    """Compute reset-aware capacity without treating missing fractions as full quota."""
    if now is None:
        now = time.time()
    quota = account.get("last_quota")
    quota = quota if isinstance(quota, dict) else {}
    g5 = quota.get("gemini_5h")
    weekly = quota.get("gemini_weekly")
    has_explicit_windows = "gemini_5h" in quota or "gemini_weekly" in quota

    q5 = _quota_fraction(g5.get("fraction")) if isinstance(g5, dict) else None
    q7 = _quota_fraction(weekly.get("fraction")) if isinstance(weekly, dict) else None
    q5_known = q5 is not None
    q7_known = q7 is not None
    legacy_fraction_known = False

    if not has_explicit_windows:
        legacy = _quota_fraction(quota.get("remaining_fraction"))
        if legacy is None:
            legacy = _quota_fraction(account.get("quota"))
        if legacy is not None:
            # A legacy combined floor remains schedulable, but is one source of
            # evidence rather than two independently measured windows.
            q5 = q7 = legacy
            legacy_fraction_known = True
        else:
            q5_pct = _quota_fraction(account.get("gemini_5h_pct") / 100.0) if account.get("gemini_5h_pct") is not None else None
            q7_pct = _quota_fraction(account.get("gemini_weekly_pct") / 100.0) if account.get("gemini_weekly_pct") is not None else None
            q5, q7 = q5_pct, q7_pct
            q5_known, q7_known = q5 is not None, q7 is not None

    reset5 = g5.get("reset_time") if isinstance(g5, dict) else None
    reset7 = weekly.get("reset_time") if isinstance(weekly, dict) else None
    if legacy_fraction_known:
        reset5 = reset7 = quota.get("reset_time")
    elif not has_explicit_windows:
        reset5 = account.get("gemini_5h_reset")
        reset7 = account.get("gemini_weekly_reset")

    def reset_ratio(seconds_key, reset, window_seconds):
        if account.get(seconds_key) is not None:
            try:
                remaining = float(account[seconds_key])
                return 1.0 if remaining <= 0 else min(1.0, remaining / window_seconds)
            except (TypeError, ValueError):
                return 1.0
        timestamp = _parse_iso_or_timestamp(reset)
        if timestamp is None:
            return 1.0
        remaining = timestamp - now
        return 1.0 if remaining <= 0 else min(1.0, remaining / window_seconds)

    r5 = reset_ratio("gemini_5h_reset_sec", reset5, WINDOW_5H_SECS)
    r7 = reset_ratio("gemini_weekly_reset_sec", reset7, WINDOW_7D_SECS)
    pace5 = q5 - r5 if q5 is not None else None
    pace7 = q7 - r7 if q7 is not None else None
    known_paces = [pace for pace in (pace5, pace7) if pace is not None]
    known_fractions = [fraction for fraction in (q5, q7) if fraction is not None]
    known_window_count = 1 if legacy_fraction_known else q5_known + q7_known
    worst_pace = min(known_paces) if known_paces else float("-inf")
    total_pace = sum(known_paces) if known_paces else float("-inf")
    raw_floor = min(known_fractions) if known_fractions else 0.0

    return {
        "q5": q5,
        "q7": q7,
        "r5": r5,
        "r7": r7,
        "pace5": pace5,
        "pace7": pace7,
        "worst_pace": worst_pace,
        "total_pace": total_pace,
        "raw_floor": raw_floor,
        "q5_known": q5_known,
        "q7_known": q7_known,
        "known_window_count": known_window_count,
        "legacy_fraction_known": legacy_fraction_known,
        "is_depleted": bool(known_fractions) and raw_floor <= 0.005,
    }


def _get_remaining_fraction(acc):
    q = acc.get("last_quota", {})
    raw_f = q.get("remaining_fraction")
    try:
        return float(raw_f) if raw_f is not None else 1.0
    except (ValueError, TypeError):
        return 1.0


def order_candidates(candidates, strategy="max_quota", pool=None, now=None):
    """
    Orders account candidates for AI generation according to the chosen load balancing strategy.

    Tiers:
      1. Eligible accounts: healthy capacity (raw floor > 0.005), not rate-limited, not restricted.
      2. Depleted accounts: raw quota floor <= 0.005, not rate-limited, not restricted.
      3. Cooldown accounts: rate_limited_until > now.
      4. Restricted accounts: validation_required or auth_error.

    Strategies applied strictly to the eligible account set:
      - max_quota: Prioritizes accounts with safest remaining 5-hour and weekly capacity (worst_pace,
        total_pace, raw_floor descending at full precision), breaking ties by lowest Hits.
      - least_used: Lowest generation count (Hits), breaking ties by reset-aware capacity pace,
        raw quota floor, then account ID.
      - round_robin: Sequential rotation starting after round_robin_last_account_id, wrapping around.

    Depleted, cooldown, and restricted tiers follow behind in stable fallback order.
    """
    if now is None:
        now = time.time()
    if pool is None:
        pool = {}

    eligible = []
    depleted = []
    cooldown = []
    restricted = []

    for acc in candidates:
        status = acc.get("status")
        if status in ("validation_required", "auth_error"):
            restricted.append(acc)
        elif acc.get("rate_limited_until", 0) > now:
            cooldown.append(acc)
        else:
            cap = compute_capacity_state(acc, now=now)
            if cap["is_depleted"]:
                depleted.append(acc)
            else:
                eligible.append(acc)

    # Strategy ordering on eligible accounts
    if strategy == "least_used":
        # Lowest hits first, tie-break by reset-aware capacity, then stable account ID
        def _least_used_key(a):
            cap = compute_capacity_state(a, now=now)
            hits = a.get("gen_count", a.get("request_count", 0))
            return (
                hits,
                -cap["known_window_count"],
                -quota_freshness_rank(a, now=now),
                -cap["worst_pace"],
                -cap["total_pace"],
                -cap["raw_floor"],
                a.get("id", "")
            )
        eligible.sort(key=_least_used_key)
    elif strategy == "round_robin":
        # Rotate starting after round_robin_last_account_id
        last_id = pool.get("round_robin_last_account_id")
        if last_id and any(a.get("id") == last_id for a in eligible):
            idx = next(i for i, a in enumerate(eligible) if a.get("id") == last_id)
            eligible = eligible[idx + 1:] + eligible[:idx + 1]
    else:
        # Default: max_quota (reset-aware capacity scheduling)
        def _max_quota_key(a):
            cap = compute_capacity_state(a, now=now)
            hits = a.get("gen_count", a.get("request_count", 0))
            return (
                cap["known_window_count"],
                quota_freshness_rank(a, now=now),
                cap["worst_pace"],
                cap["total_pace"],
                cap["raw_floor"],
                -hits
            )
        eligible.sort(key=_max_quota_key, reverse=True)

    # Secondary tiers ordering
    depleted.sort(key=lambda a: (
        compute_capacity_state(a, now=now)["raw_floor"],
        -a.get("gen_count", a.get("request_count", 0))
    ), reverse=True)

    return eligible + depleted + cooldown + restricted


def reserve_round_robin_candidates(now=None):
    """
    Atomically select candidate ordering and advance the persisted round_robin cursor
    under pool_transaction() before upstream network dispatch begins.

    Redefines the persisted cursor from 'last successfully completed account' to
    'last account reserved/selected for a new generation dispatch' to prevent
    concurrent generation handlers from reading an unadvanced cursor and selecting
    the same account.
    """
    if now is None:
        now = time.time()

    def update(pool):
        accounts = pool.get("accounts", [])
        if not accounts:
            return []
        candidates = order_candidates(accounts, strategy="round_robin", pool=pool, now=now)
        if candidates and candidates[0].get("id"):
            pool["round_robin_last_account_id"] = candidates[0]["id"]
        return [dict(a) for a in candidates]

    return pool_transaction(update)


def reserve_round_robin_account(now=None):
    """
    Atomically reserve and return the next round-robin account candidate.
    """
    candidates = reserve_round_robin_candidates(now=now)
    return candidates[0] if candidates else None


__all__ = [
    "VALID_STRATEGIES",
    "compute_capacity_state",
    "_get_remaining_fraction",
    "order_candidates",
    "reserve_round_robin_candidates",
    "reserve_round_robin_account",
]

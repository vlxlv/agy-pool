#!/usr/bin/env python3
"""
scripts/live_test.py: Test utility helper for agy-pool live integration tests.

Features:
- Strip ANSI escape sequences from terminal output.
- Parse `agy-pool list` / `quota` output into structured account records.
- Track log file byte offsets and extract log deltas from ~/.gemini/agy-pool.log.
- Analyze Hits deltas and apply GCD normalization.
- Validate scheduler dispatch behavior (round_robin, least_used, max_quota).
- Detect concurrent pool traffic.
- Provide JSON and human-readable CLI outputs.

Zero external dependencies: Python 3.8+ standard library only.
"""

import argparse
import json
import math
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

# Standard regex to strip ANSI escape codes (CSI, OSC, colors, movements)
ANSI_ESCAPE_RE = re.compile(
    r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])'
)

# Pattern matching account header in `agy-pool list`
# Example: [1] user@example.com (Display Name)  [* Active]  Hits: 5
ACCOUNT_HEADER_RE = re.compile(
    r'^\s*\[(\d+)\]\s+(\S+)(.*?)\s+\[([^\]]+)\]\s+Hits:\s+(\d+)\s*$'
)

QUOTA_5H_RE = re.compile(r'Gemini 5-Hour:\s+\[.*?\]\s+([\d\.]+)%')
QUOTA_WEEKLY_RE = re.compile(r'Gemini Weekly:\s+\[.*?\]\s+([\d\.]+)%')

# Patterns for agy-pool gateway proxy log lines
LOG_PROXY_RE = re.compile(
    r'^\[(?P<ts>[^\]]+)\]\s+\[PROXY\]\s+(?P<method>\S+)\s+(?P<endpoint>\S+)\s+->\s+(?P<account>\S+)\s+\(Status:\s+(?P<status>\d+)\)'
)
LOG_FAILOVER_RE = re.compile(
    r'^\[(?P<ts>[^\]]+)\]\s+\[FAILOVER\]\s+Account\s+(?P<account>\S+)\s+(?P<reason>.*)'
)
LOG_PROXY_ERROR_RE = re.compile(
    r'^\[(?P<ts>[^\]]+)\]\s+\[PROXY ERROR\]\s+(?P<method>\S+)\s+(?P<endpoint>\S+)\s+->\s+(?P<account>\S+)\s+HTTP\s+(?P<status>\d+)'
)
LOG_PROXY_EXC_RE = re.compile(
    r'^\[(?P<ts>[^\]]+)\]\s+\[PROXY EXCEPTION\]\s+(?P<method>\S+)\s+(?P<endpoint>\S+)\s+->\s+(?P<account>\S+):\s+(?P<error>.*)'
)

# Authoritative generation endpoint identifiers based on production SmartProxyHandler.handle_proxy()
# In production bin/agy-pool, candidate load-balancing is performed if and only if:
#     is_generation = ("generatecontent" in path.lower())
# In gateway proxy log lines, endpoints appear as path.split('/')[-1] (e.g.
# streamGenerateContent, generateContent, v1internal:streamGenerateContent, etc.,
# potentially followed by query parameters such as ?alt=sse).
GENERATION_ENDPOINTS = frozenset({
    "streamGenerateContent",
    "generateContent",
    "v1internal:streamGenerateContent",
    "v1internal:generateContent",
})


def is_generation_endpoint(endpoint: str) -> bool:
    """
    Check if an endpoint string from gateway logs corresponds to an authoritative generation request.
    Matches production SmartProxyHandler.handle_proxy() candidate selection logic:
    load balancing is performed when 'generatecontent' in path.lower().
    Strips query parameters (e.g. ?alt=sse) and handles colon/model prefixes.
    """
    if not endpoint:
        return False
    base = endpoint.split("?")[0]
    if base in GENERATION_ENDPOINTS:
        return True
    method = base.split(":")[-1]
    if method in GENERATION_ENDPOINTS:
        return True
    return "generatecontent" in base.lower()


def strip_ansi(text: str) -> str:
    """Remove all ANSI escape codes from string."""
    if not text:
        return ""
    return ANSI_ESCAPE_RE.sub("", text)


def parse_accounts(text: str) -> List[Dict[str, Any]]:
    """
    Parse output of `agy-pool list`, `agy-pool quota`, or pool JSON into structured accounts list.
    Handles spaces in display names, statuses, hits, and quota percentages.
    """
    if not text:
        return []

    # Check if text is raw JSON pool data
    stripped_first = text.strip()
    if stripped_first.startswith("{") and '"accounts"' in stripped_first:
        try:
            data = json.loads(stripped_first)
            if isinstance(data, dict) and isinstance(data.get("accounts"), list):
                result = []
                for i, acc in enumerate(data["accounts"], start=1):
                    q = acc.get("last_quota", {})
                    g5 = q.get("gemini_5h", {}).get("fraction", q.get("remaining_fraction", 1.0))
                    gw = q.get("gemini_weekly", {}).get("fraction", 1.0)
                    eff_q = round(min(g5, gw), 4) if (g5 is not None and gw is not None) else 1.0
                    status = acc.get("status") or "ready"
                    is_cooling = acc.get("rate_limited_until", 0) > 0
                    result.append({
                        "index": i,
                        "id": acc.get("id", f"acc_{i}"),
                        "email": acc.get("email", ""),
                        "name": acc.get("name"),
                        "active": (acc.get("id") == data.get("active_account_id")),
                        "status": status,
                        "is_cooling": is_cooling,
                        "is_eligible": (status in ("ready", "active") and not is_cooling and eff_q > 0.005),
                        "hits": acc.get("gen_count", acc.get("request_count", 0)),
                        "quota": eff_q,
                        "gemini_5h_pct": round(g5 * 100, 1) if g5 is not None else None,
                        "gemini_weekly_pct": round(gw * 100, 1) if gw is not None else None,
                    })
                return result
        except Exception:
            pass

    clean_text = strip_ansi(text)
    accounts = []
    current_acc: Optional[Dict[str, Any]] = None

    for raw_line in clean_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        header_match = ACCOUNT_HEADER_RE.match(line)
        if header_match:
            idx_str, email, name_part, marker, hits_str = header_match.groups()
            name_part = name_part.strip()
            display_name = None
            if name_part.startswith("(") and name_part.endswith(")"):
                display_name = name_part[1:-1].strip()

            marker = marker.strip()
            is_active = "* Active" in marker

            if "Verify Required" in marker:
                status = "validation_required"
            elif "Auth Error" in marker:
                status = "auth_error"
            elif "Cooldown" in marker:
                status = "cooldown"
            elif "Exhausted" in marker:
                status = "exhausted"
            elif "Ready" in marker:
                status = "ready"
            else:
                status = "active" if is_active else "ready"

            is_cooling = "Cooldown" in marker
            is_exhausted = "Exhausted" in marker
            is_eligible = (
                status in ("ready", "active")
                and not is_cooling
                and not is_exhausted
                and status not in ("validation_required", "auth_error")
            )

            current_acc = {
                "index": int(idx_str),
                "id": f"acc_{idx_str}",
                "email": email,
                "name": display_name,
                "active": is_active,
                "status": status,
                "is_cooling": is_cooling,
                "is_exhausted": is_exhausted,
                "is_eligible": is_eligible,
                "hits": int(hits_str),
                "gemini_5h_pct": None,
                "gemini_weekly_pct": None,
                "quota": 0.0 if (status in ("validation_required", "auth_error") or is_exhausted) else 1.0,
            }
            accounts.append(current_acc)
            continue

        if current_acc:
            m_5h = QUOTA_5H_RE.search(line)
            if m_5h:
                try:
                    pct = float(m_5h.group(1))
                    current_acc["gemini_5h_pct"] = pct
                    if current_acc["quota"] == 1.0:
                        current_acc["quota"] = round(pct / 100.0, 4)
                except ValueError:
                    pass

            m_w = QUOTA_WEEKLY_RE.search(line)
            if m_w:
                try:
                    pct = float(m_w.group(1))
                    current_acc["gemini_weekly_pct"] = pct
                    q5 = current_acc.get("gemini_5h_pct")
                    if q5 is not None:
                        current_acc["quota"] = round(min(q5, pct) / 100.0, 4)
                    else:
                        current_acc["quota"] = round(pct / 100.0, 4)
                except ValueError:
                    pass

            # Update eligibility if quota <= 0.005 (0.5%)
            if current_acc["quota"] <= 0.005:
                current_acc["is_eligible"] = False

    return accounts


def get_log_offset(log_path: str) -> int:
    """Return the current byte offset / size of the gateway log file."""
    if not os.path.exists(log_path):
        return 0
    try:
        return os.path.getsize(log_path)
    except OSError:
        return 0


def parse_log_delta(log_path: str, start_offset: int = 0) -> Dict[str, Any]:
    """
    Read newly appended lines from log_path starting at start_offset.
    Parses PROXY, FAILOVER, and ERROR lines, distinguishing:
      - all gateway proxy events
      - generation attempts (all requests targeting generation endpoints)
      - successful generation dispatches (successful 200 responses for generation endpoints)
      - failovers (gateway account switch events)
      - quota/rate-limit events (HTTP 429/403 or quota exhaustion failovers)
      - auxiliary gateway events (metadata, auth, config, session traffic)
    """
    if not os.path.exists(log_path):
        return {
            "new_offset": 0,
            "events": [],
            "gateway_proxy_events": [],
            "generation_attempts": [],
            "generation_dispatches": [],
            "successful_generation_dispatches": [],
            "dispatches": [],
            "failovers": [],
            "quota_events": [],
            "auxiliary_events": [],
            "total_events": 0,
            "total_proxy_events": 0,
            "generation_attempts_count": 0,
            "successful_dispatches_count": 0,
            "auxiliary_events_count": 0,
            "failovers_count": 0,
            "quota_events_count": 0,
            "truncated": False,
        }

    events = []
    gateway_proxy_events = []
    generation_attempts = []
    generation_dispatches = []
    failovers = []
    quota_events = []
    auxiliary_events = []
    new_offset = start_offset
    truncated = False

    try:
        file_size = os.path.getsize(log_path)
        if start_offset > file_size:
            truncated = True
            start_offset = 0

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(start_offset)
            for line in f:
                line_str = line.strip()
                if not line_str:
                    continue

                m_proxy = LOG_PROXY_RE.match(line_str)
                if m_proxy:
                    d = m_proxy.groupdict()
                    status_code = int(d["status"])
                    is_gen = is_generation_endpoint(d["endpoint"])
                    ev = {
                        "type": "proxy",
                        "timestamp": d["ts"],
                        "method": d["method"],
                        "endpoint": d["endpoint"],
                        "account": d["account"],
                        "status": status_code,
                        "is_generation": is_gen,
                    }
                    events.append(ev)
                    gateway_proxy_events.append(ev)
                    if is_gen:
                        generation_attempts.append(ev)
                        if status_code == 200:
                            generation_dispatches.append(d["account"])
                        elif status_code in (429, 403):
                            quota_events.append(ev)
                    else:
                        auxiliary_events.append(ev)
                    continue

                m_fail = LOG_FAILOVER_RE.match(line_str)
                if m_fail:
                    d = m_fail.groupdict()
                    reason = d["reason"].strip()
                    ev = {
                        "type": "failover",
                        "timestamp": d["ts"],
                        "account": d["account"],
                        "reason": reason,
                    }
                    events.append(ev)
                    failovers.append(ev)
                    reason_lower = reason.lower()
                    if "rate limit" in reason_lower or "quota" in reason_lower or "429" in reason_lower:
                        quota_events.append(ev)
                    continue

                m_err = LOG_PROXY_ERROR_RE.match(line_str)
                if m_err:
                    d = m_err.groupdict()
                    status_code = int(d["status"])
                    is_gen = is_generation_endpoint(d["endpoint"])
                    ev = {
                        "type": "proxy_error",
                        "timestamp": d["ts"],
                        "method": d["method"],
                        "endpoint": d["endpoint"],
                        "account": d["account"],
                        "status": status_code,
                        "is_generation": is_gen,
                    }
                    events.append(ev)
                    gateway_proxy_events.append(ev)
                    if is_gen:
                        generation_attempts.append(ev)
                        if status_code in (429, 403):
                            quota_events.append(ev)
                    else:
                        auxiliary_events.append(ev)
                    continue

                m_exc = LOG_PROXY_EXC_RE.match(line_str)
                if m_exc:
                    d = m_exc.groupdict()
                    is_gen = is_generation_endpoint(d["endpoint"])
                    ev = {
                        "type": "proxy_exception",
                        "timestamp": d["ts"],
                        "method": d["method"],
                        "endpoint": d["endpoint"],
                        "account": d["account"],
                        "error": d["error"].strip(),
                        "is_generation": is_gen,
                    }
                    events.append(ev)
                    gateway_proxy_events.append(ev)
                    if is_gen:
                        generation_attempts.append(ev)
                    else:
                        auxiliary_events.append(ev)
                    continue

            new_offset = f.tell()
    except OSError as e:
        sys.stderr.write(f"[live_test] Error reading log file {log_path}: {e}\n")

    return {
        "new_offset": new_offset,
        "events": events,
        "gateway_proxy_events": gateway_proxy_events,
        "generation_attempts": generation_attempts,
        "generation_dispatches": generation_dispatches,
        "successful_generation_dispatches": generation_dispatches,
        "dispatches": generation_dispatches,
        "failovers": failovers,
        "quota_events": quota_events,
        "auxiliary_events": auxiliary_events,
        "total_events": len(events),
        "total_proxy_events": len(gateway_proxy_events),
        "generation_attempts_count": len(generation_attempts),
        "successful_dispatches_count": len(generation_dispatches),
        "auxiliary_events_count": len(auxiliary_events),
        "failovers_count": len(failovers),
        "quota_events_count": len(quota_events),
        "truncated": truncated,
    }


def analyze_hits_delta(
    before_accounts: List[Dict[str, Any]],
    after_accounts: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Compare Hits between before and after account snapshots.
    Calculates per-account delta, total delta, GCD of non-zero deltas,
    and normalized delta units.
    """
    before_map = {a.get("email") or a.get("id"): a.get("hits", 0) for a in before_accounts}
    after_map = {a.get("email") or a.get("id"): a.get("hits", 0) for a in after_accounts}

    deltas = {}
    details = []
    all_keys = []
    for a in before_accounts:
        k = a.get("email") or a.get("id")
        if k and k not in all_keys:
            all_keys.append(k)
    for a in after_accounts:
        k = a.get("email") or a.get("id")
        if k and k not in all_keys:
            all_keys.append(k)

    for key in all_keys:
        b_hits = before_map.get(key, 0)
        a_hits = after_map.get(key, 0)
        d = max(0, a_hits - b_hits)
        deltas[key] = d
        details.append({
            "account": key,
            "before": b_hits,
            "after": a_hits,
            "delta": d,
        })

    total_delta = sum(deltas.values())
    positive_deltas = [d for d in deltas.values() if d > 0]

    if positive_deltas:
        gcd_val = positive_deltas[0]
        for val in positive_deltas[1:]:
            gcd_val = math.gcd(gcd_val, val)
    else:
        gcd_val = 1

    normalized = {k: (v // gcd_val if gcd_val > 0 else v) for k, v in deltas.items()}

    return {
        "deltas": deltas,
        "total_delta": total_delta,
        "positive_counts": len(positive_deltas),
        "gcd": gcd_val,
        "normalized": normalized,
        "zero_delta": (total_delta == 0),
        "per_account": details,
    }


def verify_round_robin(
    dispatches: List[str],
    accounts: List[Dict[str, Any]],
    initial_last_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Verify that dispatches followed round_robin sequential rotation across eligible accounts.
    """
    if not dispatches:
        return {"passed": False, "reason": "No dispatches recorded in log delta"}

    eligible = [a for a in accounts if a.get("is_eligible", True)]
    if not eligible:
        return {"passed": False, "reason": "No eligible accounts available for round_robin rotation"}

    eligible_emails = [a.get("email") or a.get("id") for a in eligible]
    n_eligible = len(eligible_emails)

    # If only 1 eligible account, all dispatches must hit that account
    if n_eligible == 1:
        expected = eligible_emails[0]
        for idx, d in enumerate(dispatches):
            if d != expected:
                return {
                    "passed": False,
                    "reason": f"Step {idx}: expected single account '{expected}', got '{d}'",
                    "step": idx,
                }
        return {
            "passed": True,
            "details": f"Single account '{expected}' received all {len(dispatches)} dispatch(es)",
            "dispatches": dispatches,
        }

    # Verify every dispatch is an eligible account
    for idx, d in enumerate(dispatches):
        if d not in eligible_emails:
            return {
                "passed": False,
                "reason": f"Step {idx}: dispatched account '{d}' is not in eligible accounts {eligible_emails}",
                "step": idx,
            }

    # If initial_last_id is known, check that first dispatch starts immediately after it.
    # If initial_last_id is stale/not in eligible, production falls back to eligible[0].
    start_pos = eligible_emails.index(dispatches[0])
    if initial_last_id:
        found = False
        for i, a in enumerate(eligible):
            if a.get("id") == initial_last_id or a.get("email") == initial_last_id:
                found = True
                expected_start_pos = (i + 1) % n_eligible
                if start_pos != expected_start_pos:
                    return {
                        "passed": False,
                        "reason": f"Initial step: expected rotation after '{initial_last_id}' -> '{eligible_emails[expected_start_pos]}', got '{dispatches[0]}'",
                        "step": 0,
                    }
                break
        if not found:
            # Stale or removed cursor: production starts at unrotated eligible[0]
            if start_pos != 0:
                return {
                    "passed": False,
                    "reason": f"Initial step: stale cursor '{initial_last_id}' expected fallback to '{eligible_emails[0]}', got '{dispatches[0]}'",
                    "step": 0,
                }

    # Check cyclic rotation for each consecutive step
    cur_pos = start_pos
    for idx in range(1, len(dispatches)):
        expected_pos = (cur_pos + 1) % n_eligible
        actual_pos = eligible_emails.index(dispatches[idx])
        if actual_pos != expected_pos:
            return {
                "passed": False,
                "reason": (
                    f"Step {idx}: rotation broken. Expected '{eligible_emails[expected_pos]}', "
                    f"got '{dispatches[idx]}'"
                ),
                "step": idx,
                "expected": eligible_emails[expected_pos],
                "actual": dispatches[idx],
            }
        cur_pos = actual_pos

    return {
        "passed": True,
        "details": f"Verified cyclic round_robin sequence of {len(dispatches)} dispatch(es) across {n_eligible} accounts: {dispatches}",
        "dispatches": dispatches,
    }


def verify_least_used(
    dispatches: List[str],
    accounts: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Verify that dispatches followed least_used strategy (lowest hits first).
    Simulates step-by-step dispatch and checks that chosen account had minimum hits among eligible candidates.
    """
    if not dispatches:
        return {"passed": False, "reason": "No dispatches recorded in log delta"}

    eligible = [dict(a) for a in accounts if a.get("is_eligible", True)]
    if not eligible:
        return {"passed": False, "reason": "No eligible accounts available for least_used"}

    # Track simulated hits
    sim_hits = {a.get("email") or a.get("id"): a.get("hits", 0) for a in eligible}

    for idx, d in enumerate(dispatches):
        if d not in sim_hits:
            return {
                "passed": False,
                "reason": f"Step {idx}: dispatched account '{d}' is not in eligible candidates",
                "step": idx,
            }

        min_hits_val = min(sim_hits.values())
        actual_hits = sim_hits[d]

        # Chosen account must have had minimum hits (or tied for minimum)
        if actual_hits > min_hits_val:
            min_candidates = [k for k, v in sim_hits.items() if v == min_hits_val]
            return {
                "passed": False,
                "reason": (
                    f"Step {idx}: account '{d}' had {actual_hits} hits, but lower-hit candidate(s) "
                    f"{min_candidates} with {min_hits_val} hits were available"
                ),
                "step": idx,
                "dispatched": d,
                "hits_at_dispatch": actual_hits,
                "min_available_hits": min_hits_val,
            }

        # Check production tie-breaking among candidates with equal min hits:
        # Sort by higher remaining quota, then stable account ID
        min_accounts = [
            a for a in eligible if sim_hits[a.get("email") or a.get("id")] == min_hits_val
        ]
        min_accounts.sort(
            key=lambda a: (-round(a.get("quota", 1.0), 2), a.get("id", ""))
        )
        expected_top = min_accounts[0].get("email") or min_accounts[0].get("id")

        if d != expected_top and len(min_accounts) > 1:
            expected_q = round(min_accounts[0].get("quota", 1.0), 2)
            actual_acc = next((a for a in min_accounts if (a.get("email") == d or a.get("id") == d)), None)
            actual_q = round(actual_acc.get("quota", 1.0), 2) if actual_acc else 0.0
            if actual_q < expected_q:
                return {
                    "passed": False,
                    "reason": (
                        f"Step {idx}: tie-breaker failed. Both had {min_hits_val} hits, but '{expected_top}' "
                        f"had higher quota ({expected_q}) than '{d}' ({actual_q})"
                    ),
                    "step": idx,
                    "expected": expected_top,
                    "actual": d,
                }
            elif actual_acc and (actual_acc.get("id", "") != min_accounts[0].get("id", "")) and actual_q == expected_q:
                return {
                    "passed": False,
                    "reason": (
                        f"Step {idx}: tie-breaker failed. Equal hits ({min_hits_val}) and equal quota ({actual_q}), "
                        f"expected ID '{min_accounts[0].get('id')}' ('{expected_top}'), got '{actual_acc.get('id')}' ('{d}')"
                    ),
                    "step": idx,
                    "expected": expected_top,
                    "actual": d,
                }

        # Increment simulated hits for next step
        sim_hits[d] += 1

    return {
        "passed": True,
        "details": f"Verified least_used dispatch sequence of {len(dispatches)} request(s): {dispatches}",
        "dispatches": dispatches,
    }


def verify_max_quota(
    dispatches: List[str],
    accounts: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Verify that dispatches followed max_quota strategy (highest remaining quota first).
    """
    if not dispatches:
        return {"passed": False, "reason": "No dispatches recorded in log delta"}

    eligible = [a for a in accounts if a.get("is_eligible", True)]
    if not eligible:
        return {"passed": False, "reason": "No eligible accounts available for max_quota"}

    # Sort eligible by quota desc, hits asc
    sorted_eligible = sorted(
        eligible,
        key=lambda a: (round(a.get("quota", 1.0), 2), -a.get("hits", 0)),
        reverse=True
    )
    top_account = sorted_eligible[0].get("email") or sorted_eligible[0].get("id")

    # In max_quota without cooldown/failover, dispatches should go to top candidate
    for idx, d in enumerate(dispatches):
        if d != top_account:
            return {
                "passed": False,
                "reason": (
                    f"Step {idx}: expected highest-quota candidate '{top_account}' "
                    f"(quota: {sorted_eligible[0].get('quota')}), but got '{d}'"
                ),
                "step": idx,
                "expected": top_account,
                "actual": d,
            }

    return {
        "passed": True,
        "details": f"Verified max_quota dispatch of {len(dispatches)} request(s) to top candidate '{top_account}'",
        "dispatches": dispatches,
    }


def detect_concurrent_activity(
    expected_runs: int,
    dispatches: List[str],
    hits_delta: Dict[str, Any],
    tolerance_ratio: float = 2.0,
    failovers: Optional[List[Any]] = None,
    external_detected: bool = False,
) -> Dict[str, Any]:
    """
    Detect whether external concurrent generation requests were processed by the pool during the test.
    Auxiliary requests (metadata, config, session, auth, quota) are excluded from this check.

    Classifications:
    - CLEAN: Observed generation activity is fully consistent with the requested live test and no unexplained
      generation traffic exists.
    - INTERNAL_MULTIDISPATCH: One or more outer test invocations legitimately produce multiple generation
      dispatches consistent with internal agy behavior (e.g. 1 outer run -> 2 generation dispatches).
      Does not claim external concurrency.
    - INCONCLUSIVE: Extra generation activity exists beyond nominal multi-dispatch slack, but cannot prove
      external traffic vs internal agy behavior / retries / failovers.
    - CONCURRENT: Strong evidence indicates unrelated external generation traffic (explicit external signal or
      severe volume far exceeding any internal multi-dispatch).
    """
    observed_dispatches = len(dispatches)
    total_delta = hits_delta.get("total_delta", 0)
    gcd_val = hits_delta.get("gcd", 1) or 1
    failover_count = len(failovers) if isinstance(failovers, list) else int(failovers or 0)

    # Hard threshold beyond which external traffic is confirmed by volume
    hard_max_dispatches = max(expected_runs + 2 + failover_count,
                              int(math.ceil(expected_runs * tolerance_ratio + failover_count)))
    hard_max_hits = max((expected_runs + 2 + failover_count) * gcd_val,
                         int(math.ceil((expected_runs * tolerance_ratio + failover_count) * gcd_val)))

    if external_detected:
        status = "CONCURRENT"
        concurrent_detected = True
        is_inconclusive = False
        message = "Confirmed concurrent external traffic: independent external generation traffic identified"
    elif observed_dispatches > hard_max_dispatches or total_delta > hard_max_hits:
        status = "CONCURRENT"
        concurrent_detected = True
        is_inconclusive = False
        message = (
            f"Confirmed concurrent external traffic: observed {observed_dispatches} generation dispatches "
            f"(expected <= {hard_max_dispatches}) or +{total_delta} Hits (expected <= {hard_max_hits})"
        )
    elif expected_runs == 1 and observed_dispatches == 2 + failover_count:
        # 1 outer invocation producing 2 generation dispatches is consistent with internal agy multi-dispatch
        status = "INTERNAL_MULTIDISPATCH"
        concurrent_detected = False
        is_inconclusive = True
        message = (
            f"1 outer agy invocation produced {observed_dispatches} generation dispatches. "
            f"This is consistent with internal multi-generation behavior. "
            f"External concurrent traffic is not proven."
        )
    else:
        # Baseline check for clean single- or multi-run executions
        # For expected_runs == 1: exactly 1 generation dispatch expected (plus failovers)
        # For expected_runs > 1: allow nominal slack of +1 for potential sub-dispatch
        minor_slack_dispatches = expected_runs + failover_count if expected_runs <= 1 else (expected_runs + 1 + failover_count)
        minor_slack_hits = minor_slack_dispatches * gcd_val

        if observed_dispatches > minor_slack_dispatches or total_delta > minor_slack_hits:
            status = "INCONCLUSIVE"
            concurrent_detected = False
            is_inconclusive = True
            message = (
                f"Ambiguous generation traffic: observed {observed_dispatches} "
                f"generation dispatches for {expected_runs} expected run(s). "
                f"The additional generation may be internal to agy. "
                f"External traffic is not proven."
            )
        else:
            status = "CLEAN"
            concurrent_detected = False
            is_inconclusive = False
            message = "No concurrent external traffic detected"

    return {
        "concurrent_detected": concurrent_detected,
        "is_inconclusive": is_inconclusive,
        "status": status,
        "expected_runs": expected_runs,
        "observed_dispatches": observed_dispatches,
        "total_hits_delta": total_delta,
        "failover_count": failover_count,
        "message": message,
    }


def resolve_gateway_port(env_val: Optional[str] = None) -> int:
    """Resolve and validate gateway port from env_val or AGY_PORT environment variable."""
    raw = os.environ.get("AGY_PORT") if env_val is None else env_val
    if raw is None or raw == "":
        return 8899
    try:
        port = int(raw)
        if 1 <= port <= 65535:
            return port
    except (ValueError, TypeError):
        pass
    raise ValueError(f"Invalid AGY_PORT '{raw}'. Must be an integer between 1 and 65535.")


def _read_input(path_or_str: Optional[str]) -> str:
    """Read content from file path, direct string, or stdin."""
    if not path_or_str or path_or_str == "-":
        return sys.stdin.read()
    if os.path.exists(path_or_str):
        with open(path_or_str, "r", encoding="utf-8", errors="replace") as f:
            return f.read()
    return path_or_str


def main():
    parser = argparse.ArgumentParser(
        prog="live_test.py",
        description="Helper utility for agy-pool live integration tests"
    )
    subparsers = parser.add_subparsers(dest="action")

    # get-port
    subparsers.add_parser("get-port", help="Resolve and print gateway port")

    # strip-ansi
    sa_p = subparsers.add_parser("strip-ansi", help="Strip ANSI color/escape codes")
    sa_p.add_argument("input", nargs="?", default="-", help="Input file or '-' for stdin")

    # parse-accounts
    pa_p = subparsers.add_parser("parse-accounts", help="Parse agy-pool list text into accounts JSON")
    pa_p.add_argument("input", nargs="?", default="-", help="Input file or '-' for stdin")

    # log-offset
    lo_p = subparsers.add_parser("log-offset", help="Get current log file byte size")
    lo_p.add_argument("file", help="Path to log file")

    # parse-log-delta
    pld_p = subparsers.add_parser("parse-log-delta", help="Extract log delta from byte offset")
    pld_p.add_argument("file", help="Path to log file")
    pld_p.add_argument("offset", type=int, default=0, help="Starting byte offset")

    # hits-delta
    hd_p = subparsers.add_parser("hits-delta", help="Analyze hits delta between account snapshots")
    hd_p.add_argument("--before", required=True, help="Before accounts JSON file or string")
    hd_p.add_argument("--after", required=True, help="After accounts JSON file or string")

    # verify-scheduler
    vs_p = subparsers.add_parser("verify-scheduler", help="Verify scheduler dispatch behavior")
    vs_p.add_argument("--strategy", required=True, choices=["round_robin", "least_used", "max_quota"], help="Strategy name")
    vs_p.add_argument("--dispatches", required=True, help="JSON list of dispatched accounts or file path")
    vs_p.add_argument("--accounts", required=True, help="Accounts JSON list or file path")
    vs_p.add_argument("--initial-last-id", default=None, help="Initial round_robin_last_account_id if known")

    # detect-concurrent
    dc_p = subparsers.add_parser("detect-concurrent", help="Check for concurrent traffic")
    dc_p.add_argument("--expected", type=int, required=True, help="Expected number of runs")
    dc_p.add_argument("--dispatches", required=True, help="JSON list of dispatched accounts or file")
    dc_p.add_argument("--hits-delta", required=True, help="Hits delta JSON or file")
    dc_p.add_argument("--failovers", default=None, help="Failovers JSON list or file (optional)")
    dc_p.add_argument("--external-detected", action="store_true", default=False, help="Flag if external traffic is independently confirmed")

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        sys.exit(0)

    if args.action == "get-port":
        print(resolve_gateway_port())

    elif args.action == "strip-ansi":
        content = _read_input(args.input)
        sys.stdout.write(strip_ansi(content))

    elif args.action == "parse-accounts":
        content = _read_input(args.input)
        accounts = parse_accounts(content)
        print(json.dumps(accounts, indent=2, ensure_ascii=False))

    elif args.action == "log-offset":
        print(get_log_offset(args.file))

    elif args.action == "parse-log-delta":
        res = parse_log_delta(args.file, args.offset)
        print(json.dumps(res, indent=2, ensure_ascii=False))

    elif args.action == "hits-delta":
        b_content = _read_input(args.before)
        a_content = _read_input(args.after)
        b_acc = json.loads(b_content) if isinstance(b_content, str) else b_content
        a_acc = json.loads(a_content) if isinstance(a_content, str) else a_content
        res = analyze_hits_delta(b_acc, a_acc)
        print(json.dumps(res, indent=2, ensure_ascii=False))

    elif args.action == "verify-scheduler":
        d_content = _read_input(args.dispatches)
        a_content = _read_input(args.accounts)
        dispatches = json.loads(d_content) if isinstance(d_content, str) else d_content
        accounts = json.loads(a_content) if isinstance(a_content, str) else a_content

        if args.strategy == "round_robin":
            res = verify_round_robin(dispatches, accounts, initial_last_id=args.initial_last_id)
        elif args.strategy == "least_used":
            res = verify_least_used(dispatches, accounts)
        elif args.strategy == "max_quota":
            res = verify_max_quota(dispatches, accounts)
        else:
            res = {"passed": False, "reason": f"Unknown strategy {args.strategy}"}

        print(json.dumps(res, indent=2, ensure_ascii=False))
        if not res.get("passed"):
            sys.exit(1)

    elif args.action == "detect-concurrent":
        d_content = _read_input(args.dispatches)
        h_content = _read_input(args.hits_delta)
        dispatches = json.loads(d_content) if isinstance(d_content, str) else d_content
        hits_delta = json.loads(h_content) if isinstance(h_content, str) else h_content
        failovers = None
        if getattr(args, "failovers", None):
            f_content = _read_input(args.failovers)
            try:
                failovers = json.loads(f_content) if isinstance(f_content, str) else f_content
            except Exception:
                failovers = None

        external_detected = bool(getattr(args, "external_detected", False))
        res = detect_concurrent_activity(
            args.expected, dispatches, hits_delta,
            failovers=failovers, external_detected=external_detected
        )
        print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

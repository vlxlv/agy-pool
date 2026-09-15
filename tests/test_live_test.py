#!/usr/bin/env python3
"""
Unit tests for the live integration test helper (scripts/live_test.py).

These tests run completely offline, without network, without live credentials,
and complete in sub-second time.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

# Ensure scripts directory is importable
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scripts.live_test import (
    GENERATION_ENDPOINTS,
    analyze_hits_delta,
    detect_concurrent_activity,
    get_log_offset,
    is_generation_endpoint,
    parse_accounts,
    parse_log_delta,
    resolve_gateway_port,
    strip_ansi,
    verify_least_used,
    verify_max_quota,
    verify_round_robin,
)


class LiveTestHelperTest(unittest.TestCase):

    def test_strip_ansi(self):
        self.assertEqual(strip_ansi(""), "")
        self.assertEqual(strip_ansi("hello world"), "hello world")
        colored = "\033[1m\033[32m[1] user@example.com\033[0m  [\033[36mReady\033[0m]  Hits: 5"
        self.assertEqual(strip_ansi(colored), "[1] user@example.com  [Ready]  Hits: 5")
        cursor = "\033[2K\rDone!"
        self.assertEqual(strip_ansi(cursor), "\rDone!")

    def test_parse_accounts_cli_output_single_with_spaces_in_name(self):
        sample = """
Refreshing quota for 1 account(s)...

====================================================================
           Antigravity Multi-Account Pool v0.1.0-alpha.9            
====================================================================
[1] admin@corp.net (Enterprise Cloud Workspace Admin)  [* Active]  Hits: 42
    • Gemini 5-Hour: [██████████] 100.0%  (Resets in 4h 30m)
    • Gemini Weekly: [████████░░]  80.0%  (Resets in 5d 12h)
====================================================================
"""
        accounts = parse_accounts(sample)
        self.assertEqual(len(accounts), 1)
        acc = accounts[0]
        self.assertEqual(acc["index"], 1)
        self.assertEqual(acc["email"], "admin@corp.net")
        self.assertEqual(acc["name"], "Enterprise Cloud Workspace Admin")
        self.assertTrue(acc["active"])
        self.assertTrue(acc["is_eligible"])
        self.assertEqual(acc["hits"], 42)
        self.assertEqual(acc["gemini_5h_pct"], 100.0)
        self.assertEqual(acc["gemini_weekly_pct"], 80.0)
        self.assertEqual(acc["quota"], 0.8)

    def test_parse_accounts_cli_output_multiple_statuses(self):
        sample = """
[1] active@test.com  [* Active]  Hits: 10
    • Gemini 5-Hour: [██████████] 100.0%  (Resets in 3h)
    • Gemini Weekly: [██████████] 100.0%  (Resets in 6d)
[2] ready@test.com (Backup Account)  [Ready]  Hits: 5
    • Gemini 5-Hour: [█████░░░░░]  50.0%  (Resets in 1h)
    • Gemini Weekly: [██████████] 100.0%  (Resets in 4d)
[3] cool@test.com  [* Active (Cooldown)]  Hits: 2
    • Gemini 5-Hour: [██████████] 100.0%  (Resets in 2h)
    • Gemini Weekly: [██████████] 100.0%  (Resets in 3d)
[4] exhausted@test.com  [Exhausted]  Hits: 8
    • Gemini 5-Hour: [░░░░░░░░░░]   0.0%  (Resets in 10m)
    • Gemini Weekly: [██████████] 100.0%  (Resets in 2d)
[5] verify@test.com  [⚠ Verify Required]  Hits: 0
    • Gemini 5-Hour: [░░░░░░░░░░]  Action Required (Blocked)
    • Gemini Weekly: [░░░░░░░░░░]  Action Required (Blocked)
[6] autherr@test.com  [✖ Auth Error]  Hits: 1
    • Gemini 5-Hour: [░░░░░░░░░░]  Authentication Failure
    • Gemini Weekly: [░░░░░░░░░░]  Authentication Failure
"""
        accounts = parse_accounts(sample)
        self.assertEqual(len(accounts), 6)

        # 1: Active
        self.assertTrue(accounts[0]["active"])
        self.assertTrue(accounts[0]["is_eligible"])
        self.assertEqual(accounts[0]["quota"], 1.0)

        # 2: Ready with name
        self.assertFalse(accounts[1]["active"])
        self.assertTrue(accounts[1]["is_eligible"])
        self.assertEqual(accounts[1]["name"], "Backup Account")
        self.assertEqual(accounts[1]["quota"], 0.5)

        # 3: Cooldown
        self.assertTrue(accounts[2]["is_cooling"])
        self.assertFalse(accounts[2]["is_eligible"])

        # 4: Exhausted
        self.assertTrue(accounts[3]["is_exhausted"])
        self.assertFalse(accounts[3]["is_eligible"])

        # 5: Verify Required
        self.assertEqual(accounts[4]["status"], "validation_required")
        self.assertFalse(accounts[4]["is_eligible"])
        self.assertEqual(accounts[4]["quota"], 0.0)

        # 6: Auth Error
        self.assertEqual(accounts[5]["status"], "auth_error")
        self.assertFalse(accounts[5]["is_eligible"])
        self.assertEqual(accounts[5]["quota"], 0.0)

    def test_parse_accounts_empty_pool(self):
        self.assertEqual(parse_accounts(""), [])
        self.assertEqual(parse_accounts("No accounts in pool yet.\nRun agy-pool login to add."), [])

    def test_parse_accounts_from_pool_json(self):
        data = {
            "version": 1,
            "strategy": "least_used",
            "active_account_id": "acc_1",
            "accounts": [
                {
                    "id": "acc_1",
                    "email": "acc1@test.com",
                    "name": "First",
                    "gen_count": 7,
                    "last_quota": {
                        "gemini_5h": {"fraction": 0.9},
                        "gemini_weekly": {"fraction": 1.0}
                    }
                },
                {
                    "id": "acc_2",
                    "email": "acc2@test.com",
                    "gen_count": 3,
                    "status": "validation_required",
                    "last_quota": {}
                }
            ]
        }
        accounts = parse_accounts(json.dumps(data))
        self.assertEqual(len(accounts), 2)
        self.assertTrue(accounts[0]["active"])
        self.assertTrue(accounts[0]["is_eligible"])
        self.assertEqual(accounts[0]["quota"], 0.9)
        self.assertEqual(accounts[0]["hits"], 7)

        self.assertFalse(accounts[1]["active"])
        self.assertFalse(accounts[1]["is_eligible"])
        self.assertEqual(accounts[1]["status"], "validation_required")

    def test_log_delta_parsing(self):
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as f:
            log_path = f.name
            f.write("[2026-09-15 10:00:00] [PROXY] POST streamGenerateContent -> a@test.com (Status: 200)\n")
            f.flush()
            mid_offset = f.tell()
            f.write("[2026-09-15 10:01:00] [PROXY ERROR] POST streamGenerateContent -> a@test.com HTTP 429\n")
            f.write("[2026-09-15 10:01:00] [FAILOVER] Account a@test.com hit rate limit! Cooldown 60s\n")
            f.write("[2026-09-15 10:01:01] [PROXY] POST streamGenerateContent -> b@test.com (Status: 200)\n")
            f.flush()

        try:
            # Full read from 0
            full = parse_log_delta(log_path, 0)
            self.assertEqual(len(full["events"]), 4)
            self.assertEqual(full["dispatches"], ["a@test.com", "b@test.com"])
            self.assertEqual(len(full["failovers"]), 1)
            self.assertEqual(full["failovers"][0]["account"], "a@test.com")

            # Delta read from mid_offset
            delta = parse_log_delta(log_path, mid_offset)
            self.assertEqual(len(delta["events"]), 3)
            self.assertEqual(delta["dispatches"], ["b@test.com"])
            self.assertEqual(len(delta["failovers"]), 1)
            self.assertGreater(delta["new_offset"], mid_offset)
        finally:
            os.unlink(log_path)

    def test_analyze_hits_delta(self):
        before = [
            {"email": "a@test.com", "hits": 10},
            {"email": "b@test.com", "hits": 20},
        ]
        after = [
            {"email": "a@test.com", "hits": 12},
            {"email": "b@test.com", "hits": 24},
        ]
        res = analyze_hits_delta(before, after)
        self.assertEqual(res["deltas"], {"a@test.com": 2, "b@test.com": 4})
        self.assertEqual(res["total_delta"], 6)
        self.assertEqual(res["gcd"], 2)
        self.assertEqual(res["normalized"], {"a@test.com": 1, "b@test.com": 2})
        self.assertFalse(res["zero_delta"])

        # Zero delta case
        res_zero = analyze_hits_delta(before, before)
        self.assertTrue(res_zero["zero_delta"])
        self.assertEqual(res_zero["total_delta"], 0)

    def test_verify_round_robin_success(self):
        accounts = [
            {"email": "a@test.com", "is_eligible": True},
            {"email": "b@test.com", "is_eligible": True},
            {"email": "c@test.com", "is_eligible": True},
        ]

        # Standard rotation starting at A
        dispatches = ["a@test.com", "b@test.com", "c@test.com", "a@test.com", "b@test.com"]
        res = verify_round_robin(dispatches, accounts)
        self.assertTrue(res["passed"])

        # Rotation starting in middle (B)
        dispatches_mid = ["b@test.com", "c@test.com", "a@test.com", "b@test.com"]
        res_mid = verify_round_robin(dispatches_mid, accounts)
        self.assertTrue(res_mid["passed"])

        # Single eligible account
        single_acc = [{"email": "solo@test.com", "is_eligible": True}]
        res_solo = verify_round_robin(["solo@test.com", "solo@test.com"], single_acc)
        self.assertTrue(res_solo["passed"])

    def test_verify_round_robin_failures(self):
        accounts = [
            {"email": "a@test.com", "is_eligible": True},
            {"email": "b@test.com", "is_eligible": True},
            {"email": "c@test.com", "is_eligible": True},
        ]

        # Out-of-order sequence (skips B: A -> C)
        dispatches = ["a@test.com", "c@test.com", "b@test.com"]
        res = verify_round_robin(dispatches, accounts)
        self.assertFalse(res["passed"])
        self.assertIn("rotation broken", res["reason"])

        # Foreign account in dispatches
        res_unknown = verify_round_robin(["a@test.com", "intruder@test.com"], accounts)
        self.assertFalse(res_unknown["passed"])
        self.assertIn("not in eligible accounts", res_unknown["reason"])

        # Empty dispatches
        res_empty = verify_round_robin([], accounts)
        self.assertFalse(res_empty["passed"])

    def test_verify_least_used(self):
        accounts = [
            {"email": "a@test.com", "hits": 2, "is_eligible": True},
            {"email": "b@test.com", "hits": 0, "is_eligible": True},
        ]
        # b has 0 hits, must receive first 2 requests before tied with a
        valid_dispatches = ["b@test.com", "b@test.com", "a@test.com"]
        res = verify_least_used(valid_dispatches, accounts)
        self.assertTrue(res["passed"])

        # Invalid: a dispatched when b had lower hits
        invalid_dispatches = ["a@test.com", "b@test.com"]
        res_invalid = verify_least_used(invalid_dispatches, accounts)
        self.assertFalse(res_invalid["passed"])
        self.assertIn("lower-hit candidate", res_invalid["reason"])

    def test_verify_max_quota(self):
        accounts = [
            {"email": "high@test.com", "quota": 0.95, "hits": 0, "is_eligible": True},
            {"email": "low@test.com", "quota": 0.30, "hits": 0, "is_eligible": True},
        ]
        valid = ["high@test.com", "high@test.com"]
        res = verify_max_quota(valid, accounts)
        self.assertTrue(res["passed"])

        invalid = ["low@test.com"]
        res_invalid = verify_max_quota(invalid, accounts)
        self.assertFalse(res_invalid["passed"])
        self.assertIn("expected highest-quota candidate", res_invalid["reason"])

    def test_detect_concurrent_activity(self):
        hits_delta = {"total_delta": 3, "gcd": 1}
        dispatches = ["a@test.com", "b@test.com", "c@test.com"]

        # Exactly 3 runs expected, 3 observed -> clean
        res = detect_concurrent_activity(3, dispatches, hits_delta)
        self.assertFalse(res["concurrent_detected"])
        self.assertFalse(res["is_inconclusive"])
        self.assertEqual(res["status"], "CLEAN")

        # 1 run expected, 10 observed -> concurrent detected
        res_conc = detect_concurrent_activity(1, ["a@test.com"] * 10, {"total_delta": 10, "gcd": 1})
        self.assertTrue(res_conc["concurrent_detected"])
        self.assertEqual(res_conc["status"], "CONCURRENT")

    def test_detect_concurrent_activity_multi_dispatch_within_invocation(self):
        # 1. 9 outer invocations resulting in 10 legitimate internal dispatches -> must NOT be a false positive
        dispatches_10 = ["a@test.com"] * 10
        hits_delta_10 = {"total_delta": 10, "gcd": 1}
        res_10 = detect_concurrent_activity(9, dispatches_10, hits_delta_10)
        self.assertFalse(res_10["concurrent_detected"], "9 outer invocations with 10 dispatches must not be flagged concurrent")
        self.assertFalse(res_10["is_inconclusive"], "9 outer invocations with 10 dispatches is within nominal slack")
        self.assertEqual(res_10["status"], "CLEAN")

        # 2. Intermediate excess traffic (9 outer runs -> 14 dispatches) -> must WARN / evaluate to INCONCLUSIVE, not silently PASS
        dispatches_14 = ["a@test.com"] * 14
        hits_delta_14 = {"total_delta": 14, "gcd": 1}
        res_14 = detect_concurrent_activity(9, dispatches_14, hits_delta_14)
        self.assertFalse(res_14["concurrent_detected"], "intermediate traffic is not confirmed external concurrent traffic")
        self.assertTrue(res_14["is_inconclusive"], "intermediate excess traffic must be flagged as INCONCLUSIVE")
        self.assertEqual(res_14["status"], "INCONCLUSIVE")

        # 3. 9 outer invocations with 25 dispatches -> must be detected as CONCURRENT
        dispatches_25 = ["a@test.com"] * 25
        hits_delta_25 = {"total_delta": 25, "gcd": 1}
        res_25 = detect_concurrent_activity(9, dispatches_25, hits_delta_25)
        self.assertTrue(res_25["concurrent_detected"], "9 outer invocations with 25 dispatches must be detected as concurrent")
        self.assertEqual(res_25["status"], "CONCURRENT")

    def test_parse_log_delta_after_rotation_or_truncation(self):
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as f:
            log_path = f.name
            # Write large initial log
            f.write("[2026-09-15 09:00:00] [PROXY] POST streamGenerateContent -> old@test.com (Status: 200)\n" * 10)
            f.flush()
            old_offset = f.tell()

        try:
            # Simulate log rotation / truncation: write a much smaller fresh log
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("[2026-09-15 10:00:00] [PROXY] POST streamGenerateContent -> fresh@test.com (Status: 200)\n")

            # Offset from previous large file exceeds new file size
            delta = parse_log_delta(log_path, start_offset=old_offset)
            self.assertTrue(delta.get("truncated"))
            self.assertEqual(delta["dispatches"], ["fresh@test.com"])
            self.assertEqual(len(delta["events"]), 1)
        finally:
            os.unlink(log_path)

    def test_verify_max_quota_rounding_tie_break_hits(self):
        accounts = [
            {"email": "a@test.com", "quota": 0.854, "hits": 10, "is_eligible": True},
            {"email": "b@test.com", "quota": 0.851, "hits": 5, "is_eligible": True},
        ]
        # Both round to 0.85; b has fewer hits (5 < 10), so b is chosen
        res = verify_max_quota(["b@test.com"], accounts)
        self.assertTrue(res["passed"])

        res_fail = verify_max_quota(["a@test.com"], accounts)
        self.assertFalse(res_fail["passed"])
        self.assertIn("expected highest-quota candidate 'b@test.com'", res_fail["reason"])

    def test_verify_max_quota_rounding_and_hits_tie_break_order(self):
        accounts = [
            {"id": "acc_1", "email": "a@test.com", "quota": 0.854, "hits": 5, "is_eligible": True},
            {"id": "acc_2", "email": "b@test.com", "quota": 0.851, "hits": 5, "is_eligible": True},
        ]
        # Both round to 0.85 and have 5 hits; stable input order tie-breaks to acc_1 (a@test.com)
        res = verify_max_quota(["a@test.com"], accounts)
        self.assertTrue(res["passed"])

        res_fail = verify_max_quota(["b@test.com"], accounts)
        self.assertFalse(res_fail["passed"])

    def test_verify_max_quota_excludes_cooldown_and_restricted(self):
        accounts = [
            {"email": "cooling@test.com", "quota": 1.0, "is_cooling": True, "is_eligible": False},
            {"email": "restricted@test.com", "quota": 1.0, "status": "validation_required", "is_eligible": False},
            {"email": "healthy@test.com", "quota": 0.40, "hits": 0, "is_eligible": True},
        ]
        # High quota cooling/restricted must be excluded; only healthy eligible account can be chosen
        res = verify_max_quota(["healthy@test.com"], accounts)
        self.assertTrue(res["passed"])

        res_fail = verify_max_quota(["cooling@test.com"], accounts)
        self.assertFalse(res_fail["passed"])

    def test_verify_least_used_quota_and_id_tie_break(self):
        accounts = [
            {"id": "acc_1", "email": "a@test.com", "hits": 0, "quota": 0.40, "is_eligible": True},
            {"id": "acc_2", "email": "b@test.com", "hits": 0, "quota": 0.80, "is_eligible": True},
            {"id": "acc_3", "email": "c@test.com", "hits": 0, "quota": 0.80, "is_eligible": True},
        ]
        # Equal hits (0): higher quota tie-breaks between b (0.80) and a (0.40) -> b chosen
        res_b = verify_least_used(["b@test.com"], accounts)
        self.assertTrue(res_b["passed"])

        res_a_fail = verify_least_used(["a@test.com"], accounts)
        self.assertFalse(res_a_fail["passed"])
        self.assertIn("had higher quota", res_a_fail["reason"])

        # Equal hits (0) and equal quota (0.80): stable ID tie-breaks acc_2 before acc_3
        res_c_fail = verify_least_used(["c@test.com"], accounts)
        self.assertFalse(res_c_fail["passed"])
        self.assertIn("expected ID 'acc_2'", res_c_fail["reason"])

    def test_verify_least_used_multi_dispatch_per_invocation(self):
        accounts = [
            {"id": "acc_1", "email": "a@test.com", "hits": 0, "quota": 0.50, "is_eligible": True},
            {"id": "acc_2", "email": "b@test.com", "hits": 3, "quota": 0.50, "is_eligible": True},
        ]
        # Simulates multiple internal dispatches to a@test.com before it reaches b's hits
        dispatches = ["a@test.com", "a@test.com", "a@test.com", "a@test.com"]
        res = verify_least_used(dispatches, accounts)
        self.assertTrue(res["passed"])

    def test_verify_round_robin_stale_or_removed_cursor(self):
        accounts = [
            {"id": "acc_1", "email": "a@test.com", "is_eligible": True},
            {"id": "acc_2", "email": "b@test.com", "is_eligible": True},
        ]
        # When cursor refers to a removed account, production falls back to eligible[0] (a@test.com)
        res = verify_round_robin(["a@test.com", "b@test.com"], accounts, initial_last_id="acc_deleted")
        self.assertTrue(res["passed"])

        res_fail = verify_round_robin(["b@test.com", "a@test.com"], accounts, initial_last_id="acc_deleted")
        self.assertFalse(res_fail["passed"])
        self.assertIn("stale cursor", res_fail["reason"])

    def test_verify_round_robin_excludes_cooldown_accounts(self):
        accounts = [
            {"email": "a@test.com", "is_eligible": True},
            {"email": "b@test.com", "is_cooling": True, "is_eligible": False},
            {"email": "c@test.com", "is_eligible": True},
        ]
        # Rotation cycles only across eligible accounts [a, c]
        res = verify_round_robin(["a@test.com", "c@test.com", "a@test.com"], accounts)
        self.assertTrue(res["passed"])

        res_fail = verify_round_robin(["a@test.com", "b@test.com"], accounts)
        self.assertFalse(res_fail["passed"])
        self.assertIn("not in eligible accounts", res_fail["reason"])

    def test_resolve_gateway_port(self):
        # Default
        self.assertEqual(resolve_gateway_port(None), 8899)
        self.assertEqual(resolve_gateway_port(""), 8899)
        # Custom
        self.assertEqual(resolve_gateway_port("9000"), 9000)
        self.assertEqual(resolve_gateway_port("1"), 1)
        self.assertEqual(resolve_gateway_port("65535"), 65535)
        # Errors
        with self.assertRaises(ValueError):
            resolve_gateway_port("0")
        with self.assertRaises(ValueError):
            resolve_gateway_port("70000")
        with self.assertRaises(ValueError):
            resolve_gateway_port("invalid")


    def test_authoritative_generation_endpoints(self):
        # Exact authoritative endpoints defined from production handle_proxy()
        self.assertIn("streamGenerateContent", GENERATION_ENDPOINTS)
        self.assertIn("generateContent", GENERATION_ENDPOINTS)
        self.assertIn("v1internal:streamGenerateContent", GENERATION_ENDPOINTS)
        self.assertIn("v1internal:generateContent", GENERATION_ENDPOINTS)

        # True for generation endpoints in various real-world logging forms
        self.assertTrue(is_generation_endpoint("streamGenerateContent"))
        self.assertTrue(is_generation_endpoint("generateContent"))
        self.assertTrue(is_generation_endpoint("v1internal:streamGenerateContent"))
        self.assertTrue(is_generation_endpoint("v1internal:streamGenerateContent?alt=sse"))
        self.assertTrue(is_generation_endpoint("v1internal:generateContent"))
        self.assertTrue(is_generation_endpoint("v1internal:generateContent?alt=sse"))
        self.assertTrue(is_generation_endpoint("models/gemini-1.5-pro:streamGenerateContent"))
        self.assertTrue(is_generation_endpoint("models/gemini-1.5-pro:generateContent"))

        # False for auxiliary gateway traffic
        self.assertFalse(is_generation_endpoint("v1internal:loadCodeAssist"))
        self.assertFalse(is_generation_endpoint("v1internal:fetchUserInfo"))
        self.assertFalse(is_generation_endpoint("v1internal:listExperiments"))
        self.assertFalse(is_generation_endpoint("v1internal:writeTrajectoryAcls"))
        self.assertFalse(is_generation_endpoint("v1internal:retrieveUserQuotaSummary"))
        self.assertFalse(is_generation_endpoint("v1internal:fetchAdminControls"))
        self.assertFalse(is_generation_endpoint("v1internal:fetchAvailableModels"))
        self.assertFalse(is_generation_endpoint("v1internal:recordTrajectoryAnalytics"))
        self.assertFalse(is_generation_endpoint(""))
        self.assertFalse(is_generation_endpoint(None))

    def test_case_a_single_generation_with_auxiliary_traffic(self):
        """
        Regression Case A:
        1 outer live generation
        15 auxiliary gateway requests
        1 successful streamGenerateContent
        Expected:
        generation dispatch count = 1
        not concurrent
        """
        aux_endpoints = [
            "v1internal:loadCodeAssist",
            "v1internal:loadCodeAssist",
            "v1internal:fetchUserInfo",
            "v1internal:listExperiments",
            "v1internal:listExperiments",
            "v1internal:writeTrajectoryAcls",
            "v1internal:retrieveUserQuotaSummary",
            "v1internal:loadCodeAssist",
            "v1internal:recordTrajectoryAnalytics",
            "v1internal:fetchAdminControls",
            "v1internal:fetchAvailableModels",
            "v1internal:loadCodeAssist",
            "v1internal:retrieveUserQuotaSummary",
            "v1internal:fetchUserInfo",
            "v1internal:writeTrajectoryAcls",
        ]
        self.assertEqual(len(aux_endpoints), 15)

        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as f:
            log_path = f.name
            for ep in aux_endpoints:
                f.write(f"[2026-09-15 13:44:34] [PROXY] POST {ep} -> zhangy0623@gmail.com (Status: 200)\n")
            f.write("[2026-09-15 13:44:35] [PROXY] POST v1internal:streamGenerateContent?alt=sse -> solaris18990@gmail.com (Status: 200)\n")
            f.flush()

        try:
            delta = parse_log_delta(log_path, 0)
            self.assertEqual(delta["total_proxy_events"], 16)
            self.assertEqual(delta["auxiliary_events_count"], 15)
            self.assertEqual(delta["generation_attempts_count"], 1)
            self.assertEqual(delta["successful_dispatches_count"], 1)
            self.assertEqual(delta["generation_dispatches"], ["solaris18990@gmail.com"])
            self.assertEqual(delta["dispatches"], ["solaris18990@gmail.com"])
            self.assertEqual(len(delta["failovers"]), 0)

            # Concurrency check must ignore auxiliary traffic and evaluate to CLEAN
            hits_delta = {"total_delta": 1, "gcd": 1}
            conc = detect_concurrent_activity(1, delta["generation_dispatches"], hits_delta)
            self.assertEqual(conc["status"], "CLEAN")
            self.assertFalse(conc["concurrent_detected"])
            self.assertFalse(conc["is_inconclusive"])

            # Verify scheduler uses the single generation dispatch
            accounts = [{"email": "solaris18990@gmail.com", "quota": 1.0, "is_eligible": True}]
            res = verify_max_quota(delta["generation_dispatches"], accounts)
            self.assertTrue(res["passed"])
        finally:
            os.unlink(log_path)

    def test_case_b_nine_generations_with_large_auxiliary_volume(self):
        """
        Regression Case B:
        9 live generations
        large volume of auxiliary gateway requests (146 auxiliary)
        9 successful generation dispatches
        Expected:
        generation dispatch count = 9
        auxiliary volume ignored for scheduler validation
        """
        accounts = [
            {"email": "a@test.com", "is_eligible": True},
            {"email": "b@test.com", "is_eligible": True},
            {"email": "c@test.com", "is_eligible": True},
        ]
        # 9 cyclic round robin dispatches
        gen_sequence = ["a@test.com", "b@test.com", "c@test.com"] * 3

        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as f:
            log_path = f.name
            # Interleave 146 auxiliary requests and 9 generation dispatches
            # Total events = 155 (exact numbers observed in real SJC run)
            gen_idx = 0
            for i in range(146):
                f.write(f"[2026-09-15 13:46:12] [PROXY] POST v1internal:loadCodeAssist -> active@test.com (Status: 200)\n")
                if i % 16 == 0 and gen_idx < 9:
                    f.write(f"[2026-09-15 13:46:17] [PROXY] POST v1internal:streamGenerateContent?alt=sse -> {gen_sequence[gen_idx]} (Status: 200)\n")
                    gen_idx += 1
            while gen_idx < 9:
                f.write(f"[2026-09-15 13:47:00] [PROXY] POST v1internal:streamGenerateContent?alt=sse -> {gen_sequence[gen_idx]} (Status: 200)\n")
                gen_idx += 1
            f.flush()

        try:
            delta = parse_log_delta(log_path, 0)
            self.assertEqual(delta["total_proxy_events"], 155)
            self.assertEqual(delta["auxiliary_events_count"], 146)
            self.assertEqual(delta["generation_attempts_count"], 9)
            self.assertEqual(delta["successful_dispatches_count"], 9)
            self.assertEqual(delta["generation_dispatches"], gen_sequence)
            self.assertEqual(len(delta["failovers"]), 0)

            # Round robin validation must pass on generation sequence, ignoring all 146 auxiliary requests
            rr_res = verify_round_robin(delta["generation_dispatches"], accounts)
            self.assertTrue(rr_res["passed"])

            # Concurrency check must not be confused by the 146 auxiliary calls
            hits_delta = {"total_delta": 9, "gcd": 1}
            conc = detect_concurrent_activity(9, delta["generation_dispatches"], hits_delta)
            self.assertEqual(conc["status"], "CLEAN")
            self.assertFalse(conc["concurrent_detected"])
            self.assertFalse(conc["is_inconclusive"])
        finally:
            os.unlink(log_path)

    def test_case_c_generation_request_fails_over_account_a_to_b(self):
        """
        Regression Case C:
        Generation request fails on account A and fails over successfully to B.
        Expected:
        - generation attempt/failover is represented correctly;
        - successful scheduler selection sequence reflects the effective dispatch semantics used by the verifier;
        - not automatically treated as unrelated external concurrency.
        """
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as f:
            log_path = f.name
            f.write("[2026-09-15 10:00:00] [PROXY ERROR] POST streamGenerateContent -> a@test.com HTTP 429\n")
            f.write("[2026-09-15 10:00:00] [FAILOVER] Account a@test.com hit rate limit/quota error! Cooldown 300s. Auto-switching to next account...\n")
            f.write("[2026-09-15 10:00:01] [PROXY] POST streamGenerateContent -> b@test.com (Status: 200)\n")
            f.flush()

        try:
            delta = parse_log_delta(log_path, 0)
            # Generation attempts: 2 (attempt on A failed 429, attempt on B succeeded 200)
            self.assertEqual(delta["generation_attempts_count"], 2)
            self.assertEqual(delta["generation_attempts"][0]["account"], "a@test.com")
            self.assertEqual(delta["generation_attempts"][0]["status"], 429)
            self.assertEqual(delta["generation_attempts"][1]["account"], "b@test.com")
            self.assertEqual(delta["generation_attempts"][1]["status"], 200)

            # Failovers and quota events
            self.assertEqual(delta["failovers_count"], 1)
            self.assertEqual(delta["failovers"][0]["account"], "a@test.com")
            self.assertGreaterEqual(delta["quota_events_count"], 1)

            # Effective successful generation dispatch sequence contains only b@test.com
            self.assertEqual(delta["successful_dispatches_count"], 1)
            self.assertEqual(delta["generation_dispatches"], ["b@test.com"])

            # Verifier semantics: account B is verified as the effective recipient
            accounts_after_failover = [
                {"email": "a@test.com", "quota": 0.90, "is_cooling": True, "is_eligible": False},
                {"email": "b@test.com", "quota": 0.80, "is_eligible": True},
            ]
            v_res = verify_max_quota(delta["generation_dispatches"], accounts_after_failover)
            self.assertTrue(v_res["passed"])

            # Concurrency detection accounts for failover and does not flag external concurrency
            hits_delta = {"total_delta": 1, "gcd": 1}
            conc = detect_concurrent_activity(1, delta["generation_dispatches"], hits_delta, failovers=delta["failovers"])
            self.assertEqual(conc["status"], "CLEAN")
            self.assertFalse(conc["concurrent_detected"])
            self.assertFalse(conc["is_inconclusive"])
        finally:
            os.unlink(log_path)

    def test_case_d_unrelated_generation_request_inconclusive(self):
        """
        Regression Case D:
        An actual unrelated generation request appears during the test window and cannot be correlated safely.
        Expected:
        INCONCLUSIVE / possible concurrent generation traffic
        not false PASS.
        """
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as f:
            log_path = f.name
            # Test expected 1 run, but 2 generation requests executed (without failover)
            f.write("[2026-09-15 10:00:00] [PROXY] POST streamGenerateContent -> a@test.com (Status: 200)\n")
            f.write("[2026-09-15 10:00:05] [PROXY] POST streamGenerateContent -> unrelated@test.com (Status: 200)\n")
            f.flush()

        try:
            delta = parse_log_delta(log_path, 0)
            self.assertEqual(delta["generation_dispatches"], ["a@test.com", "unrelated@test.com"])
            self.assertEqual(len(delta["failovers"]), 0)

            # Uncorrelated extra generation traffic must evaluate to INCONCLUSIVE (not CLEAN / false PASS)
            hits_delta = {"total_delta": 2, "gcd": 1}
            conc = detect_concurrent_activity(1, delta["generation_dispatches"], hits_delta, failovers=delta["failovers"])
            self.assertEqual(conc["status"], "INCONCLUSIVE")
            self.assertTrue(conc["is_inconclusive"])
            self.assertFalse(conc["concurrent_detected"])
            self.assertIn("concurrent generation traffic", conc["message"].lower())
        finally:
            os.unlink(log_path)

    def test_analyze_hits_delta_per_account_details(self):
        before = [
            {"email": "a@test.com", "hits": 10},
            {"email": "b@test.com", "hits": 5},
        ]
        after = [
            {"email": "a@test.com", "hits": 12},
            {"email": "b@test.com", "hits": 5},
        ]
        res = analyze_hits_delta(before, after)
        self.assertIn("per_account", res)
        self.assertEqual(len(res["per_account"]), 2)
        self.assertEqual(res["per_account"][0], {"account": "a@test.com", "before": 10, "after": 12, "delta": 2})
        self.assertEqual(res["per_account"][1], {"account": "b@test.com", "before": 5, "after": 5, "delta": 0})

    def test_auxiliary_error_does_not_count_as_generation_attempt(self):
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as f:
            log_path = f.name
            f.write("[2026-09-15 10:00:00] [PROXY ERROR] POST v1internal:loadCodeAssist -> a@test.com HTTP 500\n")
            f.write("[2026-09-15 10:00:01] [PROXY EXCEPTION] POST v1internal:fetchUserInfo -> a@test.com: timeout\n")
            f.flush()

        try:
            delta = parse_log_delta(log_path, 0)
            self.assertEqual(delta["total_proxy_events"], 2)
            self.assertEqual(delta["auxiliary_events_count"], 2)
            self.assertEqual(delta["generation_attempts_count"], 0)
            self.assertEqual(delta["successful_dispatches_count"], 0)
        finally:
            os.unlink(log_path)

    def test_live_report_log_delta_dispatches_count_extraction(self):
        """
        Regression check: ensure live-test.sh report extraction of gen_dispatches_count
        from log_delta.json parses and evaluates without SyntaxError.
        """
        sample_delta = {
            "successful_dispatches_count": 9,
            "generation_dispatches": ["a@example.com"] * 9,
            "dispatches": ["a@example.com"] * 9,
            "total_proxy_events": 155,
            "generation_attempts_count": 9,
            "auxiliary_events_count": 146,
            "failovers_count": 0,
        }
        with tempfile.NamedTemporaryFile("w+", delete=False, encoding="utf-8") as f:
            delta_path = f.name
            json.dump(sample_delta, f)

        try:
            expr = (
                f"import json; d=json.load(open(r'{delta_path}')); "
                f"print(d.get('successful_dispatches_count', len(d.get('generation_dispatches', d.get('dispatches', [])))))"
            )
            out = subprocess.check_output([sys.executable, "-c", expr], text=True).strip()
            self.assertEqual(out, "9")

            # Verify fallback when successful_dispatches_count is not present
            with open(delta_path, "w", encoding="utf-8") as f:
                json.dump({"generation_dispatches": ["b@example.com"] * 5}, f)
            out_fallback = subprocess.check_output([sys.executable, "-c", expr], text=True).strip()
            self.assertEqual(out_fallback, "5")

            # Also verify scripts/live-test.sh contains the exact valid python expression
            live_test_sh = os.path.join(ROOT, "scripts", "live-test.sh")
            with open(live_test_sh, "r", encoding="utf-8") as f:
                sh_text = f.read()
            self.assertIn(
                "print(d.get('successful_dispatches_count', len(d.get('generation_dispatches', d.get('dispatches', [])))))",
                sh_text,
            )
        finally:
            os.unlink(delta_path)


if __name__ == "__main__":
    unittest.main()

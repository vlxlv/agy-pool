#!/usr/bin/env python3
"""
Unit tests for the live integration test helper (scripts/live_test.py).

These tests run completely offline, without network, without live credentials,
and complete in sub-second time.
"""

import json
import os
import sys
import tempfile
import unittest

# Ensure scripts directory is importable
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scripts.live_test import (
    analyze_hits_delta,
    detect_concurrent_activity,
    get_log_offset,
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

        # 2 runs expected, 3 observed -> concurrent detected
        res_conc = detect_concurrent_activity(2, dispatches, hits_delta)
        self.assertTrue(res_conc["concurrent_detected"])

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


if __name__ == "__main__":
    unittest.main()

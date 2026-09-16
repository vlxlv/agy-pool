import base64
import contextlib
import email.message
import http.client
import http.server
import importlib.machinery
import importlib.util
import io
import json
import multiprocessing
import os
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from unittest import mock


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "bin", "agy-pool")
loader = importlib.machinery.SourceFileLoader("agy_pool", SCRIPT)
spec = importlib.util.spec_from_loader(loader.name, loader)
agy_pool = importlib.util.module_from_spec(spec)
loader.exec_module(agy_pool)


def account(account_id, token=None):
    return {
        "id": account_id,
        "email": f"{account_id}@example.test",
        "refresh_token": f"refresh-{account_id}",
        "access_token": token or f"token-{account_id}",
        "token_expiry": time.time() + 3600,
        "request_count": 0,
        "error_count": 0,
        "last_quota": {"remaining_fraction": 1.0},
    }


class Scenario:
    def __init__(self, actions):
        self.actions = {key: list(value) for key, value in actions.items()}
        self.calls = []
        self.lock = threading.Lock()

    def take(self, token, request):
        with self.lock:
            self.calls.append((token, request.path, dict(request.headers)))
            choices = self.actions.get(token, self.actions.get("*", []))
            if not choices:
                return (200, b"ok", {})
            return choices.pop(0)


def upstream_handler(scenario):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            self.respond()

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            self.body = self.rfile.read(length)
            self.respond()

        def respond(self):
            authorization = self.headers.get("Authorization", "")
            token = authorization[7:] if authorization.startswith("Bearer ") else authorization
            action = scenario.take(token, self)
            if callable(action):
                action(self)
                return
            status, body, headers = action
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    return Handler


class AgyPoolTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        gemini = os.path.join(self.temp.name, ".gemini")

        # Capture prior state before configuring isolated environment
        orig_gemini_dir = agy_pool.GEMINI_DIR
        orig_test_mode = agy_pool.is_test_mode()

        self.env_patch = mock.patch.dict(os.environ, {
            "HOME": self.temp.name,
            "AGY_GEMINI_DIR": gemini,
            "AGY_TEST_MODE": "1",
        })
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

        # Explicitly configure isolated storage paths and enable fail-closed test guard
        agy_pool.configure_paths(gemini)
        agy_pool.set_test_mode(True)

        def _restore_state():
            agy_pool.set_test_mode(orig_test_mode)
            agy_pool.configure_paths(orig_gemini_dir)
        self.addCleanup(_restore_state)

        agy_pool.FILE_LOCKS.clear()
        agy_pool._QUOTA_REFRESH_IN_FLIGHT.clear()
        agy_pool._QUOTA_REFRESH_RETRY.clear()
        self.refresh_patch = mock.patch.object(agy_pool, "schedule_quota_refresh")
        self.refresh_mock = self.refresh_patch.start()
        self.addCleanup(self.refresh_patch.stop)

    def save_accounts(self, accounts, active="a"):
        agy_pool.save_pool({
            "version": 1,
            "strategy": "max_quota",
            "active_account_id": active,
            "accounts": accounts,
        })

    def start_server(self, handler):
        server = agy_pool.ThreadedHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def start_proxy(self, scenario):
        upstream = self.start_server(upstream_handler(scenario))
        host, port = upstream.server_address
        agy_pool.BACKEND_HOST = f"{host}:{port}"
        agy_pool.BACKEND_URL_BASE = f"http://{host}:{port}"
        proxy = self.start_server(agy_pool.SmartProxyHandler)
        self.assertEqual(proxy.server_address[0], "127.0.0.1")
        return proxy

    def request(self, proxy, path="/v1/test", body=b"{}", headers=None):
        conn = http.client.HTTPConnection(*proxy.server_address, timeout=3)
        conn.request("POST", path, body=body, headers=headers or {})
        response = conn.getresponse()
        data = response.read()
        result = (response.status, data, dict(response.getheaders()))
        conn.close()
        return result

    def test_fresh_snapshot_needs_no_refresh_and_aging_needs_one(self):
        now = 1_000_000.0
        fresh = {"last_quota": {"updated_at": now - 30}}
        aging = {"last_quota": {"updated_at": now - 120}}
        self.assertFalse(agy_pool.quota_refresh_needed(fresh, now))
        self.assertTrue(agy_pool.quota_refresh_needed(aging, now))
        agy_pool.schedule_quota_refresh(aging)
        self.refresh_mock.assert_called_once_with(aging)

    def test_quota_freshness_classes_and_reset_expiry(self):
        now = 1_000_000.0
        def snapshot(updated, reset=None):
            return {"last_quota": {"updated_at": updated, "gemini_5h": {"fraction": 0.7, "reset_time": reset}}}
        self.assertEqual(agy_pool.quota_freshness(snapshot(now - 30), now)["class"], "fresh")
        self.assertEqual(agy_pool.quota_freshness(snapshot(now - 120), now)["class"], "aging")
        self.assertEqual(agy_pool.quota_freshness(snapshot(now - 600), now)["class"], "stale")
        self.assertEqual(agy_pool.quota_freshness({"last_quota": {}}, now)["class"], "unknown")
        self.assertEqual(agy_pool.quota_freshness(snapshot(now - 1, now - 1), now)["class"], "stale")

    def test_refresh_failure_uses_bounded_backoff_and_success_resets(self):
        self.refresh_patch.stop()
        acc = account("retry")
        acc["last_quota"] = {"updated_at": 1, "gemini_5h": {"fraction": 0.7}}
        calls = []
        def fail(_):
            calls.append(1)
        with mock.patch.object(agy_pool, "_safe_quota", side_effect=fail):
            self.assertTrue(agy_pool.schedule_quota_refresh(acc, now=1000))
            for _ in range(20):
                if not agy_pool._QUOTA_REFRESH_IN_FLIGHT:
                    break
                time.sleep(0.01)
            self.assertFalse(agy_pool.schedule_quota_refresh(acc, now=1001))
            retry = agy_pool._QUOTA_REFRESH_RETRY["retry"]
            self.assertGreaterEqual(retry["next_at"], time.time() + 29)
            self.assertLessEqual(retry["next_at"], time.time() + 31)
            self.assertEqual(len(calls), 1)
        # Exhaust the sequence deterministically without sleeping.
        for expected in (60, 120, 240, 300, 300):
            retry["next_at"] = 0
            with mock.patch.object(agy_pool, "_safe_quota", return_value=False):
                self.assertTrue(agy_pool.schedule_quota_refresh(acc, now=2000))
            for _ in range(20):
                if not agy_pool._QUOTA_REFRESH_IN_FLIGHT:
                    break
                time.sleep(0.01)
            retry = agy_pool._QUOTA_REFRESH_RETRY["retry"]
            self.assertLessEqual(abs((retry["next_at"] - time.time()) - expected), 1)
        retry["next_at"] = 0
        with mock.patch.object(agy_pool, "_safe_quota", return_value=True):
            self.assertTrue(agy_pool.schedule_quota_refresh(acc, now=3000))
        for _ in range(20):
            if not agy_pool._QUOTA_REFRESH_IN_FLIGHT:
                break
            time.sleep(0.01)
        self.assertNotIn("retry", agy_pool._QUOTA_REFRESH_RETRY)

    def test_fresh_and_aging_capacity_order_by_capacity(self):
        now = time.time()
        low = account("low")
        low["last_quota"] = {"updated_at": now - 30, "gemini_5h": {"fraction": 0.20}, "gemini_weekly": {"fraction": 0.20}}
        high = account("high")
        high["last_quota"] = {"updated_at": now - 61, "gemini_5h": {"fraction": 0.90}, "gemini_weekly": {"fraction": 0.90}}
        ordered = agy_pool.order_candidates([low, high], now=now)
        self.assertEqual(ordered[0]["id"], "high")

    def test_fresh_and_aging_equal_capacity_use_existing_ties(self):
        now = time.time()
        accounts = []
        for account_id, age in (("first", 30), ("second", 61)):
            acc = account(account_id)
            acc["last_quota"] = {"updated_at": now - age, "gemini_5h": {"fraction": 0.8}, "gemini_weekly": {"fraction": 0.8}}
            accounts.append(acc)
        self.assertEqual(agy_pool.order_candidates(accounts, now=now)[0]["id"], "first")

    def test_stale_is_below_fresh_and_unknown_is_lowest_freshness(self):
        now = time.time()
        def make(account_id, updated):
            acc = account(account_id)
            acc["last_quota"] = {"updated_at": updated, "gemini_5h": {"fraction": 0.8}, "gemini_weekly": {"fraction": 0.8}}
            return acc
        fresh = make("fresh", now - 30)
        stale = make("stale", now - 600)
        unknown = make("unknown", None)
        unknown["last_quota"].pop("updated_at")
        ordered = agy_pool.order_candidates([unknown, stale, fresh], now=now)
        self.assertEqual([a["id"] for a in ordered], ["fresh", "stale", "unknown"])

    def test_stale_refresh_is_single_flight(self):
        self.refresh_patch.stop()
        acc = account("single")
        acc["last_quota"] = {"updated_at": 1, "gemini_5h": {"fraction": 0.7}}
        started = threading.Event()
        release = threading.Event()
        calls = []
        def refresh(_):
            calls.append(1)
            started.set()
            release.wait(2)
        with mock.patch.object(agy_pool, "_safe_quota", side_effect=refresh):
            self.assertTrue(agy_pool.schedule_quota_refresh(acc))
            self.assertFalse(agy_pool.schedule_quota_refresh(acc))
            self.assertTrue(started.wait(1))
            release.set()
        for _ in range(20):
            if not agy_pool._QUOTA_REFRESH_IN_FLIGHT:
                break
            time.sleep(0.01)
        self.assertEqual(len(calls), 1)

    def test_safe_quota_success_persists_fresh_timestamp(self):
        acc = account("freshened")
        old = {"updated_at": 1, "gemini_5h": {"fraction": 0.4}}
        acc["last_quota"] = old
        self.save_accounts([acc])
        new = {"updated_at": int(time.time()), "gemini_5h": {"fraction": 0.8}}
        with mock.patch.object(agy_pool, "query_quota", side_effect=lambda a: a.update(last_quota=new) or new):
            self.assertTrue(agy_pool._safe_quota(dict(acc)))
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["last_quota"], new)

    def test_safe_quota_failure_preserves_snapshot(self):
        acc = account("not-freshened")
        old = {"updated_at": 1, "gemini_5h": {"fraction": 0.4}}
        acc["last_quota"] = old
        self.save_accounts([acc])
        with mock.patch.object(agy_pool, "query_quota", side_effect=OSError("offline")):
            self.assertFalse(agy_pool._safe_quota(dict(acc)))
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["last_quota"], old)

    def test_unknown_quota_does_not_beat_known_capacity(self):
        now = time.time()
        known = account("known")
        known["last_quota"] = {
            "gemini_5h": {"fraction": 0.70, "reset_time": now + 3600},
            "gemini_weekly": {"fraction": 0.70, "reset_time": now + 86400},
        }
        unknown = account("unknown")
        unknown["last_quota"] = {}
        ordered = agy_pool.order_candidates([unknown, known], now=now)
        self.assertEqual([a["id"] for a in ordered], ["known", "unknown"])
        self.assertEqual(agy_pool.compute_capacity_state(unknown)["known_window_count"], 0)

    def test_known_full_quota_is_distinct_from_unknown(self):
        full = account("full")
        full["last_quota"] = {
            "gemini_5h": {"fraction": 1.0},
            "gemini_weekly": {"fraction": 1.0},
        }
        unknown = account("unknown")
        unknown["last_quota"] = {}
        state = agy_pool.compute_capacity_state(full)
        self.assertEqual(state["known_window_count"], 2)
        self.assertEqual(agy_pool.compute_capacity_state(unknown)["known_window_count"], 0)

    def test_fallback_5h_refresh_preserves_cached_weekly(self):
        account_data = account("partial")
        account_data["last_quota"] = {
            "gemini_5h": {"fraction": 0.60, "reset_time": "old-5h"},
            "gemini_weekly": {"fraction": 0.20, "reset_time": "old-weekly"},
        }
        fallback = mock.MagicMock()
        fallback.__enter__.return_value = fallback
        fallback.read.return_value = json.dumps({
            "models": {"gemini-3.8-flash-high": {"quotaInfo": {"remainingFraction": 0.80, "resetTime": "new-5h"}}}
        }).encode()
        primary_error = urllib.error.HTTPError("https://quota", 500, "error", {}, io.BytesIO(b"temporary"))
        with mock.patch.object(agy_pool.urllib.request, "urlopen", side_effect=[primary_error, fallback]), mock.patch.object(agy_pool, "refresh_token", return_value="token"):
            result = agy_pool.query_quota(account_data)
        self.assertEqual(result["gemini_5h"]["fraction"], 0.80)
        self.assertEqual(result["gemini_weekly"]["fraction"], 0.20)

    def test_partial_quota_merge_preserves_cached_weekly(self):
        account_data = account("partial")
        account_data["last_quota"] = {
            "gemini_5h": {"fraction": 0.60, "reset_time": "old-5h"},
            "gemini_weekly": {"fraction": 0.20, "reset_time": "old-weekly"},
        }
        merged = agy_pool._cached_quota_state(account_data["last_quota"])
        merged["gemini_5h"] = {"fraction": 0.80, "reset_time": "new-5h"}
        agy_pool._recompute_compat_quota(merged)
        self.assertEqual(merged["gemini_5h"]["fraction"], 0.80)
        self.assertEqual(merged["gemini_weekly"]["fraction"], 0.20)

    def test_partial_quota_without_cached_weekly_stays_unknown(self):
        merged = agy_pool._cached_quota_state({})
        merged["gemini_5h"] = {"fraction": 0.80, "reset_time": None}
        agy_pool._recompute_compat_quota(merged)
        self.assertNotIn("gemini_weekly", merged)
        self.assertEqual(merged["remaining_fraction"], 0.80)

    def test_complete_quota_refresh_failure_does_not_replace_cache(self):
        account_data = account("cached")
        cached = {"gemini_5h": {"fraction": 0.40}, "gemini_weekly": {"fraction": 0.30}}
        account_data["last_quota"] = cached
        with mock.patch.object(agy_pool, "refresh_token", return_value="token"), \
             mock.patch.object(agy_pool.urllib.request, "urlopen", side_effect=OSError("offline")):
            with self.assertRaises(OSError):
                agy_pool.query_quota(account_data)
        self.assertEqual(account_data["last_quota"], cached)

    def test_validation_and_auth_are_known_unavailable(self):
        account_data = account("restricted")
        state = agy_pool.compute_capacity_state(account_data)
        self.assertFalse(state["is_depleted"])
        account_data["last_quota"] = {
            "gemini_5h": {"fraction": 0.0}, "gemini_weekly": {"fraction": 0.0}
        }
        self.assertTrue(agy_pool.compute_capacity_state(account_data)["is_depleted"])

    def test_legacy_remaining_fraction_is_supported(self):
        state = agy_pool.compute_capacity_state({"last_quota": {"remaining_fraction": 0.4}})
        self.assertEqual(state["known_window_count"], 1)
        self.assertEqual(state["q5"], 0.4)
        self.assertEqual(state["q7"], 0.4)

    def test_unknown_quota_cli_display_is_not_full(self):
        self.assertIn("N/A", agy_pool.render_progress_bar(None))
        self.assertNotIn("100.0%", agy_pool.render_progress_bar(None))

    def test_threaded_request_count_transaction_has_no_lost_updates(self):
        self.save_accounts([account("a")])

        def increment():
            def update(pool):
                pool["accounts"][0]["request_count"] += 1
            agy_pool.pool_transaction(update)

        threads = [threading.Thread(target=increment) for _ in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["request_count"], 20)

    def test_processes_update_different_accounts_without_overwrite(self):
        self.save_accounts([account("a"), account("b")])
        context = multiprocessing.get_context("fork")

        def worker(account_id):
            for _ in range(5):
                def update(pool):
                    next(a for a in pool["accounts"] if a["id"] == account_id)["request_count"] += 1
                agy_pool.pool_transaction(update)

        processes = [context.Process(target=worker, args=(account_id,))
                     for account_id in ("a", "b")]
        for process in processes:
            process.start()
        for process in processes:
            process.join(20)
            self.assertEqual(process.exitcode, 0)
        counts = {a["id"]: a["request_count"] for a in agy_pool.load_pool()["accounts"]}
        self.assertEqual(counts, {"a": 5, "b": 5})

    def test_quota_refresh_and_request_handler_merge_fields(self):
        acc = account("a")
        self.save_accounts([acc])

        def quota_update(snapshot):
            time.sleep(0.02)
            snapshot["last_quota"] = {"remaining_fraction": 0.4, "updated_at": 123}
            return snapshot["last_quota"]

        with mock.patch.object(agy_pool, "query_quota", quota_update):
            refresher = threading.Thread(target=agy_pool._safe_quota, args=(dict(acc),))
            refresher.start()
            for _ in range(25):
                agy_pool.SmartProxyHandler._record_success(None, acc)
            refresher.join()
        stored = agy_pool.load_pool()["accounts"][0]
        self.assertEqual(stored["request_count"], 25)
        self.assertEqual(stored["last_quota"]["remaining_fraction"], 0.4)

    def test_simultaneous_token_refresh_is_single_flight(self):
        acc = account("a")
        acc["token_expiry"] = 0
        self.save_accounts([acc])
        calls = []
        call_lock = threading.Lock()

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self):
                return b'{"access_token":"fresh","expires_in":3600}'

        def urlopen(*args, **kwargs):
            with call_lock:
                calls.append(1)
            time.sleep(0.03)
            return Response()

        snapshots = [dict(acc) for _ in range(12)]
        with mock.patch.object(agy_pool.urllib.request, "urlopen", urlopen):
            threads = [threading.Thread(target=agy_pool.refresh_token, args=(item,))
                       for item in snapshots]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        self.assertEqual(len(calls), 1)
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["access_token"], "fresh")

    def test_process_token_refresh_is_single_flight(self):
        acc = account("a")
        acc["token_expiry"] = 0
        self.save_accounts([acc])
        context = multiprocessing.get_context("fork")
        calls = context.Value("i", 0)

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self):
                return b'{"access_token":"fresh-process","expires_in":3600}'

        def urlopen(*args, **kwargs):
            with calls.get_lock():
                calls.value += 1
            time.sleep(0.03)
            return Response()

        def refresh():
            agy_pool.refresh_token(dict(acc))

        with mock.patch.object(agy_pool.urllib.request, "urlopen", urlopen):
            processes = [context.Process(target=refresh) for _ in range(4)]
            for process in processes:
                process.start()
            for process in processes:
                process.join(10)
                self.assertEqual(process.exitcode, 0)
        self.assertEqual(calls.value, 1)

    def test_failed_quota_refresh_preserves_cached_quota(self):
        acc = account("a")
        acc["last_quota"] = {"remaining_fraction": 0.25, "updated_at": 10}
        self.save_accounts([acc])
        with mock.patch.object(agy_pool.urllib.request, "urlopen",
                               side_effect=OSError("upstream unavailable")):
            self.assertFalse(agy_pool._safe_quota(dict(acc)))
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["last_quota"],
                         {"remaining_fraction": 0.25, "updated_at": 10})

        class EmptyResponse:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self):
                return b"{}"

        with mock.patch.object(agy_pool.urllib.request, "urlopen", return_value=EmptyResponse()):
            self.assertFalse(agy_pool._safe_quota(dict(acc)))
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["last_quota"],
                         {"remaining_fraction": 0.25, "updated_at": 10})

    def test_429_before_commit_fails_over(self):
        self.save_accounts([account("a"), account("b")])
        agy_pool.write_agy_token_file(account("a"))
        with open(agy_pool.AGY_TOKEN_FILE, "rb") as token_file:
            compatibility_token = token_file.read()
        scenario = Scenario({
            "token-a": [(429, b'{"error":{"status":"RESOURCE_EXHAUSTED"}}', {})],
            "token-b": [(200, b"from-b", {})],
        })
        proxy = self.start_proxy(scenario)
        status, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual((status, body), (200, b"from-b"))
        self.assertEqual([call[0] for call in scenario.calls], ["token-a", "token-b"])
        with open(agy_pool.AGY_TOKEN_FILE, "rb") as token_file:
            self.assertEqual(token_file.read(), compatibility_token)

    def test_quota_403_fails_over_but_permission_403_does_not(self):
        self.save_accounts([account("a"), account("b")])
        quota = Scenario({
            "token-a": [(403, b'{"error":{"status":"RESOURCE_EXHAUSTED"}}', {})],
            "token-b": [(200, b"from-b", {})],
        })
        proxy = self.start_proxy(quota)
        self.assertEqual(self.request(proxy)[0:2], (200, b"from-b"))

        self.save_accounts([account("a"), account("b")])
        denied = Scenario({
            "token-a": [(403, b'{"error":{"status":"PERMISSION_DENIED"}}', {})],
            "token-b": [(200, b"wrong-retry", {})],
        })
        proxy = self.start_proxy(denied)
        self.assertEqual(self.request(proxy)[0:2], (403, b'{"error":{"status":"PERMISSION_DENIED"}}'))
        self.assertEqual([call[0] for call in denied.calls], ["token-a"])

    def test_validation_required_fails_over_and_persists_status(self):
        self.save_accounts([account("a"), account("b")])
        val_error = {
            "error": {
                "code": 403,
                "message": "Verify your account to continue.",
                "status": "PERMISSION_DENIED",
                "details": [
                    {
                        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                        "reason": "VALIDATION_REQUIRED",
                        "metadata": {
                            "validation_url": "https://accounts.google.com/signin/continue?foo=bar"
                        }
                    }
                ]
            }
        }
        val_body = json.dumps(val_error).encode()
        scenario = Scenario({
            "token-a": [(403, val_body, {})],
            "token-b": [(200, b"success-from-b", {})],
        })
        proxy = self.start_proxy(scenario)
        status, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual((status, body), (200, b"success-from-b"))
        self.assertEqual([call[0] for call in scenario.calls], ["token-a", "token-b"])

        pool = agy_pool.load_pool()
        acc_a = next(a for a in pool["accounts"] if a["id"] == "a")
        self.assertEqual(acc_a.get("status"), "validation_required")
        self.assertEqual(acc_a.get("validation_url"), "https://accounts.google.com/signin/continue?foo=bar")
        self.assertEqual(acc_a.get("last_quota", {}).get("remaining_fraction"), 0.0)

    def test_started_sse_failure_is_not_replayed(self):
        self.save_accounts([account("a"), account("b")])

        def broken_stream(handler):
            handler.send_response(200)
            handler.send_header("Content-Type", "text/event-stream")
            handler.send_header("Transfer-Encoding", "chunked")
            handler.end_headers()
            handler.wfile.write(b"B\r\ndata: one\n\n\r\nZZ\r\n")
            handler.wfile.flush()
            handler.close_connection = True

        scenario = Scenario({"token-a": [broken_stream], "token-b": [(200, b"replayed", {})]})
        proxy = self.start_proxy(scenario)
        conn = http.client.HTTPConnection(*proxy.server_address, timeout=3)
        conn.request("POST", "/v1internal:streamGenerateContent", body=b"{}")
        response = conn.getresponse()
        with self.assertRaises(http.client.IncompleteRead) as error:
            response.read()
        self.assertIn(b"data: one", error.exception.partial)
        conn.close()
        self.assertEqual([call[0] for call in scenario.calls], ["token-a"])

    def test_client_disconnect_closes_upstream_response(self):
        self.save_accounts([account("a")])
        closed = threading.Event()

        class Response:
            status = 200
            headers = email.message.Message()
            headers["Content-Type"] = "text/event-stream"

            def __enter__(self):
                return self

            def __exit__(self, *args):
                closed.set()

            def read1(self, size):
                return b"x" * 262144

        proxy = self.start_server(agy_pool.SmartProxyHandler)
        with mock.patch.object(agy_pool.urllib.request, "urlopen", return_value=Response()):
            client = socket.create_connection(proxy.server_address, timeout=2)
            client.sendall(b"POST /stream HTTP/1.1\r\nHost: local\r\nContent-Length: 2\r\n\r\n{}")
            client.recv(1024)
            client.close()
            self.assertTrue(closed.wait(3))

    def test_clean_upstream_eof_finishes_chunked_stream(self):
        self.save_accounts([account("a")])
        scenario = Scenario({"token-a": [(200, b"data: done\n\n", {"Content-Type": "text/event-stream"})]})
        proxy = self.start_proxy(scenario)
        self.assertEqual(self.request(proxy, "/stream")[0:2], (200, b"data: done\n\n"))

    def test_malformed_upstream_chunking_returns_502_before_commit(self):
        self.save_accounts([account("a")])

        def malformed(handler):
            handler.send_response(200)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Transfer-Encoding", "chunked")
            handler.end_headers()
            handler.wfile.write(b"5\r\nabc")
            handler.wfile.flush()
            handler.close_connection = True

        proxy = self.start_proxy(Scenario({"token-a": [malformed]}))
        self.assertEqual(self.request(proxy)[0], 502)

    def test_upstream_timeout_returns_504_without_replay(self):
        self.save_accounts([account("a"), account("b")])
        proxy = self.start_server(agy_pool.SmartProxyHandler)
        calls = []

        def timeout(*args, **kwargs):
            calls.append(1)
            raise socket.timeout("timed out")

        with mock.patch.object(agy_pool.urllib.request, "urlopen", timeout):
            self.assertEqual(self.request(proxy)[0], 504)
        self.assertEqual(len(calls), 1)

    def test_malformed_client_chunking_is_rejected(self):
        self.save_accounts([account("a")])
        scenario = Scenario({"token-a": [(200, b"must-not-run", {})]})
        proxy = self.start_proxy(scenario)
        client = socket.create_connection(proxy.server_address, timeout=2)
        client.sendall(b"POST /v1/test HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nZ\r\n")
        client.shutdown(socket.SHUT_WR)
        response = b""
        while True:
            part = client.recv(4096)
            if not part:
                break
            response += part
        client.close()
        self.assertIn(b" 400 ", response.split(b"\r\n", 1)[0])
        self.assertEqual(scenario.calls, [])

    def test_valid_chunked_request_is_reassembled(self):
        self.save_accounts([account("a")])
        received = []

        def capture(handler):
            received.append(handler.body)
            handler.send_response(200)
            handler.send_header("Content-Length", "2")
            handler.end_headers()
            handler.wfile.write(b"ok")

        proxy = self.start_proxy(Scenario({"token-a": [capture]}))
        client = socket.create_connection(proxy.server_address, timeout=2)
        client.sendall(b"POST /v1/test HTTP/1.1\r\nHost: local\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n")
        response = b""
        while True:
            part = client.recv(4096)
            if not part:
                break
            response += part
        client.close()
        self.assertIn(b" 200 ", response.split(b"\r\n", 1)[0])
        self.assertEqual(received, [b"abcde"])

    def test_hop_by_hop_headers_removed_and_content_encoding_preserved(self):
        self.save_accounts([account("a")])
        scenario = Scenario({"token-a": [(200, b"encoded", {"Content-Encoding": "test"})]})
        proxy = self.start_proxy(scenario)
        status, _, headers = self.request(proxy, headers={
            "Connection": "X-Remove",
            "X-Remove": "secret",
            "Proxy-Connection": "keep-alive",
        })
        self.assertEqual(status, 200)
        upstream_headers = scenario.calls[0][2]
        self.assertNotIn("X-Remove", upstream_headers)
        self.assertNotIn("Proxy-Connection", upstream_headers)
        self.assertEqual(headers["Content-Encoding"], "test")

    def test_two_concurrent_sessions_update_state_without_corrupting_token_file(self):
        self.save_accounts([account("a")])
        agy_pool.write_agy_token_file(account("a"))
        with open(agy_pool.AGY_TOKEN_FILE, "rb") as token_file:
            before = token_file.read()
        scenario = Scenario({"token-a": [(200, b"one", {}), (200, b"two", {})]})
        proxy = self.start_proxy(scenario)
        results = []

        def session():
            results.append(self.request(proxy, "/v1internal:generateContent")[0])

        threads = [threading.Thread(target=session) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(results, [200, 200])
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["request_count"], 2)
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["gen_count"], 2)
        with open(agy_pool.AGY_TOKEN_FILE, "rb") as token_file:
            self.assertEqual(token_file.read(), before)
        self.assertEqual(os.stat(agy_pool.AGY_TOKEN_FILE).st_mode & 0o777, 0o600)

    def test_generation_vs_metadata_request_counts(self):
        self.save_accounts([account("a")])
        scenario = Scenario({"token-a": [(200, b"gen", {}), (200, b"meta", {})]})
        proxy = self.start_proxy(scenario)
        self.request(proxy, "/v1internal:generateContent")
        acc = agy_pool.load_pool()["accounts"][0]
        self.assertEqual(acc.get("gen_count"), 1)
        self.assertEqual(acc.get("request_count"), 1)

        self.request(proxy, "/v1internal:fetchUserInfo")
        acc = agy_pool.load_pool()["accounts"][0]
        self.assertEqual(acc.get("gen_count"), 1)
        self.assertEqual(acc.get("request_count"), 2)

    def test_active_switch_does_not_change_in_flight_account(self):
        self.save_accounts([account("a"), account("b")])
        started = threading.Event()
        release = threading.Event()

        def blocked_stream(handler):
            handler.send_response(200)
            handler.send_header("Content-Type", "text/event-stream")
            handler.send_header("Content-Length", "12")
            handler.end_headers()
            started.set()
            release.wait(2)
            handler.wfile.write(b"data: done\n\n")

        scenario = Scenario({"token-a": [blocked_stream], "token-b": [(200, b"new-active", {})]})
        proxy = self.start_proxy(scenario)
        result = []
        running = threading.Thread(target=lambda: result.append(self.request(proxy, "/stream")[1]))
        running.start()
        self.assertTrue(started.wait(2))
        agy_pool.pool_transaction(lambda pool: pool.update(active_account_id="b"))
        release.set()
        running.join(3)
        self.assertEqual(result, [b"data: done\n\n"])
        self.assertEqual(self.request(proxy, "/metadata")[1], b"new-active")
        self.assertEqual([call[0] for call in scenario.calls], ["token-a", "token-b"])

    def test_state_permissions_and_failed_transaction_preserve_state(self):
        self.save_accounts([account("a")])
        self.assertEqual(os.stat(agy_pool.POOL_CONFIG_FILE).st_mode & 0o777, 0o600)
        with open(agy_pool.POOL_CONFIG_FILE, "rb") as state:
            before = state.read()
        with self.assertRaises(RuntimeError):
            agy_pool.pool_transaction(lambda pool: (_ for _ in ()).throw(RuntimeError("crash")))
        with open(agy_pool.POOL_CONFIG_FILE, "rb") as state:
            self.assertEqual(state.read(), before)

    def test_corrupt_state_is_not_replaced(self):
        agy_pool.ensure_dirs()
        with open(agy_pool.POOL_CONFIG_FILE, "wb") as state:
            state.write(b"{broken")
        with self.assertRaises(json.JSONDecodeError):
            agy_pool.pool_transaction(lambda pool: pool.update(strategy="round_robin"))
        with open(agy_pool.POOL_CONFIG_FILE, "rb") as state:
            self.assertEqual(state.read(), b"{broken")

    def test_daemon_sigterm_closes_listener_and_removes_owned_pid(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        previous_port = agy_pool.DEFAULT_PORT
        agy_pool.DEFAULT_PORT = port
        self.addCleanup(setattr, agy_pool, "DEFAULT_PORT", previous_port)
        process = multiprocessing.get_context("fork").Process(
            target=agy_pool.start_proxy_daemon, args=(True,))
        process.start()
        self.addCleanup(lambda: process.is_alive() and process.terminate())
        for _ in range(50):
            if os.path.exists(agy_pool.PID_FILE):
                break
            time.sleep(0.02)
        self.assertTrue(os.path.exists(agy_pool.PID_FILE))
        process.terminate()
        process.join(5)
        self.assertEqual(process.exitcode, 0)
        self.assertFalse(os.path.exists(agy_pool.PID_FILE))
        with socket.socket() as probe:
            self.assertNotEqual(probe.connect_ex(("127.0.0.1", port)), 0)

    def test_conversation_continuation_lookup(self):
        workspace = os.path.join(self.temp.name, "project", "subdir")
        os.makedirs(workspace)
        db_dir = os.path.join(self.temp.name, ".gemini", "antigravity-cli")
        os.makedirs(db_dir, exist_ok=True)
        db = os.path.join(db_dir, "conversation_summaries.db")
        with sqlite3.connect(db) as connection:
            connection.execute("CREATE TABLE conversation_summaries (conversation_id, title, workspace_uris, last_modified_time)")
            connection.execute("INSERT INTO conversation_summaries VALUES (?, ?, ?, ?)",
                               ("old", "Old", json.dumps(["file://" + workspace]), 1))
            connection.execute("INSERT INTO conversation_summaries VALUES (?, ?, ?, ?)",
                               ("new", "New", json.dumps(["file://" + workspace]), 2))
        connection.close()
        self.assertEqual(agy_pool.find_latest_conversation_for_dir(workspace)[0], "new")

    def test_agy_raw_bypasses_proxy_and_preserves_arguments(self):
        native = os.path.join(self.temp.name, "agy-native")
        with open(native, "w", encoding="utf-8") as script:
            script.write('#!/bin/sh\nprintf "%s\\n" "${CLOUD_CODE_URL-unset}" "$@"\n')
        os.chmod(native, 0o700)
        env = dict(os.environ, AGY_BIN=native, CLOUD_CODE_URL="http://127.0.0.1:8899")
        result = subprocess.run(["bash", os.path.join(ROOT, "bin", "agy-raw"), "--model", "x"],
                                env=env, text=True, capture_output=True, check=True)
        self.assertEqual(result.stdout.splitlines(), ["unset", "--model", "x"])

    def test_do_verify_opens_browser_and_clears_status(self):
        acc = account("a")
        acc["status"] = "validation_required"
        acc["validation_url"] = "https://accounts.google.com/verify-test"
        self.save_accounts([acc])

        def fake_query(account_data):
            account_data.pop("status", None)
            account_data.pop("validation_url", None)
            agy_pool._persist_account_fields(account_data, ("status", "validation_url"))
            return {"remaining_fraction": 1.0}

        with mock.patch.object(agy_pool, "query_quota", fake_query):
            res = agy_pool.do_verify("a")
            self.assertTrue(res)
            pool = agy_pool.load_pool()
            self.assertIsNone(pool["accounts"][0].get("status"))

    def test_rotate_log_if_needed_threshold(self):
        log_path = os.path.join(self.temp.name, "test.log")
        with open(log_path, "wb") as f:
            f.write(b"x" * 1000)

        # Below threshold: no rotation
        rotated = agy_pool.rotate_log_if_needed(log_path, max_bytes=2000, backup_count=1)
        self.assertFalse(rotated)
        self.assertEqual(os.path.getsize(log_path), 1000)
        self.assertFalse(os.path.exists(log_path + ".1"))

        # Above threshold: rotate
        rotated = agy_pool.rotate_log_if_needed(log_path, max_bytes=500, backup_count=1)
        self.assertTrue(rotated)
        self.assertEqual(os.path.getsize(log_path), 0)
        self.assertTrue(os.path.exists(log_path + ".1"))
        self.assertEqual(os.path.getsize(log_path + ".1"), 1000)

    def test_rotate_log_copytruncate_preserves_open_fd(self):
        log_path = os.path.join(self.temp.name, "active.log")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write("first line\n")
            f.flush()

            # Rotate while file is still held open by a process/daemon
            rotated = agy_pool.rotate_log_if_needed(log_path, force=True, backup_count=1)
            self.assertTrue(rotated)

            # Subsequent writes to open fd continue writing to truncated active log
            f.write("second line\n")
            f.flush()

        with open(log_path, "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), "second line\n")
        with open(log_path + ".1", "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), "first line\n")

    def test_clear_log(self):
        log_path = os.path.join(self.temp.name, "clear.log")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("active content\n")
        with open(log_path + ".1", "w", encoding="utf-8") as f:
            f.write("backup content\n")

        agy_pool.clear_log(log_path, backup_count=1)
        self.assertEqual(os.path.getsize(log_path), 0)
        self.assertFalse(os.path.exists(log_path + ".1"))

    def test_show_logs_clear_and_rotate_flags(self):
        log_path = os.path.join(self.temp.name, "cli.log")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("log line 1\nlog line 2\n")

        with mock.patch.object(agy_pool, "LOG_FILE", log_path):
            with mock.patch("sys.stdout") as mock_stdout:
                agy_pool.show_logs(rotate=True)
                self.assertTrue(os.path.exists(log_path + ".1"))
                self.assertEqual(os.path.getsize(log_path), 0)

                agy_pool.show_logs(clear=True)
                self.assertFalse(os.path.exists(log_path + ".1"))

    def test_list_accounts_exhausted_and_hits(self):
        acc1 = account("a")
        acc1["gen_count"] = 42
        acc1["request_count"] = 100
        acc1["last_quota"] = {
            "gemini_5h": {"fraction": 0.8},
            "gemini_weekly": {"fraction": 0.9},
        }

        acc2 = account("b")
        acc2["gen_count"] = 15
        acc2["request_count"] = 30
        acc2["last_quota"] = {
            "gemini_5h": {"fraction": 1.0},
            "gemini_weekly": {"fraction": 0.0},
        }

        acc3 = account("c")
        acc3["rate_limited_until"] = time.time() + 300  # Cooldown

        self.save_accounts([acc1, acc2, acc3])
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=None), \
             mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.list_accounts()
        output = buf.getvalue()
        self.assertIn("* Active", output)
        self.assertIn("Hits: 42", output)
        self.assertIn("Exhausted", output)
        self.assertIn("Hits: 15", output)
        self.assertIn("Cooldown", output)
        self.assertIn("Hits: 0", output)
        self.assertIn("CLI Base Token", output)
        self.assertIn("In Rotation Pool", output)
        self.assertNotIn("Total:", output)
        self.assertNotIn("AI Gen:", output)

    def test_crypto_bundle_round_trip(self):
        msg = b"secret-oauth-data-12345"
        enc = agy_pool.encrypt_bundle(msg, "pass123")
        self.assertEqual(enc["format"], "agy-pool-encrypted-v1")
        dec = agy_pool.decrypt_bundle(enc, "pass123")
        self.assertEqual(dec, msg)

        with self.assertRaises(ValueError):
            agy_pool.decrypt_bundle(enc, "wrong-pass")

        # Tamper tag
        tampered = dict(enc, tag=base64.b64encode(b"0" * 32).decode("ascii"))
        with self.assertRaises(ValueError):
            agy_pool.decrypt_bundle(tampered, "pass123")

    def test_export_and_import_plain(self):
        acc1 = account("a")
        acc1["name"] = "Alice"
        acc1["gen_count"] = 10
        acc1["request_count"] = 25
        acc2 = account("b")
        acc2["name"] = "Bob"
        self.save_accounts([acc1, acc2])

        export_file = os.path.join(self.temp.name, "backup.json")
        res = agy_pool.export_pool(export_file)
        self.assertTrue(res)
        self.assertTrue(os.path.exists(export_file))
        self.assertEqual(os.stat(export_file).st_mode & 0o777, 0o600)

        with open(export_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(len(data["accounts"]), 2)
        self.assertEqual(data["strategy"], "max_quota")
        self.assertEqual(data["active_account_email"], "a@example.test")

        # Now wipe local pool and import with replace
        self.save_accounts([])
        res = agy_pool.import_pool(export_file, replace=True)
        self.assertTrue(res)
        restored = agy_pool.load_pool()["accounts"]
        self.assertEqual(len(restored), 2)
        self.assertEqual(restored[0]["email"], "a@example.test")
        self.assertEqual(restored[1]["email"], "b@example.test")
        self.assertEqual(restored[0]["name"], "Alice")
        self.assertEqual(restored[0]["gen_count"], 10)

    def test_export_and_import_encrypted(self):
        acc = account("a")
        self.save_accounts([acc])

        enc_file = os.path.join(self.temp.name, "backup.enc")
        res = agy_pool.export_pool(enc_file, encrypt=True, password="mypassword")
        self.assertTrue(res)

        with open(enc_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
        self.assertEqual(raw["format"], "agy-pool-encrypted-v1")

        # Test import wrong password
        buf = io.StringIO()
        with mock.patch("sys.stderr", buf):
            res = agy_pool.import_pool(enc_file, password="badpass")
        self.assertFalse(res)

        # Test import correct password
        self.save_accounts([])
        res = agy_pool.import_pool(enc_file, password="mypassword")
        self.assertTrue(res)
        self.assertEqual(len(agy_pool.load_pool()["accounts"]), 1)

    def test_import_merge_and_skip_existing(self):
        acc1 = account("a", token="old-token")
        acc1["name"] = "Alice Old"
        self.save_accounts([acc1])

        backup_accounts = [
            {
                "email": "a@example.test",
                "name": "Alice Updated",
                "refresh_token": "new-refresh-a",
                "access_token": "new-token-a",
                "token_expiry": time.time() + 7200,
            },
            {
                "email": "b@example.test",
                "name": "Bob",
                "refresh_token": "refresh-b",
            }
        ]
        backup_file = os.path.join(self.temp.name, "merge_backup.json")
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "accounts": backup_accounts}, f)

        agy_pool.import_pool(backup_file)
        pool = agy_pool.load_pool()["accounts"]
        self.assertEqual(len(pool), 2)
        acc_a = next(a for a in pool if a["email"] == "a@example.test")
        self.assertEqual(acc_a["refresh_token"], "new-refresh-a")
        self.assertEqual(acc_a["access_token"], "new-token-a")

        # Test skip-existing
        backup_accounts[0]["refresh_token"] = "should-not-apply"
        with open(backup_file, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "accounts": backup_accounts}, f)
        agy_pool.import_pool(backup_file, skip_existing=True)
        pool = agy_pool.load_pool()["accounts"]
        acc_a = next(a for a in pool if a["email"] == "a@example.test")
        self.assertEqual(acc_a["refresh_token"], "new-refresh-a")

    def test_daemon_info_and_outdated_detection(self):
        agy_pool.ensure_dirs()
        # 1. Non-existent PID file
        if os.path.exists(agy_pool.PID_FILE):
            os.unlink(agy_pool.PID_FILE)
        self.assertIsNone(agy_pool.get_daemon_info())

        # 2. Legacy integer PID file
        my_pid = os.getpid()
        with open(agy_pool.PID_FILE, "w", encoding="utf-8") as f:
            f.write(str(my_pid))
        info = agy_pool.get_daemon_info()
        self.assertIsNotNone(info)
        self.assertEqual(info["pid"], my_pid)
        self.assertIsNone(info["version"])

        # 3. Structured JSON PID file
        meta = {"pid": my_pid, "version": agy_pool.VERSION, "script_mtime": int(time.time()) + 1000}
        with open(agy_pool.PID_FILE, "w", encoding="utf-8") as f:
            json.dump(meta, f)
        info = agy_pool.get_daemon_info()
        self.assertIsNotNone(info)
        self.assertEqual(info["pid"], my_pid)
        self.assertEqual(info["version"], agy_pool.VERSION)

        # 4. Outdated detection
        with mock.patch.object(agy_pool, "is_port_listening", return_value=True):
            # Same version, future script_mtime -> not outdated
            self.assertFalse(agy_pool.is_daemon_outdated())

            # Older version -> outdated
            meta["version"] = "0.1.0-alpha1"
            with open(agy_pool.PID_FILE, "w", encoding="utf-8") as f:
                json.dump(meta, f)
            self.assertTrue(agy_pool.is_daemon_outdated())

            # Outdated mtime -> outdated
            meta["version"] = agy_pool.VERSION
            meta["script_mtime"] = 100  # long past
            with open(agy_pool.PID_FILE, "w", encoding="utf-8") as f:
                json.dump(meta, f)
            self.assertTrue(agy_pool.is_daemon_outdated())

    def test_ensure_daemon_running_hot_reloads_outdated_daemon(self):
        with mock.patch.object(agy_pool, "is_daemon_running", return_value=True), \
             mock.patch.object(agy_pool, "is_daemon_outdated", return_value=True), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=1234), \
             mock.patch.object(agy_pool, "stop_proxy_daemon") as mock_stop, \
             mock.patch.object(agy_pool, "start_proxy_daemon") as mock_start, \
             mock.patch("time.sleep"):
            agy_pool.ensure_daemon_running()
            mock_stop.assert_called_once()
            mock_start.assert_called_once_with(foreground=False)

    def test_cli_version_command(self):
        buf = io.StringIO()
        with mock.patch("sys.argv", ["agy-pool", "version"]), \
             mock.patch("sys.stdout", buf):
            agy_pool.main()
        self.assertIn(f"agy-pool {agy_pool.VERSION}", buf.getvalue())

    def test_parse_retry_after(self):
        # Integer seconds
        self.assertEqual(agy_pool._parse_retry_after({"Retry-After": "45"}), 45)
        # Min clamp (5s)
        self.assertEqual(agy_pool._parse_retry_after({"Retry-After": "1"}), 5)
        # Max clamp (86400s)
        self.assertEqual(agy_pool._parse_retry_after({"Retry-After": "999999"}), 86400)
        # Invalid / missing -> default
        self.assertEqual(agy_pool._parse_retry_after({}), 300)
        self.assertEqual(agy_pool._parse_retry_after({"Retry-After": "invalid"}), 300)

        # HTTP-date format (RFC 2822 / RFC 7231)
        future_dt = datetime.now(timezone.utc)
        date_str = email.utils.format_datetime(future_dt)
        parsed = agy_pool._parse_retry_after({"Retry-After": date_str})
        self.assertGreaterEqual(parsed, 5)

    def test_429_retry_after_cooldown_duration(self):
        self.save_accounts([account("a"), account("b")])
        scenario = Scenario({
            "token-a": [(429, b'{"error":{"status":"RESOURCE_EXHAUSTED"}}', {"Retry-After": "60"})],
            "token-b": [(200, b"from-b", {})],
        })
        proxy = self.start_proxy(scenario)
        now_before = time.time()
        status, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual((status, body), (200, b"from-b"))
        pool = agy_pool.load_pool()
        acc_a = next(a for a in pool["accounts"] if a["id"] == "a")
        self.assertGreaterEqual(acc_a["rate_limited_until"], now_before + 55)
        self.assertLessEqual(acc_a["rate_limited_until"], now_before + 65)

    def test_strategy_least_used_and_round_robin_selection(self):
        acc1 = account("a")
        acc1["gen_count"] = 10
        acc1["last_used_at"] = 1000
        acc1["last_quota"] = {"remaining_fraction": 1.0}

        acc2 = account("b")
        acc2["gen_count"] = 2
        acc2["last_used_at"] = 2000
        acc2["last_quota"] = {"remaining_fraction": 0.5}

        # 1. least_used strategy: b has gen_count=2, a has 10 -> b selected first despite lower quota
        self.save_accounts([acc1, acc2])
        agy_pool.pool_transaction(lambda p: p.update(strategy="least_used"))
        scenario = Scenario({"token-b": [(200, b"ok-b", {})]})
        proxy = self.start_proxy(scenario)
        status, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual((status, body), (200, b"ok-b"))
        self.assertEqual(scenario.calls[0][0], "token-b")

        # 2. round_robin strategy: a has last_used_at=1000 (older), b has 2000 -> a selected first
        self.save_accounts([acc1, acc2])
        agy_pool.pool_transaction(lambda p: p.update(strategy="round_robin"))
        scenario2 = Scenario({"token-a": [(200, b"ok-a", {})]})
        proxy2 = self.start_proxy(scenario2)
        status, body, _ = self.request(proxy2, "/v1internal:streamGenerateContent")
        self.assertEqual((status, body), (200, b"ok-a"))
        self.assertEqual(scenario2.calls[0][0], "token-a")

    def test_manage_strategy_cli_and_validation(self):
        self.save_accounts([account("a")])
        self.assertEqual(agy_pool.load_pool().get("strategy"), "max_quota")

        # Update to least_used
        self.assertTrue(agy_pool.manage_strategy("least_used"))
        self.assertEqual(agy_pool.load_pool().get("strategy"), "least_used")

        # Update to round_robin
        self.assertTrue(agy_pool.manage_strategy("round_robin"))
        self.assertEqual(agy_pool.load_pool().get("strategy"), "round_robin")

        # Reject invalid strategy
        self.assertFalse(agy_pool.manage_strategy("nonexistent"))
        self.assertEqual(agy_pool.load_pool().get("strategy"), "round_robin")

        # Query strategy without args returns current
        self.assertEqual(agy_pool.manage_strategy(), "round_robin")

    def test_rename_account(self):
        acc = account("a")
        acc["name"] = "OldName"
        self.save_accounts([acc])

        # Rename by index
        self.assertTrue(agy_pool.rename_account("1", "Primary Work"))
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["name"], "Primary Work")

        # Rename by id
        self.assertTrue(agy_pool.rename_account("a", "Personal"))
        self.assertEqual(agy_pool.load_pool()["accounts"][0]["name"], "Personal")

        # Non-existent account
        self.assertFalse(agy_pool.rename_account("acc_999", "Test"))
        # Empty name
        self.assertFalse(agy_pool.rename_account("a", "   "))

    def test_doctor_command_execution(self):
        acc = account("a")
        self.save_accounts([acc])
        buf = io.StringIO()
        fake_sock = mock.MagicMock()
        with mock.patch("sys.stdout", buf), \
             mock.patch("socket.create_connection", return_value=fake_sock), \
             mock.patch("ssl.create_default_context"):
            res = agy_pool.run_doctor()
        out = buf.getvalue()
        self.assertIn("Antigravity System Doctor", out)
        self.assertIn("Python Runtime", out)
        self.assertIn("Account Pool: 1 account(s)", out)

    def test_conversation_unquoting_with_spaces(self):
        workspace = os.path.join(self.temp.name, "my workspace", "sub project")
        os.makedirs(workspace)
        db_dir = os.path.join(self.temp.name, ".gemini", "antigravity-cli")
        os.makedirs(db_dir, exist_ok=True)
        db = os.path.join(db_dir, "conversation_summaries.db")
        with sqlite3.connect(db) as connection:
            connection.execute("CREATE TABLE conversation_summaries (conversation_id, title, workspace_uris, last_modified_time)")
            uri = "file://" + urllib.parse.quote(workspace)
            connection.execute("INSERT INTO conversation_summaries VALUES (?, ?, ?, ?)",
                               ("space-cid", "Space Title", json.dumps([uri]), 10))
        connection.close()
        cid, title, matched = agy_pool.find_latest_conversation_for_dir(workspace)
        self.assertEqual(cid, "space-cid")
        self.assertEqual(title, "Space Title")
        self.assertEqual(matched, os.path.realpath(workspace))

    def test_resolve_continue_arg_with_equals_syntax(self):
        args = ["--conversation=custom-cid", "-c"]
        resolved = agy_pool.resolve_continue_arg(args)
        self.assertEqual(resolved, args)

    def test_resolve_gateway_port(self):
        # Default
        self.assertEqual(agy_pool.resolve_gateway_port(), 8899)
        self.assertEqual(agy_pool.resolve_gateway_port(""), 8899)
        self.assertEqual(agy_pool.resolve_gateway_port(None), 8899)

        # Valid override
        self.assertEqual(agy_pool.resolve_gateway_port("9000"), 9000)
        self.assertEqual(agy_pool.resolve_gateway_port(8080), 8080)
        self.assertEqual(agy_pool.resolve_gateway_port(1), 1)
        self.assertEqual(agy_pool.resolve_gateway_port(65535), 65535)

        # Env var override
        with mock.patch.dict(os.environ, {"AGY_PORT": "9999"}):
            self.assertEqual(agy_pool.resolve_gateway_port(), 9999)

        # Invalid strings
        with self.assertRaises(ValueError):
            agy_pool.resolve_gateway_port("abc")
        with self.assertRaises(ValueError):
            agy_pool.resolve_gateway_port("8899-invalid")

        # Range violations
        with self.assertRaises(ValueError):
            agy_pool.resolve_gateway_port(0)
        with self.assertRaises(ValueError):
            agy_pool.resolve_gateway_port("0")
        with self.assertRaises(ValueError):
            agy_pool.resolve_gateway_port(-1)
        with self.assertRaises(ValueError):
            agy_pool.resolve_gateway_port(65536)
        with self.assertRaises(ValueError):
            agy_pool.resolve_gateway_port("70000")

    def test_round_robin_rotation_and_cursor_advancement(self):
        acc1 = account("a")
        acc2 = account("b")
        acc3 = account("c")
        self.save_accounts([acc1, acc2, acc3])
        agy_pool.pool_transaction(lambda p: p.update(strategy="round_robin"))

        # Candidate order does not advance cursor
        pool = agy_pool.load_pool()
        ordered = agy_pool.order_candidates(pool["accounts"], strategy="round_robin", pool=pool)
        self.assertEqual([a["id"] for a in ordered], ["a", "b", "c"])
        self.assertIsNone(agy_pool.load_pool().get("round_robin_last_account_id"))

        # Request 1 dispatches to A
        scenario = Scenario({
            "token-a": [(200, b"res-a", {}), (200, b"res-a", {}), (200, b"res-a", {})],
            "token-b": [(200, b"res-b", {})],
            "token-c": [(200, b"res-c", {}), (200, b"res-c", {})],
            "token-d": [(200, b"res-d", {})],
        })
        proxy = self.start_proxy(scenario)
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(body, b"res-a")
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "a")

        # Request 2 dispatches to B
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(body, b"res-b")
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "b")

        # Request 3 dispatches to C
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(body, b"res-c")
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "c")

        # Request 4 wraps around to A
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(body, b"res-a")
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "a")

        # Put account B in cooldown; next request should skip B and dispatch to C
        agy_pool.pool_transaction(lambda p: [a.update(rate_limited_until=time.time() + 300) for a in p["accounts"] if a["id"] == "b"])
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(body, b"res-c")
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "c")

        # Delete account C; cursor was on C; fallback cleanly picks next available (A)
        agy_pool.pool_transaction(lambda p: p.update(accounts=[a for a in p["accounts"] if a["id"] != "c"]))
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(body, b"res-a")

        # Add account D; it joins rotation
        acc4 = account("d")
        agy_pool.pool_transaction(lambda p: p["accounts"].append(acc4))
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(body, b"res-d")
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "d")

    def test_round_robin_concurrent_selection_prevents_duplicate_account(self):
        acc1 = account("a")
        acc2 = account("b")
        acc3 = account("c")
        self.save_accounts([acc1, acc2, acc3])
        agy_pool.pool_transaction(lambda p: p.update(strategy="round_robin", round_robin_last_account_id="a"))

        # Upstream coordination: hold both upstream handlers until both have arrived
        barrier = threading.Barrier(2)
        received_tokens = []
        tokens_lock = threading.Lock()

        def make_upstream_handler(token_name):
            def handler(req_handler):
                with tokens_lock:
                    received_tokens.append(token_name)
                barrier.wait(timeout=5)
                req_handler.send_response(200)
                req_handler.send_header("Content-Type", "text/event-stream")
                req_handler.send_header("Connection", "close")
                req_handler.end_headers()
                req_handler.wfile.write(b"data: ok\n\n")
            return handler

        scenario = Scenario({
            "token-b": [make_upstream_handler("token-b")],
            "token-c": [make_upstream_handler("token-c")],
        })
        proxy = self.start_proxy(scenario)

        results = []
        def send_request():
            st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
            results.append((st, body))

        t1 = threading.Thread(target=send_request)
        t2 = threading.Thread(target=send_request)
        t1.start()
        t2.start()
        t1.join(timeout=6)
        t2.join(timeout=6)

        self.assertEqual(len(results), 2)
        self.assertTrue(all(st == 200 for st, _ in results))

        # Both concurrent requests must have selected distinct accounts: {token-b, token-c}
        self.assertEqual(len(received_tokens), 2)
        self.assertEqual(set(received_tokens), {"token-b", "token-c"})
        # Persisted cursor must be 'c'
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "c")

    def test_round_robin_out_of_order_completion_preserves_cursor(self):
        acc1 = account("a")
        acc2 = account("b")
        acc3 = account("c")
        self.save_accounts([acc1, acc2, acc3])
        agy_pool.pool_transaction(lambda p: p.update(strategy="round_robin", round_robin_last_account_id="a"))

        # Request 1 (reserving B) will be held until Request 2 (reserving C) has completely finished
        b_arrived = threading.Event()
        b_can_finish = threading.Event()

        def b_handler(req_handler):
            b_arrived.set()
            b_can_finish.wait(timeout=5)
            req_handler.send_response(200)
            req_handler.send_header("Content-Length", "4")
            req_handler.send_header("Connection", "close")
            req_handler.end_headers()
            req_handler.wfile.write(b"ok-b")

        scenario = Scenario({
            "token-b": [b_handler],
            "token-c": [(200, b"ok-c", {})],
        })
        proxy = self.start_proxy(scenario)

        results = {}
        def run_req1():
            st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
            results["req1"] = (st, body)

        t1 = threading.Thread(target=run_req1)
        t1.start()

        # Wait until Request 1 has reserved B and arrived at upstream
        self.assertTrue(b_arrived.wait(timeout=5))
        # Cursor is now B
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "b")

        # Now execute Request 2 synchronously. It reserves C, dispatches C, and finishes!
        st2, body2, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual((st2, body2), (200, b"ok-c"))
        # Request 2 completed, cursor is C
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "c")

        # Now allow Request 1 (which reserved B) to finish later
        b_can_finish.set()
        t1.join(timeout=5)
        self.assertEqual(results["req1"], (200, b"ok-b"))

        # Late completion of B must NOT move cursor backward to B; cursor remains C
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "c")

    def test_round_robin_failure_does_not_rollback_cursor(self):
        acc1 = account("a")
        acc2 = account("b")
        acc3 = account("c")
        self.save_accounts([acc1, acc2, acc3])
        agy_pool.pool_transaction(lambda p: p.update(strategy="round_robin", round_robin_last_account_id="a"))

        # B returns 500 (non-failover error)
        scenario = Scenario({
            "token-b": [(500, b"internal server error", {})],
            "token-c": [(200, b"ok-c", {})],
        })
        proxy = self.start_proxy(scenario)

        # Request 1 reserves B; upstream returns 500
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(st, 500)
        self.assertEqual(body, b"internal server error")

        # Cursor must NOT rollback to A; it remains B
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "b")

        # Next independent RR request starts after B and chooses C
        st2, body2, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(st2, 200)
        self.assertEqual(body2, b"ok-c")
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "c")

    def test_strategy_isolation_non_rr_does_not_mutate_rr_cursor(self):
        acc1 = account("a")
        acc1["last_quota"] = {"remaining_fraction": 0.9}
        acc2 = account("b")
        acc2["last_quota"] = {"remaining_fraction": 0.5}
        self.save_accounts([acc1, acc2])
        # Set an existing round_robin_last_account_id
        agy_pool.pool_transaction(lambda p: p.update(strategy="max_quota", round_robin_last_account_id="existing_cursor"))

        scenario = Scenario({
            "token-a": [(200, b"res-a", {})],
            "token-b": [(200, b"res-b", {})],
        })
        proxy = self.start_proxy(scenario)

        # 1. max_quota dispatches to highest quota (a)
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(st, 200)
        # Cursor must NOT be overwritten by max_quota
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "existing_cursor")

        # 2. Switch to least_used
        agy_pool.pool_transaction(lambda p: p.update(strategy="least_used"))
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(st, 200)
        # Cursor must NOT be overwritten by least_used
        self.assertEqual(agy_pool.load_pool().get("round_robin_last_account_id"), "existing_cursor")

    def test_retry_after_account_isolation(self):
        acc1 = account("a")
        acc2 = account("b")
        self.save_accounts([acc1, acc2])

        # Acc1 returns 429 with Retry-After: 60, failover to Acc2 succeeds
        scenario = Scenario({
            "token-a": [(429, b"ResourceExhausted", {"Retry-After": "60"})],
            "token-b": [(200, b"ok-b", {})],
        })
        proxy = self.start_proxy(scenario)
        st, body, _ = self.request(proxy, "/v1internal:streamGenerateContent")
        self.assertEqual(body, b"ok-b")

        pool = agy_pool.load_pool()
        a = next(x for x in pool["accounts"] if x["id"] == "a")
        b = next(x for x in pool["accounts"] if x["id"] == "b")

        # Acc1 is throttled
        self.assertGreater(a.get("rate_limited_until", 0), time.time() + 40)
        # Acc2 is untouched and NOT throttled
        self.assertIsNone(b.get("rate_limited_until"))
        self.assertEqual(b.get("gen_count", 0), 1)

    def test_doctor_diagnostics_and_exit_policy(self):
        acc = account("a")
        self.save_accounts([acc])

        fake_sock = mock.MagicMock()

        # 1. Normal run with warnings (e.g. missing native binary in temp test dir) -> returns True
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf), \
             mock.patch("socket.create_connection", return_value=fake_sock), \
             mock.patch("ssl.create_default_context"), \
             mock.patch.object(agy_pool, "refresh_token") as mock_refresh:
            res = agy_pool.run_doctor()
            self.assertTrue(res)
            # Verify read-only: no token refresh calls
            mock_refresh.assert_not_called()

        # 2. Fatal failure: mock Python version < 3.8 -> returns False (exit 1)
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf), \
             mock.patch("socket.create_connection", return_value=fake_sock), \
             mock.patch("ssl.create_default_context"), \
             mock.patch("sys.version_info", (3, 7, 0)):
            res = agy_pool.run_doctor()
            self.assertFalse(res)
        self.assertIn("Python 3.8+ required", buf.getvalue())

        # 3. Upstream TLS/network failure -> produces WARN and returns True (exit 0)
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf), \
             mock.patch("socket.create_connection", side_effect=OSError("Network unreachable")), \
             mock.patch.object(agy_pool, "refresh_token") as mock_refresh:
            res = agy_pool.run_doctor()
            self.assertTrue(res)
            mock_refresh.assert_not_called()
        self.assertIn(f"Cloud Code API: Connection to {agy_pool.BACKEND_HOST} failed", buf.getvalue())
        self.assertIn("warning(s)", buf.getvalue())

        # 4. Missing or corrupted pool file -> returns False
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf), \
             mock.patch("socket.create_connection", return_value=fake_sock), \
             mock.patch("ssl.create_default_context"), \
             mock.patch.object(agy_pool, "load_pool", side_effect=ValueError("corrupted JSON")):
            res = agy_pool.run_doctor()
            self.assertFalse(res)
        self.assertIn("Failed to read pool file", buf.getvalue())

    def test_max_quota_fallback_preserves_raw_order(self):
        now = time.time()
        cd1 = account("cd_later")
        cd1["rate_limited_until"] = now + 500
        cd2 = account("cd_sooner")
        cd2["rate_limited_until"] = now + 10

        res1 = account("z_restricted")
        res1["status"] = "validation_required"
        res2 = account("a_restricted")
        res2["status"] = "auth_error"

        # Under max_quota, fallback ordering must preserve exact input order (pre-alpha9 stable sort)
        ordered = agy_pool.order_candidates([cd1, cd2, res1, res2], strategy="max_quota", now=now)
        self.assertEqual([a["id"] for a in ordered], ["cd_later", "cd_sooner", "z_restricted", "a_restricted"])

    def test_strategy_legacy_pool_and_tie_breaking(self):
        # Legacy pool missing "strategy" field
        self.save_accounts([account("a")])
        agy_pool.pool_transaction(lambda p: p.pop("strategy", None))
        pool = agy_pool.load_pool()
        self.assertNotIn("strategy", pool)

        # Default query returns max_quota
        self.assertEqual(agy_pool.manage_strategy(), "max_quota")

        # order_candidates defaults to max_quota
        acc1 = account("a")
        acc1["last_quota"] = {"remaining_fraction": 0.5}
        acc2 = account("b")
        acc2["last_quota"] = {"remaining_fraction": 0.9}
        ordered = agy_pool.order_candidates([acc1, acc2], pool=pool)
        self.assertEqual([x["id"] for x in ordered], ["b", "a"])

        # least_used tie-breaking: equal hits -> highest quota first -> stable ID
        acc1 = account("a")
        acc1["gen_count"] = 5
        acc1["last_quota"] = {"remaining_fraction": 0.50}
        acc2 = account("b")
        acc2["gen_count"] = 5
        acc2["last_quota"] = {"remaining_fraction": 0.80}
        acc3 = account("c")
        acc3["gen_count"] = 5
        acc3["last_quota"] = {"remaining_fraction": 0.80}

        ordered_lu = agy_pool.order_candidates([acc1, acc2, acc3], strategy="least_used")
        # acc2 and acc3 have higher quota (0.80) than acc1 (0.50). Between acc2 and acc3, 'b' < 'c'
        self.assertEqual([x["id"] for x in ordered_lu], ["b", "c", "a"])

    def test_conversation_unquoting_additional_characters(self):
        # Test directory with '+' (%2B) and '#' (%23) and spaces
        workspace = os.path.join(self.temp.name, "c++ project #1", "src")
        os.makedirs(workspace)
        db_dir = os.path.join(self.temp.name, ".gemini", "antigravity-cli")
        os.makedirs(db_dir, exist_ok=True)
        db = os.path.join(db_dir, "conversation_summaries.db")
        with sqlite3.connect(db) as connection:
            connection.execute("CREATE TABLE conversation_summaries (conversation_id, title, workspace_uris, last_modified_time)")
            uri = "file://" + urllib.parse.quote(workspace)
            connection.execute("INSERT INTO conversation_summaries VALUES (?, ?, ?, ?)",
                               ("special-cid", "Special Chars Title", json.dumps([uri]), 100))
        connection.close()
        cid, title, matched = agy_pool.find_latest_conversation_for_dir(workspace)
        self.assertEqual(cid, "special-cid")
        self.assertEqual(title, "Special Chars Title")
        self.assertEqual(matched, os.path.realpath(workspace))

    def test_cli_exit_codes_on_subcommand_errors(self):
        self.save_accounts([account("a")])
        # Invalid strategy via sys.argv mock to main()
        with mock.patch("sys.argv", ["agy-pool", "strategy", "invalid_strat"]), \
             self.assertRaises(SystemExit) as cm:
            agy_pool.main()
        self.assertEqual(cm.exception.code, 1)

        # Rename failure (empty name)
        with mock.patch("sys.argv", ["agy-pool", "rename", "a", "   "]), \
             self.assertRaises(SystemExit) as cm:
            agy_pool.main()
        self.assertEqual(cm.exception.code, 1)

    def test_compute_capacity_state_math_and_fallbacks(self):
        now = 1726400000.0

        # Positive pace (surplus: 60% remaining with 30m left in 5h window)
        acc_surplus = {
            "last_quota": {
                "gemini_5h": {"fraction": 0.60, "reset_time": now + 1800},
                "gemini_weekly": {"fraction": 0.70, "reset_time": now + 10800},
            }
        }
        cap = agy_pool.compute_capacity_state(acc_surplus, now=now)
        # r5 = 1800 / 18000 = 0.10 -> pace5 = 0.60 - 0.10 = 0.50
        self.assertAlmostEqual(cap["pace5"], 0.50, places=4)
        self.assertAlmostEqual(cap["pace7"], 0.70 - (10800 / 604800.0), places=4)
        self.assertAlmostEqual(cap["worst_pace"], 0.50, places=4)
        self.assertFalse(cap["is_depleted"])

        # Clamping: future reset beyond window clamped to r=1.0
        acc_clamped = {
            "last_quota": {
                "gemini_5h": {"fraction": 0.90, "reset_time": now + 36000},  # 10h > 5h
                "gemini_weekly": {"fraction": 0.90, "reset_time": now + 1000000},  # > 7d
            }
        }
        cap_c = agy_pool.compute_capacity_state(acc_clamped, now=now)
        self.assertEqual(cap_c["r5"], 1.0)
        self.assertEqual(cap_c["r7"], 1.0)
        self.assertAlmostEqual(cap_c["pace5"], -0.10, places=4)
        self.assertAlmostEqual(cap_c["pace7"], -0.10, places=4)

        # Past/stale reset time (t <= 0) falls back to r=1.0
        acc_stale = {
            "last_quota": {
                "gemini_5h": {"fraction": 0.80, "reset_time": now - 100},
                "gemini_weekly": {"fraction": 0.80, "reset_time": now},
            }
        }
        cap_s = agy_pool.compute_capacity_state(acc_stale, now=now)
        self.assertEqual(cap_s["r5"], 1.0)
        self.assertEqual(cap_s["r7"], 1.0)
        self.assertAlmostEqual(cap_s["worst_pace"], -0.20, places=4)

        # Missing reset time falls back to r=1.0
        acc_missing = {"last_quota": {"remaining_fraction": 0.75}}
        cap_m = agy_pool.compute_capacity_state(acc_missing, now=now)
        self.assertEqual(cap_m["r5"], 1.0)
        self.assertEqual(cap_m["r7"], 1.0)
        self.assertAlmostEqual(cap_m["worst_pace"], -0.25, places=4)

        # Hard quota floor: min(q5, q7) <= 0.005 marks is_depleted = True
        acc_depleted = {
            "last_quota": {
                "gemini_5h": {"fraction": 0.003, "reset_time": now + 60},
                "gemini_weekly": {"fraction": 0.90, "reset_time": now + 86400},
            }
        }
        cap_d = agy_pool.compute_capacity_state(acc_depleted, now=now)
        self.assertTrue(cap_d["is_depleted"])
        self.assertAlmostEqual(cap_d["raw_floor"], 0.003, places=4)

    def test_max_quota_reset_aware_scenarios(self):
        now = 1726400000.0

        # Scenario A: Misleading high raw quota vs moderate quota resetting soon
        # Account A: 90% (5h away), 90% (7d away) -> worst pace -0.10
        # Account B: 60% (30m away), 70% (3h away) -> worst pace +0.50
        acc_a = account("acc_a")
        acc_a["last_quota"] = {
            "gemini_5h": {"fraction": 0.90, "reset_time": now + 18000},
            "gemini_weekly": {"fraction": 0.90, "reset_time": now + 604800},
        }
        acc_b = account("acc_b")
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.60, "reset_time": now + 1800},
            "gemini_weekly": {"fraction": 0.70, "reset_time": now + 10800},
        }
        ordered = agy_pool.order_candidates([acc_a, acc_b], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_b", "acc_a"])

        # Scenario B: Weekly bottleneck protection
        # Account A: 80% 5h (1h away), 10% weekly (6d away) -> worst pace ≈ -0.757
        # Account B: 50% 5h (2.5h away), 50% weekly (3.5d away) -> worst pace 0.0
        acc_a["last_quota"] = {
            "gemini_5h": {"fraction": 0.80, "reset_time": now + 3600},
            "gemini_weekly": {"fraction": 0.10, "reset_time": now + (6 * 86400)},
        }
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.50, "reset_time": now + 9000},
            "gemini_weekly": {"fraction": 0.50, "reset_time": now + (3.5 * 86400)},
        }
        ordered = agy_pool.order_candidates([acc_a, acc_b], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_b", "acc_a"])

        # Scenario C: 5-Hour bottleneck protection
        # Account A: 15% 5h (4.5h away), 85% weekly (1d away) -> worst pace -0.75
        # Account B: 40% 5h (2h away), 40% weekly (2d away) -> worst pace 0.0
        acc_a["last_quota"] = {
            "gemini_5h": {"fraction": 0.15, "reset_time": now + 16200},
            "gemini_weekly": {"fraction": 0.85, "reset_time": now + 86400},
        }
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.40, "reset_time": now + 7200},
            "gemini_weekly": {"fraction": 0.40, "reset_time": now + (2 * 86400)},
        }
        ordered = agy_pool.order_candidates([acc_a, acc_b], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_b", "acc_a"])

        # Scenario D: Hard depletion floor overrides pace score
        # Account A: 0.2% quota resetting in 1 minute (depleted)
        # Account B: 20% quota resetting in 4 hours (healthy eligible)
        acc_a["last_quota"] = {
            "gemini_5h": {"fraction": 0.002, "reset_time": now + 60},
            "gemini_weekly": {"fraction": 0.80, "reset_time": now + 86400},
        }
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.20, "reset_time": now + 14400},
            "gemini_weekly": {"fraction": 0.20, "reset_time": now + (5 * 86400)},
        }
        ordered = agy_pool.order_candidates([acc_a, acc_b], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_b", "acc_a"])

        # Scenario E: Missing reset time fallback
        # Account A has 80% with no reset (worst_pace = -0.20)
        # Account B has 70% with reset in 1h 5h and 1d weekly (worst_pace = +0.50)
        acc_a["last_quota"] = {"remaining_fraction": 0.80}
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.70, "reset_time": now + 3600},
            "gemini_weekly": {"fraction": 0.70, "reset_time": now + 86400},
        }
        ordered = agy_pool.order_candidates([acc_a, acc_b], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_b", "acc_a"])

        # Both missing reset: raw quota ranking preserved
        acc_b["last_quota"] = {"remaining_fraction": 0.70}
        ordered = agy_pool.order_candidates([acc_a, acc_b], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_a", "acc_b"])

        # Scenario F: Stable exact tie
        acc_1 = account("acc_1")
        acc_1["last_quota"] = {"remaining_fraction": 0.80}
        acc_2 = account("acc_2")
        acc_2["last_quota"] = {"remaining_fraction": 0.80}
        ordered = agy_pool.order_candidates([acc_1, acc_2], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_1", "acc_2"])

    def test_least_used_reset_aware_tie_breaking(self):
        now = 1726400000.0
        # Hits is primary: lower hits account always selected first
        acc_few_hits = account("few_hits")
        acc_few_hits["gen_count"] = 1
        acc_few_hits["last_quota"] = {
            "gemini_5h": {"fraction": 0.20, "reset_time": now + 14400},
            "gemini_weekly": {"fraction": 0.20, "reset_time": now + 400000},
        }
        acc_many_hits = account("many_hits")
        acc_many_hits["gen_count"] = 10
        acc_many_hits["last_quota"] = {
            "gemini_5h": {"fraction": 0.90, "reset_time": now + 1800},
            "gemini_weekly": {"fraction": 0.90, "reset_time": now + 10800},
        }
        ordered = agy_pool.order_candidates([acc_many_hits, acc_few_hits], strategy="least_used", now=now)
        self.assertEqual([x["id"] for x in ordered], ["few_hits", "many_hits"])

        # Equal hits: reset-aware capacity pace tie-breaks
        acc_low_cap = account("low_cap")
        acc_low_cap["gen_count"] = 3
        acc_low_cap["last_quota"] = {
            "gemini_5h": {"fraction": 0.30, "reset_time": now + 14400},  # worst_pace = 0.30 - 0.80 = -0.50
            "gemini_weekly": {"fraction": 0.80, "reset_time": now + 86400},
        }
        acc_high_cap = account("high_cap")
        acc_high_cap["gen_count"] = 3
        acc_high_cap["last_quota"] = {
            "gemini_5h": {"fraction": 0.60, "reset_time": now + 1800},   # worst_pace = 0.60 - 0.10 = +0.50
            "gemini_weekly": {"fraction": 0.80, "reset_time": now + 86400},
        }
        ordered = agy_pool.order_candidates([acc_low_cap, acc_high_cap], strategy="least_used", now=now)
        self.assertEqual([x["id"] for x in ordered], ["high_cap", "low_cap"])

    def test_max_quota_full_precision_beats_hits(self):
        now = 1726400000.0
        # A: worst_pace = 0.014, hits = 100
        # B: worst_pace = 0.011, hits = 0
        # 5h window: W5 = 18000s, reset at now + 9000 (r5 = 0.5)
        # weekly window: W7 = 604800s, reset at now + 302400 (r7 = 0.5)
        acc_a = account("acc_a")
        acc_a["gen_count"] = 100
        acc_a["last_quota"] = {
            "gemini_5h": {"fraction": 0.514, "reset_time": now + 9000},
            "gemini_weekly": {"fraction": 0.514, "reset_time": now + 302400},
        }
        acc_b = account("acc_b")
        acc_b["gen_count"] = 0
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.511, "reset_time": now + 9000},
            "gemini_weekly": {"fraction": 0.511, "reset_time": now + 302400},
        }
        # A has worst_pace 0.014 > B's 0.011; full precision must not round to 0.01 and let Hits decide
        ordered = agy_pool.order_candidates([acc_b, acc_a], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_a", "acc_b"])

    def test_max_quota_total_pace_beats_hits(self):
        now = 1726400000.0
        # A: worst_pace = 0.10, total_pace = 0.40, hits = 100
        # B: worst_pace = 0.10, total_pace = 0.20, hits = 0
        acc_a = account("acc_a")
        acc_a["gen_count"] = 100
        acc_a["last_quota"] = {
            "gemini_5h": {"fraction": 0.60, "reset_time": now + 9000},       # pace5 = 0.60 - 0.50 = 0.10
            "gemini_weekly": {"fraction": 0.80, "reset_time": now + 302400}, # pace7 = 0.80 - 0.50 = 0.30
        }
        acc_b = account("acc_b")
        acc_b["gen_count"] = 0
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.60, "reset_time": now + 9000},       # pace5 = 0.60 - 0.50 = 0.10
            "gemini_weekly": {"fraction": 0.60, "reset_time": now + 302400}, # pace7 = 0.60 - 0.50 = 0.10
        }
        # Equal worst_pace (0.10), A has higher total_pace (0.40 > 0.20); must evaluate before Hits
        ordered = agy_pool.order_candidates([acc_b, acc_a], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_a", "acc_b"])

    def test_max_quota_raw_floor_beats_hits(self):
        now = 1726400000.0
        # A: worst_pace = 0.25, total_pace = 0.50, raw_floor = 0.75, hits = 50
        # B: worst_pace = 0.25, total_pace = 0.50, raw_floor = 0.50, hits = 0
        # Using exact dyadic fractions (powers of 2) for zero floating-point representation error:
        acc_a = account("acc_a")
        acc_a["gen_count"] = 50
        acc_a["last_quota"] = {
            "gemini_5h": {"fraction": 0.75, "reset_time": now + 9000},       # r5 = 0.50 -> pace5 = 0.25
            "gemini_weekly": {"fraction": 0.75, "reset_time": now + 302400}, # r7 = 0.50 -> pace7 = 0.25
        }
        acc_b = account("acc_b")
        acc_b["gen_count"] = 0
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.50, "reset_time": now + 4500},       # r5 = 0.25 -> pace5 = 0.25
            "gemini_weekly": {"fraction": 0.875, "reset_time": now + 378000},# r7 = 0.625 -> pace7 = 0.25
        }
        # Equal worst_pace (0.25) and total_pace (0.50); A has higher raw_floor (0.75 > 0.50); must evaluate before Hits
        ordered = agy_pool.order_candidates([acc_b, acc_a], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_a", "acc_b"])

    def test_max_quota_hits_remains_final_capacity_tie_break(self):
        now = 1726400000.0
        acc_a = account("acc_a")
        acc_a["gen_count"] = 25
        acc_a["last_quota"] = {
            "gemini_5h": {"fraction": 0.70, "reset_time": now + 9000},
            "gemini_weekly": {"fraction": 0.70, "reset_time": now + 302400},
        }
        acc_b = account("acc_b")
        acc_b["gen_count"] = 5
        acc_b["last_quota"] = {
            "gemini_5h": {"fraction": 0.70, "reset_time": now + 9000},
            "gemini_weekly": {"fraction": 0.70, "reset_time": now + 302400},
        }
        # Identical capacity metrics: lower Hits (acc_b with 5 < 25) must rank first
        ordered = agy_pool.order_candidates([acc_a, acc_b], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered], ["acc_b", "acc_a"])

    def test_max_quota_exact_full_tie_preserves_stable_order(self):
        now = 1726400000.0
        acc_1 = account("first")
        acc_1["gen_count"] = 10
        acc_1["last_quota"] = {"remaining_fraction": 0.85}
        acc_2 = account("second")
        acc_2["gen_count"] = 10
        acc_2["last_quota"] = {"remaining_fraction": 0.85}

        # Original order [first, second] preserved
        ordered_1 = agy_pool.order_candidates([acc_1, acc_2], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered_1], ["first", "second"])

        # Original order [second, first] preserved
        ordered_2 = agy_pool.order_candidates([acc_2, acc_1], strategy="max_quota", now=now)
        self.assertEqual([x["id"] for x in ordered_2], ["second", "first"])

    def test_display_account_name_resolution(self):
        # 1. Explicit friendly name
        self.assertEqual(agy_pool.display_account_name({"name": "Work", "id": "acc_1", "email": "user@secret.com"}), "Work")
        self.assertEqual(agy_pool.display_account_name({"name": "  Project Lead  ", "id": "acc_2"}), "Project Lead")
        # 2. Safe fallback for acc_N
        self.assertEqual(agy_pool.display_account_name({"name": None, "id": "acc_1", "email": "user@secret.com"}), "Account 1")
        self.assertEqual(agy_pool.display_account_name({"name": "", "id": "acc_42", "email": "user@secret.com"}), "Account 42")
        # 3. Generic safe fallback
        self.assertEqual(agy_pool.display_account_name({"name": None, "id": "custom_uuid", "email": "user@secret.com"}), "Account")
        self.assertEqual(agy_pool.display_account_name({}), "Account")
        self.assertEqual(agy_pool.display_account_name(None), "Account")
        self.assertEqual(agy_pool.display_account_name("invalid"), "Account")

    def test_list_accounts_privacy_and_target_filtering(self):
        acc1 = account("acc_1")
        acc1["email"] = "supersecret_alpha@example.org"
        acc1["name"] = "Production Cloud"
        acc1["gen_count"] = 12

        acc2 = account("acc_2")
        acc2["email"] = "confidential_beta@enterprise.com"
        acc2["name"] = None
        acc2["gen_count"] = 7

        self.save_accounts([acc1, acc2], active="acc_1")

        # Full listing
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=None), \
             mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.list_accounts()
        output = buf.getvalue()

        self.assertIn("[1] Production Cloud", output)
        self.assertIn("[2] Account 2", output)
        self.assertNotIn("supersecret_alpha@example.org", output)
        self.assertNotIn("supersecret_alpha", output)
        self.assertNotIn("confidential_beta@enterprise.com", output)
        self.assertNotIn("confidential_beta", output)

        # Filtered by target index
        buf_idx = io.StringIO()
        with mock.patch("sys.stdout", buf_idx), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=None), \
             mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.list_accounts("1")
        out_idx = buf_idx.getvalue()
        self.assertIn("[1] Production Cloud", out_idx)
        self.assertNotIn("Account 2", out_idx)
        self.assertNotIn("supersecret_alpha", out_idx)

        # Filtered by email target input (matching succeeds, but email is NEVER echoed in output)
        buf_email = io.StringIO()
        with mock.patch("sys.stdout", buf_email), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=None), \
             mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.list_accounts("confidential_beta@enterprise.com")
        out_email = buf_email.getvalue()
        self.assertIn("Account 2", out_email)
        self.assertNotIn("Production Cloud", out_email)
        self.assertNotIn("confidential_beta@enterprise.com", out_email)
        self.assertNotIn("confidential_beta", out_email)

        # Filtered by friendly name target input
        buf_name = io.StringIO()
        with mock.patch("sys.stdout", buf_name), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=None), \
             mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.list_accounts("Production Cloud")
        out_name = buf_name.getvalue()
        self.assertIn("[1] Production Cloud", out_name)
        self.assertNotIn("Account 2", out_name)
        self.assertNotIn("supersecret_alpha", out_name)

    def test_account_management_privacy_with_real_email_targets(self):
        acc1 = account("acc_1")
        acc1["email"] = "alice_dev@corp.internal"
        acc1["name"] = None

        acc2 = account("acc_2")
        acc2["email"] = "bob_ops@corp.internal"
        acc2["name"] = None

        self.save_accounts([acc1, acc2], active="acc_1")

        # switch using real email target
        buf_sw = io.StringIO()
        with mock.patch("sys.stdout", buf_sw), mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.switch_account("bob_ops@corp.internal")
        out_sw = buf_sw.getvalue()
        self.assertIn("Account 2", out_sw)
        self.assertNotIn("bob_ops@corp.internal", out_sw)
        self.assertNotIn("bob_ops", out_sw)
        self.assertEqual(agy_pool.load_pool()["active_account_id"], "acc_2")

        # rename using real email target
        buf_ren = io.StringIO()
        with mock.patch("sys.stdout", buf_ren):
            agy_pool.rename_account("bob_ops@corp.internal", "Operations Lead")
        out_ren = buf_ren.getvalue()
        self.assertIn("Operations Lead", out_ren)
        self.assertIn("acc_2", out_ren)
        self.assertNotIn("bob_ops@corp.internal", out_ren)
        self.assertNotIn("bob_ops", out_ren)
        self.assertEqual(agy_pool.load_pool()["accounts"][1]["name"], "Operations Lead")

        # switch using new friendly name
        buf_sw_name = io.StringIO()
        with mock.patch("sys.stdout", buf_sw_name), mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.switch_account("Operations Lead")
        out_sw_name = buf_sw_name.getvalue()
        self.assertIn("Operations Lead", out_sw_name)
        self.assertNotIn("bob_ops", out_sw_name)

        # remove using real email target
        buf_rm = io.StringIO()
        with mock.patch("sys.stdout", buf_rm):
            agy_pool.remove_account("alice_dev@corp.internal")
        out_rm = buf_rm.getvalue()
        self.assertIn("Account 1", out_rm)
        self.assertNotIn("alice_dev@corp.internal", out_rm)
        self.assertNotIn("alice_dev", out_rm)
        pool = agy_pool.load_pool()
        self.assertEqual(len(pool["accounts"]), 1)
        self.assertEqual(pool["accounts"][0]["name"], "Operations Lead")

        # remove using friendly name
        buf_rm_name = io.StringIO()
        with mock.patch("sys.stdout", buf_rm_name):
            agy_pool.remove_account("Operations Lead")
        out_rm_name = buf_rm_name.getvalue()
        self.assertIn("Operations Lead", out_rm_name)
        self.assertNotIn("bob_ops", out_rm_name)
        self.assertEqual(len(agy_pool.load_pool()["accounts"]), 0)

    def test_backward_compatibility_alpha_9_state(self):
        """
        Backward-compatibility gate: alpha.9-style pool state.
        Older account entries lack:
          - gemini_weekly
          - quota freshness timestamps (updated_at)
          - known-window flags
          - friendly display names ('name' field is None or missing)
        Expected:
          - pool loads successfully
          - accounts remain usable
          - no migration subsystem required
          - no account deleted or rewritten incorrectly
          - missing friendly name displays as Account N
        """
        alpha_9_pool = {
            "version": 1,
            "strategy": "least_used",
            "active_account_id": "acc_1",
            "accounts": [
                {
                    "id": "acc_1",
                    "email": "alpha9_user1@example.com",
                    "refresh_token": "mock_rf_1",
                    "access_token": "mock_at_1",
                    "token_expiry": 1726000000,
                    "request_count": 20,
                    "gen_count": 8,
                    "created_at": 1725000000,
                    "last_quota": {
                        "remaining_fraction": 0.85,
                        "reset_time": 1726050000,
                    },
                },
                {
                    "id": "acc_2",
                    "email": "alpha9_user2@example.com",
                    "refresh_token": "mock_rf_2",
                    "access_token": "mock_at_2",
                    "token_expiry": 1726000000,
                    "request_count": 5,
                    "gen_count": 2,
                    "created_at": 1725000000,
                    "last_quota": {
                        "remaining_fraction": 0.40,
                    },
                },
            ],
        }
        os.makedirs(agy_pool.GEMINI_DIR, exist_ok=True)
        with open(agy_pool.POOL_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(alpha_9_pool, f)

        # 1. Pool loads successfully
        pool = agy_pool.load_pool()
        self.assertEqual(len(pool["accounts"]), 2)
        self.assertEqual(pool["active_account_id"], "acc_1")
        self.assertEqual(pool["strategy"], "least_used")

        # 2. Display rendering uses Account N and never exposes email
        buf = io.StringIO()
        with mock.patch("sys.stdout", buf), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=None), \
             mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.list_accounts()
        out = buf.getvalue()
        self.assertIn("[1] Account 1", out)
        self.assertIn("[2] Account 2", out)
        self.assertNotIn("alpha9_user1@example.com", out)
        self.assertNotIn("alpha9_user2@example.com", out)
        self.assertIn("Hits: 8", out)
        self.assertIn("Hits: 2", out)

        # 3. Capacity calculation handles missing weekly and missing updated_at
        cap1 = agy_pool.compute_capacity_state(pool["accounts"][0])
        self.assertEqual(cap1["known_window_count"], 1)
        self.assertAlmostEqual(cap1["raw_floor"], 0.85, places=4)
        self.assertEqual(agy_pool.quota_freshness(pool["accounts"][0])["class"], "unknown")  # missing updated_at

        # 4. Scheduling operates correctly across all strategies
        ordered_lu = agy_pool.order_candidates(pool["accounts"], strategy="least_used")
        self.assertEqual([x["id"] for x in ordered_lu], ["acc_2", "acc_1"])

        ordered_mq = agy_pool.order_candidates(pool["accounts"], strategy="max_quota")
        self.assertEqual([x["id"] for x in ordered_mq], ["acc_1", "acc_2"])

        # 5. Accounts are preserved without data loss after save
        agy_pool.save_pool(pool)
        reloaded = agy_pool.load_pool()
        self.assertEqual(len(reloaded["accounts"]), 2)
        self.assertEqual(reloaded["accounts"][0]["email"], "alpha9_user1@example.com")
        self.assertEqual(reloaded["accounts"][1]["email"], "alpha9_user2@example.com")

    def test_backward_compatibility_alpha_10_state(self):
        """
        Backward-compatibility gate: alpha.10-style pool state.
        Expected:
          - existing last_quota structures load
          - remaining_fraction legacy fallback still works
          - partial/unknown semantics remain compatible
          - friendly names render where available, Account N fallback otherwise
        """
        alpha_10_pool = {
            "version": 1,
            "strategy": "max_quota",
            "active_account_id": "acc_1",
            "accounts": [
                {
                    "id": "acc_1",
                    "name": "Production Node",
                    "email": "node1@example.com",
                    "refresh_token": "rf_1",
                    "access_token": "at_1",
                    "token_expiry": 1726450000,
                    "updated_at": time.time(),
                    "gen_count": 10,
                    "last_quota": {
                        "updated_at": time.time(),
                        "gemini_5h": {"fraction": 0.90, "reset_time": time.time() + 18000},
                        "gemini_weekly": {"fraction": 0.95, "reset_time": time.time() + 600000},
                    },
                },
                {
                    "id": "acc_2",
                    "email": "node2@example.com",
                    "refresh_token": "rf_2",
                    "access_token": "at_2",
                    "token_expiry": 1726450000,
                    "gen_count": 3,
                    "last_quota": {
                        "gemini_5h": {"fraction": 0.60},
                    },
                },
                {
                    "id": "acc_3",
                    "email": "node3@example.com",
                    "refresh_token": "rf_3",
                    "access_token": "at_3",
                    "token_expiry": 1726450000,
                    "gen_count": 1,
                    "last_quota": {
                        "gemini_5h": {"fraction": 0.002, "reset_time": time.time() + 3600},
                        "gemini_weekly": {"fraction": 0.90, "reset_time": time.time() + 500000},
                    },
                },
            ],
        }
        os.makedirs(agy_pool.GEMINI_DIR, exist_ok=True)
        with open(agy_pool.POOL_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(alpha_10_pool, f)

        pool = agy_pool.load_pool()
        self.assertEqual(len(pool["accounts"]), 3)

        buf = io.StringIO()
        with mock.patch("sys.stdout", buf), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=None), \
             mock.patch.object(agy_pool, "_safe_quota"):
            agy_pool.list_accounts()
        out = buf.getvalue()

        self.assertIn("[1] Production Node", out)
        self.assertIn("[2] Account 2", out)
        self.assertIn("[3] Account 3", out)
        self.assertIn("Exhausted", out)  # acc_3 <= 0.005
        self.assertNotIn("node1@example.com", out)
        self.assertNotIn("node2@example.com", out)
        self.assertNotIn("node3@example.com", out)

        # Quota confidence & capacity checks
        cap1 = agy_pool.compute_capacity_state(pool["accounts"][0])
        self.assertEqual(cap1["known_window_count"], 2)
        self.assertEqual(agy_pool.quota_freshness(pool["accounts"][0])["class"], "fresh")

        cap2 = agy_pool.compute_capacity_state(pool["accounts"][1])
        self.assertEqual(cap2["known_window_count"], 1)

        cap3 = agy_pool.compute_capacity_state(pool["accounts"][2])
        self.assertTrue(cap3["is_depleted"])

        ordered = agy_pool.order_candidates(pool["accounts"], strategy="max_quota")
        # Healthy dual-window acc_1 ranks top, depleted acc_3 ranks lowest
        self.assertEqual(ordered[0]["id"], "acc_1")
        self.assertEqual(ordered[-1]["id"], "acc_3")

    def test_installer_and_upgrade_lifecycle(self):
        """
        Installer / upgrade gate:
        - Fresh install
        - Upgrade over existing install (idempotent, no duplicate aliases)
        - Uninstall (cleans binaries & aliases, preserves pool data)
        - Reinstall
        """
        fake_home = os.path.join(self.temp.name, "fake_home")
        os.makedirs(fake_home, exist_ok=True)
        bashrc = os.path.join(fake_home, ".bashrc")
        with open(bashrc, "w", encoding="utf-8") as f:
            f.write("# existing bashrc content\nexport FOO=bar\n")

        # Create dummy pool data before install
        gemini_dir = os.path.join(fake_home, ".gemini")
        os.makedirs(gemini_dir, exist_ok=True)
        pool_file = os.path.join(gemini_dir, "agy-pool-accounts.json")
        with open(pool_file, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "accounts": [{"id": "acc_keep", "email": "keep@example.com"}]}, f)

        repo_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        install_script = os.path.join(repo_dir, "install.sh")
        uninstall_script = os.path.join(repo_dir, "uninstall.sh")

        env = dict(os.environ, HOME=fake_home, PREFIX="")
        # 1. Fresh install
        proc = subprocess.run(["bash", install_script], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(proc.returncode, 0, f"install.sh failed: {proc.stderr}")

        bin_dir = os.path.join(fake_home, ".local", "bin")
        self.assertTrue(os.path.islink(os.path.join(bin_dir, "agy-pool")))
        self.assertTrue(os.path.islink(os.path.join(bin_dir, "agy-raw")))
        self.assertTrue(os.path.islink(os.path.join(bin_dir, "agy-orig")))

        with open(bashrc, "r", encoding="utf-8") as f:
            bashrc_content = f.read()
        self.assertEqual(bashrc_content.count("# >>> agy-pool integration >>>"), 1)
        self.assertIn("alias agy='agy-pool run'", bashrc_content)

        # 2. Upgrade (re-run install.sh)
        proc_up = subprocess.run(["bash", install_script], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(proc_up.returncode, 0, f"install.sh upgrade failed: {proc_up.stderr}")
        with open(bashrc, "r", encoding="utf-8") as f:
            bashrc_content_up = f.read()
        self.assertEqual(bashrc_content_up.count("# >>> agy-pool integration >>>"), 1, "Duplicate alias block introduced during upgrade")

        # Verify pool file preserved
        with open(pool_file, "r", encoding="utf-8") as f:
            pool_data = json.load(f)
        self.assertEqual(pool_data["accounts"][0]["id"], "acc_keep")

        # 3. Uninstall
        proc_un = subprocess.run(["bash", uninstall_script], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(proc_un.returncode, 0, f"uninstall.sh failed: {proc_un.stderr}")

        self.assertFalse(os.path.exists(os.path.join(bin_dir, "agy-pool")))
        self.assertFalse(os.path.exists(os.path.join(bin_dir, "agy-raw")))
        self.assertFalse(os.path.exists(os.path.join(bin_dir, "agy-orig")))

        with open(bashrc, "r", encoding="utf-8") as f:
            bashrc_clean = f.read()
        self.assertNotIn("agy-pool integration", bashrc_clean)

        # Pool file must still be preserved
        self.assertTrue(os.path.exists(pool_file))
        with open(pool_file, "r", encoding="utf-8") as f:
            self.assertEqual(json.load(f)["accounts"][0]["id"], "acc_keep")

    def test_daemon_restart_and_recovery_preserves_state(self):
        """
        Daemon restart / recovery gate:
        Verify that daemon restart does not reset quota snapshot,
        preserves friendly names, and keeps cooldown state intact.
        """
        cooldown_target = time.time() + 1200
        now = time.time()
        pool_state = {
            "version": 1,
            "strategy": "round_robin",
            "active_account_id": "acc_1",
            "accounts": [
                {
                    "id": "acc_1",
                    "name": "Workstation Alpha",
                    "email": "alpha@example.com",
                    "refresh_token": "rf_1",
                    "access_token": "at_1",
                    "token_expiry": now + 3600,
                    "updated_at": now - 30,
                    "last_quota": {
                        "gemini_5h": {"fraction": 0.72, "reset_time": now + 7200},
                        "gemini_weekly": {"fraction": 0.88, "reset_time": now + 86400},
                    },
                },
                {
                    "id": "acc_2",
                    "name": "Standby Beta",
                    "email": "beta@example.com",
                    "refresh_token": "rf_2",
                    "access_token": "at_2",
                    "token_expiry": now + 3600,
                    "rate_limited_until": cooldown_target,
                    "updated_at": now - 50,
                    "last_quota": {
                        "gemini_5h": {"fraction": 0.40, "reset_time": now + 1800},
                        "gemini_weekly": {"fraction": 0.60, "reset_time": now + 86400},
                    },
                },
            ],
        }
        os.makedirs(agy_pool.GEMINI_DIR, exist_ok=True)
        with open(agy_pool.POOL_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(pool_state, f)

        # Simulate restart sequence (ensure_daemon_running / reload)
        with mock.patch.object(agy_pool, "is_daemon_running", return_value=True), \
             mock.patch.object(agy_pool, "is_daemon_outdated", return_value=True), \
             mock.patch.object(agy_pool, "get_daemon_pid", return_value=9999), \
             mock.patch.object(agy_pool, "stop_proxy_daemon") as mock_stop, \
             mock.patch.object(agy_pool, "start_proxy_daemon") as mock_start, \
             mock.patch("time.sleep"):
            agy_pool.ensure_daemon_running()
            mock_stop.assert_called_once()
            mock_start.assert_called_once_with(foreground=False)

        # Verify state integrity after simulated restart
        reloaded = agy_pool.load_pool()
        acc1 = reloaded["accounts"][0]
        acc2 = reloaded["accounts"][1]

        self.assertEqual(acc1["name"], "Workstation Alpha")
        self.assertEqual(acc1["last_quota"]["gemini_5h"]["fraction"], 0.72)
        self.assertEqual(acc1["last_quota"]["gemini_weekly"]["fraction"], 0.88)

        self.assertEqual(acc2["name"], "Standby Beta")
        self.assertEqual(acc2["rate_limited_until"], cooldown_target)
        self.assertEqual(acc2["last_quota"]["gemini_5h"]["fraction"], 0.40)

    def test_production_pool_path_isolation_and_fail_closed_guard(self):
        """
        Regression test: Verify that all pool state operations are strictly
        redirected to isolated test paths, a sentinel file in a protected location
        remains completely untouched, and direct writes to forbidden paths raise
        RuntimeError in test mode.
        """
        with tempfile.TemporaryDirectory() as protected_dir:
            sentinel_path = os.path.join(protected_dir, "agy-pool-accounts.json")
            sentinel_payload = {"version": 1, "accounts": [{"id": "protected_account", "email": "protected@example.com"}]}
            with open(sentinel_path, "w", encoding="utf-8") as f:
                json.dump(sentinel_payload, f)
            initial_mtime = os.path.getmtime(sentinel_path)

            agy_pool.register_forbidden_path(protected_dir)

            # 1. Perform writes using the standard test harness
            self.save_accounts([account("isolated_1"), account("isolated_2")], active="isolated_1")
            agy_pool.pool_transaction(lambda p: p["accounts"].append(account("isolated_3")))

            # Verify isolated pool got updated inside self.temp.name
            pool = agy_pool.load_pool()
            self.assertEqual(len(pool["accounts"]), 3)
            self.assertEqual([a["id"] for a in pool["accounts"]], ["isolated_1", "isolated_2", "isolated_3"])
            self.assertTrue(agy_pool.POOL_CONFIG_FILE.startswith(self.temp.name))

            # Verify sentinel is 100% untouched
            with open(sentinel_path, "r", encoding="utf-8") as f:
                sentinel_after = json.load(f)
            self.assertEqual(sentinel_after, sentinel_payload)
            self.assertEqual(os.path.getmtime(sentinel_path), initial_mtime)

            # 2. Verify fail-closed guard prevents writing to protected paths
            with self.assertRaises(RuntimeError) as cm_write:
                agy_pool._atomic_json_write(sentinel_path, {"hacked": True})
            self.assertIn("[FAIL-CLOSED TEST GUARD]", str(cm_write.exception))

            # Verify fail-closed guard prevents acquiring lock in protected path
            with self.assertRaises(RuntimeError) as cm_lock:
                with agy_pool._file_lock(os.path.join(protected_dir, "test.lock")):
                    pass
            self.assertIn("[FAIL-CLOSED TEST GUARD]", str(cm_lock.exception))

            # Verify fail-closed guard prevents log rotation / clear on protected path
            with self.assertRaises(RuntimeError) as cm_log:
                agy_pool.rotate_log_if_needed(os.path.join(protected_dir, "test.log"), force=True)
            self.assertIn("[FAIL-CLOSED TEST GUARD]", str(cm_log.exception))

            with self.assertRaises(RuntimeError) as cm_clear:
                agy_pool.clear_log(os.path.join(protected_dir, "test.log"))
            self.assertIn("[FAIL-CLOSED TEST GUARD]", str(cm_clear.exception))

            # Verify fail-closed guard prevents configuring paths to protected path
            with self.assertRaises(RuntimeError) as cm_cfg:
                agy_pool.configure_paths(protected_dir)
            self.assertIn("[FAIL-CLOSED TEST GUARD]", str(cm_cfg.exception))

            # Verify fail-closed guard strictly protects production GEMINI_DIR
            with self.assertRaises(RuntimeError) as cm_prod:
                agy_pool._assert_safe_write_path(
                    os.path.join(agy_pool._REAL_PRODUCTION_GEMINI_DIR, "agy-pool-accounts.json")
                )
            self.assertIn("[FAIL-CLOSED TEST GUARD]", str(cm_prod.exception))

            # Verify sentinel was still never modified
            with open(sentinel_path, "r", encoding="utf-8") as f:
                self.assertEqual(json.load(f), sentinel_payload)


if __name__ == "__main__":
    unittest.main()

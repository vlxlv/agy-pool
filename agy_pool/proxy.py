"""
HTTP proxy and request forwarding handler for agy-pool.

Orchestrates account candidate selection, request reading/parsing,
header sanitization, upstream dispatch, failover handling, and
streaming/buffered response relay.
"""

from datetime import datetime
import http.client
import http.server
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from agy_pool.storage import load_pool, pool_transaction, _find_account
from agy_pool.auth import (
    refresh_token as auth_refresh_token,
    _is_validation_error,
    _extract_validation_url,
    _is_auth_error,
)
from agy_pool.accounts import display_account_name, ThreadedHTTPServer
from agy_pool import quota
from agy_pool.scheduler import order_candidates, reserve_round_robin_candidates

BACKEND_HOST = quota.BACKEND_HOST
BACKEND_URL_BASE = quota.BACKEND_URL_BASE
DEFAULT_UA = quota.DEFAULT_UA

_backend_host_provider = None
_backend_url_provider = None
_user_agent_provider = None
_token_refresher = None
_quota_refresher = None
_log_rotator = None


def set_backend_host_provider(fn):
    global _backend_host_provider
    _backend_host_provider = fn


def set_backend_url_provider(fn):
    global _backend_url_provider
    _backend_url_provider = fn


def set_user_agent_provider(fn):
    global _user_agent_provider
    _user_agent_provider = fn


def set_token_refresher(fn):
    global _token_refresher
    _token_refresher = fn


def set_quota_refresher(fn):
    global _quota_refresher
    _quota_refresher = fn


def set_log_rotator(fn):
    global _log_rotator
    _log_rotator = fn


def get_backend_host():
    if _backend_host_provider is not None:
        return _backend_host_provider()
    return BACKEND_HOST


def get_backend_url():
    if _backend_url_provider is not None:
        return _backend_url_provider()
    return BACKEND_URL_BASE


def get_user_agent():
    if _user_agent_provider is not None:
        return _user_agent_provider()
    return quota.get_user_agent()


def _do_refresh_token(account):
    if _token_refresher is not None:
        return _token_refresher(account)
    return auth_refresh_token(account)


def _do_schedule_quota_refresh(account):
    if _quota_refresher is not None:
        return _quota_refresher(account)
    return quota.schedule_quota_refresh(account)


def _maybe_rotate_log():
    if _log_rotator is not None:
        _log_rotator()


HOP_BY_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "proxy-connection", "te", "trailer", "transfer-encoding", "upgrade"
}


def _hop_by_hop_names(headers):
    names = set(HOP_BY_HOP_HEADERS)
    for value in headers.get_all("Connection", []):
        names.update(part.strip().lower() for part in value.split(",") if part.strip())
    return names


def _is_timeout_error(error):
    if isinstance(error, (socket.timeout, TimeoutError)):
        return True
    return isinstance(error, urllib.error.URLError) and isinstance(error.reason, (socket.timeout, TimeoutError))


class SmartProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.handle_proxy()

    def do_POST(self):
        self.handle_proxy()

    def _bad_request(self, message):
        self.close_connection = True
        try:
            self.send_error(400, message)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _read_exact(self, length):
        parts = []
        remaining = length
        while remaining:
            part = self.rfile.read(remaining)
            if not part:
                raise ValueError("unexpected EOF in request body")
            parts.append(part)
            remaining -= len(part)
        return b"".join(parts)

    def _read_request_body(self):
        content_lengths = self.headers.get_all("Content-Length", [])
        transfer_encoding = self.headers.get("Transfer-Encoding")
        if content_lengths and transfer_encoding:
            raise ValueError("both Content-Length and Transfer-Encoding are present")
        if len(set(content_lengths)) > 1:
            raise ValueError("conflicting Content-Length headers")
        if content_lengths:
            try:
                length = int(content_lengths[0])
            except ValueError as e:
                raise ValueError("invalid Content-Length") from e
            if length < 0:
                raise ValueError("negative Content-Length")
            return self._read_exact(length)
        if not transfer_encoding:
            return b""
        codings = [part.strip().lower() for part in transfer_encoding.split(",")]
        if codings != ["chunked"]:
            raise ValueError("unsupported Transfer-Encoding")
        chunks = []
        while True:
            line = self.rfile.readline(65537)
            if not line or len(line) > 65536 or not line.endswith(b"\r\n"):
                raise ValueError("malformed chunk header")
            try:
                size = int(line[:-2].split(b";", 1)[0], 16)
            except ValueError as e:
                raise ValueError("invalid chunk size") from e
            if size == 0:
                while True:
                    trailer = self.rfile.readline(65537)
                    if not trailer or len(trailer) > 65536 or not trailer.endswith(b"\r\n"):
                        raise ValueError("malformed chunk trailer")
                    if trailer == b"\r\n":
                        return b"".join(chunks)
            chunks.append(self._read_exact(size))
            if self._read_exact(2) != b"\r\n":
                raise ValueError("malformed chunk terminator")

    def _request_headers(self, access_token, client_ua):
        excluded = _hop_by_hop_names(self.headers) | {
            "host", "content-length", "authorization", "accept-encoding", "expect"
        }
        headers = {key: value for key, value in self.headers.items()
                   if key.lower() not in excluded}
        headers["Host"] = get_backend_host()
        headers["Authorization"] = f"Bearer {access_token}"
        headers["User-Agent"] = client_ua
        headers["Accept-Encoding"] = "identity"
        return headers

    def _response_headers(self, headers):
        excluded = _hop_by_hop_names(headers) | {"content-length", "transfer-encoding"}
        return [(key, value) for key, value in headers.items() if key.lower() not in excluded]

    def _send_buffered(self, status, headers, body):
        self.close_connection = True
        self.send_response(status)
        for key, value in self._response_headers(headers):
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _send_stream(self, status, headers, response):
        """Relay one committed stream. Errors truncate it; callers must not replay it."""
        self.close_connection = True
        try:
            self.send_response(status)
            for key, value in self._response_headers(headers):
                self.send_header(key, value)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Connection", "close")
            self.end_headers()
            expected = headers.get("Content-Length")
            expected = int(expected) if expected is not None else None
            sent = 0
            while True:
                chunk = response.read1(4096) if hasattr(response, "read1") else response.read(4096)
                if not chunk:
                    break
                sent += len(chunk)
                self.wfile.write(f"{len(chunk):X}\r\n".encode() + chunk + b"\r\n")
                self.wfile.flush()
            if expected is not None and sent != expected:
                raise http.client.IncompleteRead(b"", expected - sent)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as e:
            sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [STREAM TRUNCATED] {e}\n")
            sys.stderr.flush()

    def _record_success(self, account, is_generation=False):
        def update(pool):
            stored = _find_account(pool, account)
            if not stored:
                return
            stored["request_count"] = stored.get("request_count", 0) + 1
            if is_generation:
                stored["gen_count"] = stored.get("gen_count", 0) + 1
                stored["last_used_at"] = int(time.time())
            if stored.get("status") in ("validation_required", "auth_error"):
                stored.pop("status", None)
                stored.pop("validation_url", None)
                stored.pop("rate_limited_until", None)
            active_id = pool.get("active_account_id")
            active = next((a for a in pool.get("accounts", []) if a.get("id") == active_id), None)
            if not active_id or (active and active.get("rate_limited_until", 0) > time.time() and
                                 stored.get("id") != active.get("id")):
                pool["active_account_id"] = stored["id"]
        pool_transaction(update)

    def _record_quota_error(self, account, err_headers=None):
        delay = quota._parse_retry_after(err_headers, default=300)
        def update(pool):
            stored = _find_account(pool, account)
            if stored:
                stored["rate_limited_until"] = time.time() + delay
                q = stored.get("last_quota")
                if isinstance(q, dict):
                    q["updated_at"] = 0
                stored["error_count"] = stored.get("error_count", 0) + 1
        pool_transaction(update)
        return delay

    def _record_validation_error(self, account, err_body):
        v_url = _extract_validation_url(err_body)
        def update(pool):
            stored = _find_account(pool, account)
            if stored:
                stored["status"] = "validation_required"
                if v_url:
                    stored["validation_url"] = v_url
                stored["rate_limited_until"] = time.time() + 86400
                stored["error_count"] = stored.get("error_count", 0) + 1
                if "last_quota" in stored and isinstance(stored["last_quota"], dict):
                    stored["last_quota"]["remaining_fraction"] = 0.0
        pool_transaction(update)

    def _record_auth_error(self, account):
        def update(pool):
            stored = _find_account(pool, account)
            if stored:
                stored["status"] = "auth_error"
                stored["rate_limited_until"] = time.time() + 3600
                stored["error_count"] = stored.get("error_count", 0) + 1
                if "last_quota" in stored and isinstance(stored["last_quota"], dict):
                    stored["last_quota"]["remaining_fraction"] = 0.0
        pool_transaction(update)

    def handle_proxy(self):
        parsed = urllib.parse.urlsplit(self.path)
        path = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        try:
            body = self._read_request_body()
        except (ValueError, OSError) as e:
            self._bad_request(str(e))
            return

        # Determine if this is a streaming/generation request
        is_generation = ("generatecontent" in path.lower())
        is_sse = is_generation or ("stream" in path.lower()) or ("text/event-stream" in str(self.headers.get("Accept", "")).lower())

        _maybe_rotate_log()

        pool = load_pool()
        accounts = pool.get("accounts", [])
        if not accounts:
            try:
                self.send_error(503, "No accounts configured in agy-pool")
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        # Selection strategy
        candidates = list(accounts)
        now = time.time()
        strategy = pool.get("strategy", "max_quota")

        if is_generation:
            if strategy == "round_robin":
                candidates = reserve_round_robin_candidates(now=now)
            else:
                candidates = order_candidates(candidates, strategy=strategy, pool=pool, now=now)
        else:
            # Non-generation: prefer active account, deprioritize restricted/cooldown accounts
            active_id = pool.get("active_account_id")
            def sort_key_non_gen(acc):
                if acc.get("status") in ("validation_required", "auth_error"):
                    return 3
                if acc.get("rate_limited_until", 0) > now:
                    return 2
                return 0 if acc.get("id") == active_id else 1
            candidates.sort(key=sort_key_non_gen)

        if is_generation and candidates and quota.quota_refresh_needed(candidates[0], now=now):
            _do_schedule_quota_refresh(candidates[0])

        # Forward with failover retry loop
        last_error = None
        client_ua = self.headers.get("User-Agent", "")
        if not client_ua.startswith("antigravity/"):
            client_ua = get_user_agent()

        for acc in candidates:
            try:
                at = _do_refresh_token(acc)
            except Exception as e:
                last_error = e
                sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [PROXY WARN] Token refresh failed for {display_account_name(acc)}: {e}. Switching to next account...\n")
                sys.stderr.flush()
                self._record_auth_error(acc)
                continue

            target_url = f"{get_backend_url()}{path}"
            headers = self._request_headers(at, client_ua)

            if self.command == "POST" and len(body) == 0:
                headers["Content-Length"] = "0"
                req_data = b""
            else:
                req_data = body if body else None

            req = urllib.request.Request(target_url, data=req_data, headers=headers, method=self.command)

            try:
                with urllib.request.urlopen(req, timeout=300) as resp:
                    status = resp.status
                    sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [PROXY] {self.command} {path.split('/')[-1]} -> {display_account_name(acc)} (Status: {status})\n")
                    sys.stderr.flush()

                    resp_content_type = str(resp.headers.get("Content-Type", "")).lower()
                    is_stream = is_sse or ("text/event-stream" in resp_content_type)

                    if is_stream:
                        self._record_success(acc, is_generation=is_generation)
                        self._send_stream(status, resp.headers, resp)
                    else:
                        resp_body = resp.read()
                        self._record_success(acc, is_generation=is_generation)
                        try:
                            self._send_buffered(status, resp.headers, resp_body)
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                    return

            except urllib.error.HTTPError as e:
                last_error = e
                err_body = b""
                try:
                    err_body = e.read()
                except Exception:
                    pass
                finally:
                    e.close()
                sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [PROXY ERROR] {self.command} {path.split('/')[-1]} -> {display_account_name(acc)} HTTP {e.code}\n")
                sys.stderr.flush()

                # Failover on Google Security Validation Required
                if _is_validation_error(e.code, err_body):
                    self._record_validation_error(acc, err_body)
                    sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [FAILOVER] Account {display_account_name(acc)} requires account verification! Auto-switching to next account...\n")
                    sys.stderr.flush()
                    continue

                # Failover on 429 (ResourceExhausted / Quota Exceeded) or 403 quota exhaustion
                elif quota._is_quota_error(e.code, err_body):
                    delay = self._record_quota_error(acc, e.headers)
                    sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [FAILOVER] Account {display_account_name(acc)} hit rate limit/quota error! Cooldown {delay}s. Auto-switching to next account...\n")
                    sys.stderr.flush()
                    continue

                # Failover on 401 Unauthenticated or disabled account
                elif _is_auth_error(e.code, err_body):
                    self._record_auth_error(acc)
                    sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [FAILOVER] Account {display_account_name(acc)} auth/permission failure! Auto-switching to next account...\n")
                    sys.stderr.flush()
                    continue

                else:
                    try:
                        self._send_buffered(e.code, e.headers, err_body)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    return

            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as e:
                last_error = e
                sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [PROXY EXCEPTION] {self.command} {path.split('/')[-1]} -> {display_account_name(acc)}: {e}\n")
                sys.stderr.flush()
                try:
                    self.close_connection = True
                    self.send_error(504 if _is_timeout_error(e) else 502,
                                    "Upstream request failed")
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

        # If all candidates exhausted
        sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ALL EXHAUSTED] {self.command} {path.split('/')[-1]} failed across all accounts: {last_error}\n")
        sys.stderr.flush()
        try:
            self.close_connection = True
            self.send_error(503, f"All accounts in pool exhausted or unavailable: {last_error}")
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, format, *args):
        pass


_record_success = SmartProxyHandler._record_success
_record_quota_error = SmartProxyHandler._record_quota_error
_record_validation_error = SmartProxyHandler._record_validation_error
_record_auth_error = SmartProxyHandler._record_auth_error

__all__ = [
    "HOP_BY_HOP_HEADERS",
    "SmartProxyHandler",
    "ThreadedHTTPServer",
    "_hop_by_hop_names",
    "_is_timeout_error",
    "_record_success",
    "_record_quota_error",
    "_record_validation_error",
    "_record_auth_error",
]

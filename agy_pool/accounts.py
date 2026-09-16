"""
Account lifecycle, target matching, login flow, import/export,
and token synchronization for agy-pool.
"""

import os
import sys
import time
import json
import base64
import hashlib
import hmac
import secrets
import getpass
import urllib.request
import urllib.parse
import urllib.error
import http.server
import socketserver
import socket
import threading
import subprocess
import shutil
from datetime import datetime, timezone

from agy_pool import config
from agy_pool import storage
from agy_pool import auth

_quota_prober = None
_quota_formatter = None


def set_quota_prober(prober, formatter=None):
    """Set optional callback to probe/format quota during account management operations."""
    global _quota_prober, _quota_formatter
    _quota_prober = prober
    if formatter:
        _quota_formatter = formatter


def display_account_name(account):
    """
    Returns the user-visible friendly display name for an account.
    Never falls back to real email or values derived from email.
    Fallback order:
      1. Explicit friendly name / label ('name' field)
      2. 'Account N' if account id matches 'acc_N'
      3. 'Account' as generic safe fallback
    """
    if not isinstance(account, dict):
        return "Account"

    friendly = str(account.get("name") or "").strip()
    if friendly:
        return friendly

    account_id = str(account.get("id") or "").strip()
    if account_id.startswith("acc_"):
        suffix = account_id[4:]
        if suffix.isdigit():
            return f"Account {int(suffix)}"

    return "Account"


def find_account_by_target(accounts, target):
    """Resolve target selector (1-based index, account ID, exact email, or friendly name)."""
    if target is None:
        return None
    target_str = str(target).strip()
    if target_str.isdigit():
        idx = int(target_str) - 1
        if 0 <= idx < len(accounts):
            return accounts[idx]
        return None
    return next(
        (a for a in accounts if a.get("id") == target_str or a.get("email") == target_str or a.get("name") == target_str),
        None
    )


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    timeout = 5.0


class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    auth_code = None
    error = None
    timeout = 5.0

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/auth/callback") or parsed.path == "/":
            query = urllib.parse.parse_qs(parsed.query)
            if "code" in query:
                OAuthCallbackHandler.auth_code = query["code"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Connection", "close")
                html = """
                <html><head><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
                <body style="font-family: sans-serif; text-align: center; padding: 40px; background: #0f172a; color: #f8fafc;">
                    <h2 style="color: #4ade80;">Authorization Successful!</h2>
                    <p>Antigravity multi-account token has been saved.</p>
                    <p>You can close this window and return to Termux.</p>
                </body></html>
                """
                body = html.strip().encode("utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif "error" in query:
                OAuthCallbackHandler.error = query["error"][0]
                self.send_response(400)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(b"Authorization failed.")
        else:
            self.send_response(404)
            self.send_header("Connection", "close")
            self.end_headers()

    def log_message(self, format, *args):
        pass


def find_free_port(start_port=8085):
    for p in range(start_port, start_port + 50):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind(("127.0.0.1", p))
                return p
        except OSError:
            continue
    return start_port


def do_login():
    print(f"\n{config.CLR_BOLD}{config.CLR_CYAN}=== Antigravity Multi-Account Login ==={config.CLR_RESET}\n")
    OAuthCallbackHandler.auth_code = None
    OAuthCallbackHandler.error = None

    port = find_free_port()
    redirect_uri = f"http://localhost:{port}/auth/callback"

    params = {
        "client_id": auth.CLIENT_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": auth.OAUTH_SCOPES,
        "access_type": "offline",
        "prompt": "consent"
    }
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)

    server = ThreadedHTTPServer(("127.0.0.1", port), OAuthCallbackHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    print(f"{config.CLR_YELLOW}1. Launching browser for Google Authorization...{config.CLR_RESET}")
    opened = False
    for opener in ["termux-open-url", "termux-open", "xdg-open", "open"]:
        if shutil.which(opener):
            try:
                subprocess.Popen([opener, auth_url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                opened = True
                break
            except Exception:
                pass

    print(f"\n{config.CLR_DIM}If the browser did not open automatically, copy and open this URL:{config.CLR_RESET}")
    print(f"{config.CLR_BLUE}{auth_url}{config.CLR_RESET}\n")
    print(f"{config.CLR_YELLOW}Waiting for login callback on {redirect_uri}...{config.CLR_RESET}")
    print(f"{config.CLR_DIM}(Or paste the redirect URL / code here if in headless/SSH mode):{config.CLR_RESET} ", end="", flush=True)

    manual_input = []

    def wait_input():
        try:
            line = sys.stdin.readline().strip()
            if line:
                manual_input.append(line)
        except Exception:
            pass

    in_thread = threading.Thread(target=wait_input, daemon=True)
    in_thread.start()

    code = None
    start_wait = time.time()
    while time.time() - start_wait < 180:
        if OAuthCallbackHandler.auth_code:
            code = OAuthCallbackHandler.auth_code
            print(f"\n{config.CLR_GREEN}✓ Received OAuth callback from browser!{config.CLR_RESET}")
            break
        if manual_input:
            user_text = manual_input[0]
            if "code=" in user_text:
                q = urllib.parse.parse_qs(urllib.parse.urlparse(user_text).query)
                code = q.get("code", [None])[0]
            else:
                code = user_text
            break
        time.sleep(0.5)

    def stop_server():
        try:
            server.shutdown()
            server.server_close()
        except Exception:
            pass

    threading.Thread(target=stop_server, daemon=True).start()

    if not code:
        print(f"\n{config.CLR_RED}[Error] Login timed out or cancelled.{config.CLR_RESET}")
        return False

    print(f"{config.CLR_CYAN}Exchanging code for tokens...{config.CLR_RESET}")
    token_params = {
        "code": code,
        "client_id": auth.CLIENT_ID,
        "client_secret": auth.CLIENT_SECRET,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code"
    }
    data = urllib.parse.urlencode(token_params).encode()
    req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            token_res = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        print(f"{config.CLR_RED}[Error] OAuth exchange failed: {e.read().decode()}{config.CLR_RESET}")
        return False

    refresh_tok = token_res.get("refresh_token")
    access_tok = token_res.get("access_token")
    id_tok = token_res.get("id_token", "")
    expires_in = token_res.get("expires_in", 3600)

    if not refresh_tok:
        print(f"{config.CLR_RED}[Error] Google did not return a refresh_token. Please revoke access or re-run with prompt=consent.{config.CLR_RESET}")
        return False

    jwt_payload = auth.decode_jwt_payload(id_tok)
    email = jwt_payload.get("email") or f"user_{int(time.time())}@gmail.com"

    account_data = {
        "email": email,
        "refresh_token": refresh_tok,
        "access_token": access_tok,
        "token_expiry": time.time() + expires_in,
        "id_token": id_tok,
        "updated_at": int(time.time())
    }
    created = [False]

    def upsert(pool):
        existing = next((acc for acc in pool["accounts"] if acc.get("email") == email), None)
        if existing:
            existing.update(account_data)
            existing.pop("status", None)
            existing.pop("validation_url", None)
            existing.pop("rate_limited_until", None)
            return dict(existing)
        created[0] = True
        entry = dict(account_data, id=storage._next_account_id(pool["accounts"]), name=None, request_count=0,
                     gen_count=0, error_count=0, created_at=int(time.time()), last_quota={})
        pool["accounts"].append(entry)
        if not pool.get("active_account_id"):
            pool["active_account_id"] = entry["id"]
        return dict(entry)

    account_entry = storage.pool_transaction(upsert)
    disp_name = display_account_name(account_entry)
    if not created[0]:
        print(f"{config.CLR_GREEN}✓ Updated existing account: {config.CLR_BOLD}{disp_name}{config.CLR_RESET}")
    else:
        print(f"{config.CLR_GREEN}✓ Added new account: {config.CLR_BOLD}{disp_name}{config.CLR_RESET}")

    # Fetch initial quota if prober registered
    print(f"{config.CLR_CYAN}Probing quota for {disp_name}...{config.CLR_RESET}")
    if _quota_prober:
        try:
            q = _quota_prober(account_entry)
            auth._persist_account_fields(account_entry, auth.REFRESH_PERSIST_FIELDS)
            if _quota_formatter:
                _quota_formatter(q)
        except Exception as e:
            print(f"  {config.CLR_YELLOW}Quota probe warning: {e}{config.CLR_RESET}")

    sync_active_agy_token_file()

    print(f"\n{config.CLR_GREEN}{config.CLR_BOLD}Success! Total accounts in pool: {len(storage.load_pool()['accounts'])}{config.CLR_RESET}\n")
    return True


def import_current():
    """Imports the current ~/.gemini/antigravity-cli/antigravity-oauth-token into pool."""
    if not os.path.exists(config.AGY_TOKEN_FILE):
        print(f"{config.CLR_YELLOW}No existing {config.AGY_TOKEN_FILE} found to import.{config.CLR_RESET}")
        return False

    try:
        with open(config.AGY_TOKEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        tok = data.get("token", {})
        rf = tok.get("refresh_token")
        if not rf:
            print(f"{config.CLR_YELLOW}No refresh_token found in existing token file.{config.CLR_RESET}")
            return False

        id_tok = data.get("id_token", "")
        payload = auth.decode_jwt_payload(id_tok)
        email = payload.get("email") or "primary_user@gmail.com"

        account_data = {
            "email": email,
            "refresh_token": rf,
            "access_token": tok.get("access_token"),
            "token_expiry": 0,
            "id_token": id_tok,
        }

        def upsert(pool):
            existing = next((a for a in pool["accounts"] if a.get("email") == email), None)
            if existing:
                existing.update(account_data)
                return dict(existing)
            acc = dict(account_data, id=storage._next_account_id(pool["accounts"]), name=None, request_count=0,
                       gen_count=0, error_count=0, created_at=int(time.time()), last_quota={})
            pool["accounts"].append(acc)
            if not pool.get("active_account_id"):
                pool["active_account_id"] = acc["id"]
            return dict(acc)

        acc = storage.pool_transaction(upsert)

        if _quota_prober:
            try:
                _quota_prober(acc)
                auth._persist_account_fields(acc, auth.TOKEN_FIELDS + ("last_quota",))
            except Exception:
                pass

        print(f"{config.CLR_GREEN}✓ Successfully imported existing account: {config.CLR_BOLD}{display_account_name(acc)}{config.CLR_RESET}")
        return True
    except Exception as e:
        print(f"{config.CLR_RED}[Error] Failed to import: {e}{config.CLR_RESET}")
        return False


def write_agy_token_file(account):
    """Writes the given account credentials into ~/.gemini/antigravity-cli/antigravity-oauth-token."""
    storage.ensure_dirs()
    payload = {
        "token": {
            "access_token": account.get("access_token", ""),
            "token_type": "Bearer",
            "refresh_token": account.get("refresh_token", ""),
            "expiry": datetime.fromtimestamp(account.get("token_expiry", time.time() + 3600), timezone.utc).isoformat()
        },
        "auth_method": "consumer",
        "id_token": account.get("id_token", "")
    }
    with storage._file_lock(config.AGY_TOKEN_FILE + ".lock"):
        storage._atomic_json_write(config.AGY_TOKEN_FILE, payload)


def sync_active_agy_token_file():
    """Synchronize native agy's compatibility token with the current active account."""
    with storage._file_lock(config.AGY_TOKEN_FILE + ".lock"):
        for _ in range(3):
            pool = storage.load_pool()
            active_id = pool.get("active_account_id")
            account = next((a for a in pool.get("accounts", []) if a.get("id") == active_id), None)
            if not account:
                return False
            auth.refresh_token(account)
            if storage.load_pool().get("active_account_id") == active_id:
                payload = {
                    "token": {
                        "access_token": account.get("access_token", ""),
                        "token_type": "Bearer",
                        "refresh_token": account.get("refresh_token", ""),
                        "expiry": datetime.fromtimestamp(account.get("token_expiry", time.time() + 3600), timezone.utc).isoformat()
                    },
                    "auth_method": "consumer",
                    "id_token": account.get("id_token", "")
                }
                storage._atomic_json_write(config.AGY_TOKEN_FILE, payload)
                return True
    return False


def remove_account(target):
    def remove(pool):
        accounts = pool.get("accounts", [])
        found = find_account_by_target(accounts, target)
        if not found:
            return None
        pool["accounts"] = [a for a in accounts if a.get("id") != found.get("id")]
        if pool.get("active_account_id") == found.get("id"):
            pool["active_account_id"] = pool["accounts"][0]["id"] if pool["accounts"] else None
        return dict(found)

    found = storage.pool_transaction(remove)
    if not found:
        print(f"{config.CLR_RED}[Error] Account '{target}' not found.{config.CLR_RESET}")
        return False
    try:
        sync_active_agy_token_file()
    except Exception:
        pass
    print(f"{config.CLR_GREEN}✓ Removed account: {display_account_name(found)}{config.CLR_RESET}")
    return True


def switch_account(target=None, silent=False):
    pool = storage.load_pool()
    accounts = pool.get("accounts", [])
    if not accounts:
        if not silent:
            print(f"{config.CLR_RED}[Error] Account pool is empty.{config.CLR_RESET}")
        return False

    if (target is None or target == "auto") and _quota_prober:
        if not silent:
            print(f"{config.CLR_CYAN}Auto-selecting account with highest quota...{config.CLR_RESET}")
        for acc in accounts:
            try:
                _quota_prober(acc)
                auth._persist_account_fields(acc, auth.REFRESH_PERSIST_FIELDS)
            except Exception:
                pass

    def select(pool):
        choices = pool.get("accounts", [])
        selected = None
        if target is None or target == "auto":
            now_ts = time.time()

            def auto_sort_key(acc):
                if acc.get("status") in ("validation_required", "auth_error"):
                    return (-2, 0.0)
                if acc.get("rate_limited_until", 0) > now_ts:
                    return (-1, 0.0)
                return (1, acc.get("last_quota", {}).get("remaining_fraction", 0.0))

            choices = sorted(choices, key=auto_sort_key, reverse=True)
            selected = choices[0] if choices else None
        else:
            selected = find_account_by_target(choices, target)
        if selected:
            pool["active_account_id"] = selected["id"]
            return dict(selected)
        return None

    selected = storage.pool_transaction(select)

    if not selected:
        if not silent:
            print(f"{config.CLR_RED}[Error] Account '{target}' not found.{config.CLR_RESET}")
        return False

    try:
        auth.refresh_token(selected)
    except Exception:
        pass
    try:
        sync_active_agy_token_file()
    except Exception:
        pass

    if not silent:
        print(f"{config.CLR_GREEN}✓ Active account switched to: {config.CLR_BOLD}{display_account_name(selected)}{config.CLR_RESET}")
        if _quota_formatter:
            _quota_formatter(selected.get("last_quota", {}))
    return True


def rename_account(target, new_name):
    new_name = str(new_name).strip()
    if not new_name:
        print(f"{config.CLR_RED}[Error] Account name cannot be empty.{config.CLR_RESET}")
        return False

    def mutate(pool):
        accounts = pool.get("accounts", [])
        found = find_account_by_target(accounts, target)
        if not found:
            return None
        found["name"] = new_name
        return dict(found)

    found = storage.pool_transaction(mutate)
    if not found:
        print(f"{config.CLR_RED}[Error] Account '{target}' not found.{config.CLR_RESET}")
        return False
    print(f"{config.CLR_GREEN}✓ Renamed account [{found.get('id')}] to: {config.CLR_BOLD}{new_name}{config.CLR_RESET}")
    return True


def do_verify(target=None):
    pool = storage.load_pool()
    accounts = pool.get("accounts", [])
    if not accounts:
        print(f"{config.CLR_RED}[Error] Account pool is empty.{config.CLR_RESET}")
        return False

    target_acc = None
    target_idx = None
    if target is not None:
        target_acc = find_account_by_target(accounts, target)
        if target_acc:
            target_idx = accounts.index(target_acc) + 1
    else:
        for idx, a in enumerate(accounts):
            if a.get("status") == "validation_required":
                target_acc = a
                target_idx = idx + 1
                break
        if not target_acc and accounts:
            target_acc = accounts[0]
            target_idx = 1

    if not target_acc:
        print(f"{config.CLR_RED}[Error] Account not found.{config.CLR_RESET}")
        return False

    email = target_acc.get("email")
    disp_target = display_account_name(target_acc)
    print(f"\n{config.CLR_BOLD}{config.CLR_CYAN}=== Google Security Verification ==={config.CLR_RESET}\n")
    print(f"Target Account: {config.CLR_BOLD}[{target_idx}] {disp_target}{config.CLR_RESET}")
    print(f"{config.CLR_YELLOW}Probing latest verification status and URL from Google Cloud Code...{config.CLR_RESET}")

    if _quota_prober:
        try:
            _quota_prober(target_acc)
        except Exception:
            pass

    refreshed_pool = storage.load_pool()
    refreshed_acc = next((a for a in refreshed_pool.get("accounts", []) if a.get("id") == target_acc.get("id")), target_acc)

    if refreshed_acc.get("status") != "validation_required":
        print(f"{config.CLR_GREEN}✓ Account {display_account_name(refreshed_acc)} is verified and fully operational! No verification required.{config.CLR_RESET}\n")
        return True

    v_url = refreshed_acc.get("validation_url")
    if not v_url:
        print(f"{config.CLR_YELLOW}Google did not return a validation URL. Please try logging in again via 'agy-pool login'.{config.CLR_RESET}\n")
        return False

    if email and "&Email=" not in v_url and "&login_hint=" not in v_url:
        v_url += f"&Email={urllib.parse.quote(email)}"

    print(f"\n{config.CLR_GREEN}✓ Security verification URL retrieved:{config.CLR_RESET}")
    print(f"\n{config.CLR_BLUE}{v_url}{config.CLR_RESET}\n")

    opened = False
    for opener in ["termux-open-url", "termux-open", "xdg-open", "open"]:
        if shutil.which(opener):
            try:
                subprocess.Popen([opener, v_url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                opened = True
                print(f"{config.CLR_GREEN}✓ Launched browser via {opener}.{config.CLR_RESET}")
                break
            except Exception:
                pass

    if not opened:
        print(f"{config.CLR_YELLOW}Could not open browser automatically. Please copy the URL above and open it manually.{config.CLR_RESET}")

    print(f"\n{config.CLR_BOLD}Instructions:{config.CLR_RESET}")
    print(f"  1. In the browser, make sure you are logged into {config.CLR_BOLD}{disp_target}{config.CLR_RESET}.")
    print(f"  2. Complete the Google account verification on screen.")
    print(f"  3. Wait until redirected to 'auth_success_gemini' (Verification Successful).")
    print()

    if sys.stdin.isatty():
        try:
            input(f"Press {config.CLR_BOLD}[Enter]{config.CLR_RESET} here once verification is completed in browser...")
        except (KeyboardInterrupt, EOFError):
            print()
            return False

        print(f"\n{config.CLR_CYAN}Re-probing quota for {disp_target}...{config.CLR_RESET}")
        if _quota_prober:
            try:
                _quota_prober(refreshed_acc)
            except Exception:
                pass
        final_pool = storage.load_pool()
        final_acc = next((a for a in final_pool.get("accounts", []) if a.get("id") == target_acc.get("id")), target_acc)
        if final_acc.get("status") != "validation_required":
            print(f"{config.CLR_GREEN}{config.CLR_BOLD}🎉 Verification Successful! Account {display_account_name(final_acc)} is now fully operational!{config.CLR_RESET}\n")
        else:
            print(f"{config.CLR_YELLOW}Account still appears restricted. If you just completed verification, wait a few moments and run 'agy-pool quota'.{config.CLR_RESET}\n")

    return True


def encrypt_bundle(data_bytes, password):
    salt = secrets.token_bytes(16)
    nonce = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100000, dklen=64)
    key_enc = dk[:32]
    key_mac = dk[32:]

    ciphertext = bytearray(len(data_bytes))
    block_size = 32
    num_blocks = (len(data_bytes) + block_size - 1) // block_size
    for i in range(num_blocks):
        ctr = nonce + i.to_bytes(8, "big")
        ks_block = hmac.new(key_enc, ctr, hashlib.sha256).digest()
        start = i * block_size
        end = min(start + block_size, len(data_bytes))
        for j in range(start, end):
            ciphertext[j] = data_bytes[j] ^ ks_block[j - start]

    tag = hmac.new(key_mac, salt + nonce + bytes(ciphertext), hashlib.sha256).digest()
    return {
        "format": "agy-pool-encrypted-v1",
        "kdf": "pbkdf2_hmac_sha256",
        "iterations": 100000,
        "salt": base64.b64encode(salt).decode("ascii"),
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
        "tag": base64.b64encode(tag).decode("ascii")
    }


def decrypt_bundle(payload, password):
    if not isinstance(payload, dict) or payload.get("format") != "agy-pool-encrypted-v1":
        raise ValueError("Unsupported or invalid encrypted bundle format")
    try:
        salt = base64.b64decode(payload["salt"])
        nonce = base64.b64decode(payload["nonce"])
        ciphertext = base64.b64decode(payload["ciphertext"])
        tag = base64.b64decode(payload["tag"])
        iterations = int(payload.get("iterations", 100000))
    except Exception as e:
        raise ValueError(f"Corrupted encrypted bundle metadata: {e}")

    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=64)
    key_enc = dk[:32]
    key_mac = dk[32:]

    expected_tag = hmac.new(key_mac, salt + nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(tag, expected_tag):
        raise ValueError("Invalid password or corrupted backup payload")

    plaintext = bytearray(len(ciphertext))
    block_size = 32
    num_blocks = (len(ciphertext) + block_size - 1) // block_size
    for i in range(num_blocks):
        ctr = nonce + i.to_bytes(8, "big")
        ks_block = hmac.new(key_enc, ctr, hashlib.sha256).digest()
        start = i * block_size
        end = min(start + block_size, len(ciphertext))
        for j in range(start, end):
            plaintext[j] = ciphertext[j] ^ ks_block[j - start]

    return bytes(plaintext)


def export_pool(file_path=None, encrypt=False, password=None, no_stats=False):
    pool = storage.load_pool()
    accounts = pool.get("accounts", [])
    if not accounts:
        print(f"\n{config.CLR_YELLOW}No accounts in pool to export.{config.CLR_RESET}")
        return False

    export_accounts = []
    for acc in accounts:
        item = {
            "email": acc.get("email"),
            "name": acc.get("name"),
            "refresh_token": acc.get("refresh_token"),
            "access_token": acc.get("access_token"),
            "token_expiry": acc.get("token_expiry"),
            "created_at": acc.get("created_at"),
        }
        if not no_stats:
            item["request_count"] = acc.get("request_count", 0)
            item["gen_count"] = acc.get("gen_count", 0)
        if acc.get("last_quota"):
            item["last_quota"] = acc.get("last_quota")
        export_accounts.append(item)

    active_email = None
    active_id = pool.get("active_account_id")
    for acc in accounts:
        if acc.get("id") == active_id:
            active_email = acc.get("email")
            break

    bundle = {
        "version": 1,
        "app": "agy-pool",
        "exported_at": int(time.time()),
        "strategy": pool.get("strategy", "max_quota"),
        "active_account_email": active_email,
        "accounts": export_accounts,
    }

    raw_json = json.dumps(bundle, indent=2, ensure_ascii=False)
    is_encrypt = encrypt or bool(password)

    if is_encrypt:
        if not password:
            if not sys.stdin.isatty():
                sys.stderr.write(f"{config.CLR_RED}[Error] Password required when encrypting in non-interactive mode. Use --password <pass>.{config.CLR_RESET}\n")
                return False
            while True:
                p1 = getpass.getpass("Enter passphrase to encrypt backup: ")
                if not p1:
                    print(f"{config.CLR_RED}Passphrase cannot be empty.{config.CLR_RESET}")
                    continue
                p2 = getpass.getpass("Confirm passphrase: ")
                if p1 != p2:
                    print(f"{config.CLR_RED}Passphrases do not match. Try again.{config.CLR_RESET}")
                    continue
                password = p1
                break
        encrypted_data = encrypt_bundle(raw_json.encode("utf-8"), password)
        output_content = json.dumps(encrypted_data, indent=2)
    else:
        output_content = raw_json

    if file_path == "-":
        sys.stdout.write(output_content + "\n")
        return True

    if not file_path:
        ext = ".enc" if is_encrypt else ".json"
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"agy-pool-backup-{date_str}{ext}"

    directory = os.path.dirname(os.path.abspath(file_path))
    config._assert_safe_write_path(file_path)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    fd = os.open(file_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(output_content + "\n")

    enc_label = f" ({config.CLR_GREEN}Encrypted with passphrase{config.CLR_RESET})" if is_encrypt else ""
    print(f"\n{config.CLR_GREEN}✓ Successfully exported {len(export_accounts)} account(s) to:{config.CLR_RESET}")
    print(f"  {config.CLR_BOLD}{file_path}{config.CLR_RESET}{enc_label}")
    print(f"{config.CLR_DIM}  File permissions: 0600 (Restricted to current user).{config.CLR_RESET}\n")
    return True


def import_pool(file_path, password=None, replace=False, skip_existing=False):
    if file_path == "-":
        content = sys.stdin.read()
    else:
        if not os.path.exists(file_path):
            sys.stderr.write(f"{config.CLR_RED}[Error] Backup file '{file_path}' does not exist.{config.CLR_RESET}\n")
            return False
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

    try:
        raw_obj = json.loads(content)
    except Exception as e:
        sys.stderr.write(f"{config.CLR_RED}[Error] Failed to parse backup file as JSON: {e}{config.CLR_RESET}\n")
        return False

    if isinstance(raw_obj, dict) and raw_obj.get("format") == "agy-pool-encrypted-v1":
        if not password:
            if not sys.stdin.isatty():
                sys.stderr.write(f"{config.CLR_RED}[Error] Password required to decrypt backup in non-interactive mode. Use --password <pass>.{config.CLR_RESET}\n")
                return False
            password = getpass.getpass("Enter passphrase to decrypt backup: ")
        try:
            plaintext = decrypt_bundle(raw_obj, password)
            raw_obj = json.loads(plaintext.decode("utf-8"))
        except Exception as e:
            sys.stderr.write(f"{config.CLR_RED}[Error] Decryption failed: {e}{config.CLR_RESET}\n")
            return False

    if not isinstance(raw_obj, dict) or "accounts" not in raw_obj or not isinstance(raw_obj["accounts"], list):
        sys.stderr.write(f"{config.CLR_RED}[Error] Invalid backup format: missing 'accounts' list.{config.CLR_RESET}\n")
        return False

    imported_accounts = raw_obj["accounts"]
    if not imported_accounts:
        print(f"{config.CLR_YELLOW}Backup contains no accounts. Nothing imported.{config.CLR_RESET}")
        return False

    imported_strategy = raw_obj.get("strategy")
    target_active_email = raw_obj.get("active_account_email")

    def mutate(pool):
        nonlocal target_active_email
        existing_accounts = pool.get("accounts", [])
        added_count = 0
        updated_count = 0
        skipped_count = 0

        if replace:
            new_list = []
            for i, acc in enumerate(imported_accounts, start=1):
                email = acc.get("email")
                if not email or not acc.get("refresh_token"):
                    continue
                new_acc = {
                    "id": f"acc_{i}",
                    "email": email,
                    "name": acc.get("name"),
                    "refresh_token": acc["refresh_token"],
                    "access_token": acc.get("access_token"),
                    "token_expiry": acc.get("token_expiry", 0),
                    "created_at": acc.get("created_at", int(time.time())),
                    "request_count": acc.get("request_count", 0),
                    "gen_count": acc.get("gen_count", 0),
                    "error_count": 0,
                    "last_quota": acc.get("last_quota", {}),
                }
                new_list.append(new_acc)
                added_count += 1
            pool["accounts"] = new_list
            if imported_strategy:
                pool["strategy"] = imported_strategy
            active_acc = next((a for a in new_list if a.get("email") == target_active_email), None)
            if not active_acc and new_list:
                active_acc = new_list[0]
            pool["active_account_id"] = active_acc["id"] if active_acc else None
            return (added_count, updated_count, skipped_count, pool.get("active_account_id"))

        email_map = {a.get("email"): a for a in existing_accounts if a.get("email")}

        for acc in imported_accounts:
            email = acc.get("email")
            if not email or not acc.get("refresh_token"):
                continue
            if email in email_map:
                if skip_existing:
                    skipped_count += 1
                    continue
                target = email_map[email]
                target["refresh_token"] = acc["refresh_token"]
                if acc.get("access_token"):
                    target["access_token"] = acc["access_token"]
                    target["token_expiry"] = acc.get("token_expiry", 0)
                if acc.get("name") and not target.get("name"):
                    target["name"] = acc.get("name")
                target.pop("status", None)
                target.pop("rate_limited_until", None)
                updated_count += 1
            else:
                next_id = storage._next_account_id(existing_accounts)
                new_acc = {
                    "id": next_id,
                    "email": email,
                    "name": acc.get("name"),
                    "refresh_token": acc["refresh_token"],
                    "access_token": acc.get("access_token"),
                    "token_expiry": acc.get("token_expiry", 0),
                    "created_at": acc.get("created_at", int(time.time())),
                    "request_count": acc.get("request_count", 0),
                    "gen_count": acc.get("gen_count", 0),
                    "error_count": 0,
                    "last_quota": acc.get("last_quota", {}),
                }
                existing_accounts.append(new_acc)
                email_map[email] = new_acc
                added_count += 1

        return (added_count, updated_count, skipped_count, pool.get("active_account_id"))

    res = storage.pool_transaction(mutate)
    if not res:
        print(f"{config.CLR_RED}[Error] Import failed.{config.CLR_RESET}")
        return False
    added, updated, skipped, active_id = res
    print(f"\n{config.CLR_GREEN}✓ Pool import completed successfully!{config.CLR_RESET}")
    print(f"  • Added   : {config.CLR_BOLD}{added}{config.CLR_RESET}")
    print(f"  • Updated : {config.CLR_BOLD}{updated}{config.CLR_RESET}")
    if skipped:
        print(f"  • Skipped : {config.CLR_BOLD}{skipped}{config.CLR_RESET}")
    print(f"  • Total   : {config.CLR_BOLD}{len(storage.load_pool().get('accounts', []))}{config.CLR_RESET} accounts\n")
    return True

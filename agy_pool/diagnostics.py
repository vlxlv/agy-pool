"""
agy_pool.diagnostics: System health and environment diagnostics for agy-pool.

Extracted in CP6A of Python modularization.
"""

import contextlib
import json
import os
import platform
import shutil
import socket
import sqlite3
import ssl
import subprocess
import sys
import time
import urllib.parse

from agy_pool import config
from agy_pool import storage
from agy_pool import accounts
from agy_pool import quota
from agy_pool import daemon


# ----------------- Dynamic Providers / Hooks -----------------
_agy_binary_finder = None
_agy_version_provider = None
_daemon_info_getter = None
_port_listener = None
_daemon_outdated_checker = None
_load_pool_fn = None
_pool_config_file_provider = None
_backend_host_provider = None
_agy_cli_dir_provider = None
_log_file_provider = None
_max_log_bytes_provider = None
_display_account_name_fn = None


def set_agy_binary_finder(fn):
    global _agy_binary_finder
    _agy_binary_finder = fn


def set_agy_version_provider(fn):
    global _agy_version_provider
    _agy_version_provider = fn


def set_daemon_info_getter(fn):
    global _daemon_info_getter
    _daemon_info_getter = fn


def set_port_listener(fn):
    global _port_listener
    _port_listener = fn


def set_daemon_outdated_checker(fn):
    global _daemon_outdated_checker
    _daemon_outdated_checker = fn


def set_load_pool_fn(fn):
    global _load_pool_fn
    _load_pool_fn = fn


def set_pool_config_file_provider(fn):
    global _pool_config_file_provider
    _pool_config_file_provider = fn


def set_backend_host_provider(fn):
    global _backend_host_provider
    _backend_host_provider = fn


def set_agy_cli_dir_provider(fn):
    global _agy_cli_dir_provider
    _agy_cli_dir_provider = fn


def set_log_file_provider(fn):
    global _log_file_provider
    _log_file_provider = fn


def set_max_log_bytes_provider(fn):
    global _max_log_bytes_provider
    _max_log_bytes_provider = fn


def set_display_account_name_fn(fn):
    global _display_account_name_fn
    _display_account_name_fn = fn


def _default_find_real_agy_binary():
    if os.environ.get("AGY_BIN") and os.path.exists(os.environ["AGY_BIN"]):
        return os.path.abspath(os.environ["AGY_BIN"])

    prefix = os.environ.get("PREFIX")
    candidates = []
    if prefix:
        candidates.append(os.path.join(prefix, "bin", "agy"))

    candidates.extend([
        "/data/data/com.termux/files/usr/bin/agy",
        "/usr/local/bin/agy",
        "/usr/bin/agy",
        os.path.expanduser("~/.local/bin/agy"),
    ])

    p = shutil.which("agy")
    if p:
        candidates.append(p)

    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return None


def _default_get_installed_agy_version():
    finder = _agy_binary_finder or _default_find_real_agy_binary
    agy_bin = finder()
    if agy_bin:
        try:
            p = subprocess.run([agy_bin, "--version"], capture_output=True, text=True, timeout=2)
            v = p.stdout.strip()
            if v and "." in v:
                return v
        except Exception:
            pass
    return "1.2.2"


def _get_agy_binary():
    if _agy_binary_finder is not None:
        return _agy_binary_finder()
    return _default_find_real_agy_binary()


def _get_installed_agy_version():
    if _agy_version_provider is not None:
        return _agy_version_provider()
    return _default_get_installed_agy_version()


def _get_daemon_info():
    if _daemon_info_getter is not None:
        return _daemon_info_getter()
    return daemon.get_daemon_info()


def _get_port_listening():
    if _port_listener is not None:
        return _port_listener()
    return daemon.is_port_listening()


def _get_daemon_outdated():
    if _daemon_outdated_checker is not None:
        return _daemon_outdated_checker()
    return daemon.is_daemon_outdated()


def _get_load_pool():
    return _load_pool_fn or storage.load_pool


def _get_pool_config_file():
    if _pool_config_file_provider is not None:
        return _pool_config_file_provider()
    return config.POOL_CONFIG_FILE


def _get_backend_host():
    if _backend_host_provider is not None:
        return _backend_host_provider()
    return quota.BACKEND_HOST


def _get_agy_cli_dir():
    if _agy_cli_dir_provider is not None:
        return _agy_cli_dir_provider()
    return config.AGY_CLI_DIR


def _get_log_file():
    if _log_file_provider is not None:
        return _log_file_provider()
    return config.LOG_FILE


def _get_max_log_bytes():
    if _max_log_bytes_provider is not None:
        return _max_log_bytes_provider()
    return config.MAX_LOG_BYTES


def _get_display_account_name():
    return _display_account_name_fn or accounts.display_account_name


def run_doctor():
    """Performs end-to-end system diagnostic checks across runtime, network, and pool state."""
    sep = "=" * 68
    print("\n" + sep)
    title = f"Antigravity System Doctor (agy-pool v{config.VERSION})"
    print(f"{config.CLR_BOLD}{title:^68}{config.CLR_RESET}")
    print(sep + "\n")

    issues_found = 0
    warnings_found = 0

    # 1. Python Environment & Concurrency
    py_ver = f"{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}"
    if sys.version_info >= (3, 8):
        print(f"  {config.CLR_GREEN}✓{config.CLR_RESET} Python Runtime: {config.CLR_BOLD}{py_ver}{config.CLR_RESET} ({platform.system()} {platform.machine()})")
    else:
        print(f"  {config.CLR_RED}✖{config.CLR_RESET} Python Runtime: {config.CLR_BOLD}{py_ver}{config.CLR_RESET} (Python 3.8+ required)")
        issues_found += 1

    if config.HAS_FCNTL:
        print(f"  {config.CLR_GREEN}✓{config.CLR_RESET} POSIX Concurrency: fcntl multi-process file locking available")
    else:
        print(f"  {config.CLR_YELLOW}⚠{config.CLR_RESET} POSIX Concurrency: fcntl unavailable (degraded multi-process locking)")
        warnings_found += 1

    # 2. Native Antigravity Executable (agy)
    real_agy = _get_agy_binary()
    if real_agy:
        agy_ver = _get_installed_agy_version()
        print(f"  {config.CLR_GREEN}✓{config.CLR_RESET} Native Binary: {config.CLR_BOLD}{real_agy}{config.CLR_RESET} (v{agy_ver})")
    else:
        print(f"  {config.CLR_YELLOW}⚠{config.CLR_RESET} Native Binary: No native 'agy' found in standard PATH candidates")
        print(f"    {config.CLR_DIM}↳ Set AGY_BIN=/path/to/agy if installed in custom location.{config.CLR_RESET}")
        warnings_found += 1

    # 3. Gateway Proxy Daemon
    info = _get_daemon_info()
    listening = _get_port_listening()
    if info and listening:
        outdated = _get_daemon_outdated()
        if outdated:
            print(f"  {config.CLR_YELLOW}⚠{config.CLR_RESET} Gateway Daemon: RUNNING (PID {info['pid']}) with {config.CLR_YELLOW}outdated disk code{config.CLR_RESET}")
            print(f"    {config.CLR_DIM}↳ Run 'agy-pool restart' to reload daemon with latest code.{config.CLR_RESET}")
            warnings_found += 1
        else:
            ver_tag = f" [v{info['version']}]" if info.get("version") else ""
            print(f"  {config.CLR_GREEN}✓{config.CLR_RESET} Gateway Daemon: RUNNING (PID {info['pid']}{ver_tag}) on port {config.DEFAULT_PORT}")
    elif listening:
        print(f"  {config.CLR_YELLOW}⚠{config.CLR_RESET} Gateway Daemon: Port {config.DEFAULT_PORT} is in use by another process")
        warnings_found += 1
    else:
        print(f"  {config.CLR_BLUE}○{config.CLR_RESET} Gateway Daemon: STOPPED (will auto-launch on 'agy' run)")

    # 4. Storage & Account Pool Health
    storage.ensure_dirs()
    pool_file = _get_pool_config_file()
    if os.path.exists(pool_file):
        try:
            pool = _get_load_pool()()
            pool_accounts = pool.get("accounts", [])
            active_id = pool.get("active_account_id")
            strategy = pool.get("strategy", "max_quota")

            ready_cnt = 0
            exhausted_cnt = 0
            cooldown_cnt = 0
            restricted_cnt = 0
            now_ts = time.time()

            for a in pool_accounts:
                status = a.get("status")
                q = a.get("last_quota", {})
                rem_f = q.get("remaining_fraction", 1.0)
                if status in ("validation_required", "auth_error"):
                    restricted_cnt += 1
                elif a.get("rate_limited_until", 0) > now_ts:
                    cooldown_cnt += 1
                elif rem_f <= 0.005:
                    exhausted_cnt += 1
                else:
                    ready_cnt += 1

            perm = oct(os.stat(pool_file).st_mode & 0o777)
            perm_ok = (perm == "0o600")
            perm_str = f"{perm}" if perm_ok else f"{config.CLR_YELLOW}{perm} (expected 0o600){config.CLR_RESET}"
            if not perm_ok:
                warnings_found += 1

            active_str = ""
            display_fn = _get_display_account_name()
            for a in pool_accounts:
                if a.get("id") == active_id:
                    active_str = f" | Active: {display_fn(a)}"
                    break

            print(f"  {config.CLR_GREEN}✓{config.CLR_RESET} Account Pool: {len(pool_accounts)} account(s) configured ({perm_str}){active_str}")
            print(f"    • Status: {config.CLR_GREEN}{ready_cnt} Ready{config.CLR_RESET}, {config.CLR_YELLOW}{cooldown_cnt} Cooldown{config.CLR_RESET}, {config.CLR_DIM}{exhausted_cnt} Exhausted{config.CLR_RESET}, {config.CLR_RED}{restricted_cnt} Restricted{config.CLR_RESET}")
            print(f"    • Strategy: {config.CLR_BOLD}{strategy}{config.CLR_RESET}")

            if restricted_cnt > 0:
                print(f"    {config.CLR_YELLOW}↳ Some accounts require verification or re-auth. Run 'agy-pool verify'.{config.CLR_RESET}")
                warnings_found += 1
            if len(pool_accounts) == 0:
                print(f"    {config.CLR_YELLOW}↳ Pool is empty. Run 'agy-pool login' or 'agy-pool import-current'.{config.CLR_RESET}")
                warnings_found += 1
        except Exception as e:
            print(f"  {config.CLR_RED}✖{config.CLR_RESET} Account Pool: Failed to read pool file: {e}")
            issues_found += 1
    else:
        print(f"  {config.CLR_YELLOW}○{config.CLR_RESET} Account Pool: No account pool file found yet ({pool_file})")
        print(f"    {config.CLR_DIM}↳ Run 'agy-pool import-current' or 'agy-pool login' to initialize.{config.CLR_RESET}")
        warnings_found += 1

    # 5. Upstream Google Cloud Code Connectivity
    backend_host = _get_backend_host()
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((backend_host, 443), timeout=5) as sock:
            with ctx.wrap_socket(sock, server_hostname=backend_host) as ssock:
                print(f"  {config.CLR_GREEN}✓{config.CLR_RESET} Cloud Code API: Reachable via TLS ({backend_host}:443)")
    except Exception as e:
        print(f"  {config.CLR_YELLOW}⚠{config.CLR_RESET} Cloud Code API: Connection to {backend_host} failed: {e}")
        warnings_found += 1

    # 6. Session Database
    db_path = os.path.join(_get_agy_cli_dir(), "conversation_summaries.db")
    if os.path.exists(db_path):
        try:
            db_uri = "file:" + urllib.parse.quote(db_path, safe="/") + "?mode=ro"
            with contextlib.closing(sqlite3.connect(db_uri, uri=True, timeout=2)) as con:
                count = con.execute("SELECT COUNT(*) FROM conversation_summaries").fetchone()[0]
            print(f"  {config.CLR_GREEN}✓{config.CLR_RESET} Session Continuity: Database active ({count} conversation(s) recorded)")
        except Exception as e:
            print(f"  {config.CLR_YELLOW}⚠{config.CLR_RESET} Session Continuity: Database query warning: {e}")
            warnings_found += 1
    else:
        print(f"  {config.CLR_DIM}○{config.CLR_RESET} Session Continuity: Database not yet created (created on first agy session)")

    # 7. Gateway Log File
    log_file = _get_log_file()
    max_log_bytes = _get_max_log_bytes()
    if os.path.exists(log_file):
        try:
            sz = os.path.getsize(log_file)
            print(f"  {config.CLR_GREEN}✓{config.CLR_RESET} Gateway Log: {log_file} ({daemon._format_size(sz)}, cap: {daemon._format_size(max_log_bytes)})")
        except Exception:
            pass

    print("\n" + "-" * 68)
    if issues_found == 0 and warnings_found == 0:
        print(f" {config.CLR_GREEN}{config.CLR_BOLD}All systems nominal! You are ready to use 'agy'.{config.CLR_RESET}")
    elif issues_found == 0:
        print(f" {config.CLR_YELLOW}{config.CLR_BOLD}System operational with {warnings_found} warning(s). Check details above.{config.CLR_RESET}")
    else:
        print(f" {config.CLR_RED}{config.CLR_BOLD}System has {issues_found} error(s) and {warnings_found} warning(s). Please fix issues above.{config.CLR_RESET}")
    print(sep + "\n")
    return issues_found == 0


__all__ = [
    "run_doctor",
    "set_agy_binary_finder",
    "set_agy_version_provider",
    "set_daemon_info_getter",
    "set_port_listener",
    "set_daemon_outdated_checker",
    "set_load_pool_fn",
    "set_pool_config_file_provider",
    "set_backend_host_provider",
    "set_agy_cli_dir_provider",
    "set_log_file_provider",
    "set_max_log_bytes_provider",
    "set_display_account_name_fn",
]

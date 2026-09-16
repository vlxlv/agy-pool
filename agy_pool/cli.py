"""
agy_pool.cli: Command-line interface, argument parsing, execution wrappers,
and account/strategy/log management for agy-pool.

Extracted in Checkpoint 6B (final checkpoint) of Python modularization.
"""

import argparse
import contextlib
import json
import os
import platform
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse

from agy_pool import config
from agy_pool import storage
from agy_pool import auth
from agy_pool import accounts
from agy_pool import quota
from agy_pool import scheduler
from agy_pool import proxy
from agy_pool import daemon
from agy_pool import diagnostics


def _get_self_paths():
    paths = set()
    try:
        paths.add(os.path.realpath(__file__))
    except Exception:
        pass
    try:
        ep = daemon.get_entrypoint_path()
        if ep:
            paths.add(os.path.realpath(ep))
    except Exception:
        pass
    if sys.argv and sys.argv[0]:
        try:
            paths.add(os.path.realpath(sys.argv[0]))
        except Exception:
            pass
    try:
        user_bin = os.path.expanduser("~/.local/bin/agy-pool")
        if os.path.exists(user_bin):
            paths.add(os.path.realpath(user_bin))
    except Exception:
        pass
    return paths


def find_real_agy_binary():
    """Locate the underlying native agy executable dynamically without self-recursion."""
    self_paths = _get_self_paths()

    if os.environ.get("AGY_BIN") and os.path.exists(os.environ["AGY_BIN"]):
        candidate = os.path.abspath(os.environ["AGY_BIN"])
        try:
            real_cand = os.path.realpath(candidate)
        except Exception:
            real_cand = candidate
        if real_cand not in self_paths:
            return candidate

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
            try:
                if os.path.realpath(candidate) not in self_paths:
                    return candidate
            except Exception:
                return candidate
    return None


def get_installed_agy_version():
    agy_bin = find_real_agy_binary()
    if agy_bin:
        try:
            p = subprocess.run([agy_bin, "--version"], capture_output=True, text=True, timeout=2)
            v = p.stdout.strip()
            if v and "." in v:
                return v
        except Exception:
            pass
    return "1.2.2"


INSTALLED_AGY_VER = get_installed_agy_version()
ARCH = "arm64" if platform.machine() in ["aarch64", "arm64"] else platform.machine()
OS_TYPE = "linux" if platform.system().lower() == "linux" else platform.system().lower()
DEFAULT_UA = f"antigravity/cli/{INSTALLED_AGY_VER} (aidev_client; os_type={OS_TYPE}; arch={ARCH}; auth_method=consumer)"

# Provider hooks for runtime dependency injection and test mocking
_load_pool_fn = None
_safe_quota_fn = None
_daemon_runner = None
_log_file_provider = None
_pid_file_provider = None
_port_provider = None
_version_provider = None
_doctor_runner = None
_daemon_info_getter = None
_port_listener = None
_daemon_outdated_checker = None
_daemon_stopper = None
_daemon_starter = None
_login_fn = None
_import_current_fn = None
_rename_account_fn = None
_remove_account_fn = None
_switch_account_fn = None
_export_pool_fn = None
_import_pool_fn = None
_verify_fn = None


def set_load_pool_fn(fn):
    global _load_pool_fn
    _load_pool_fn = fn


def set_safe_quota_fn(fn):
    global _safe_quota_fn
    _safe_quota_fn = fn


def set_daemon_runner(fn):
    global _daemon_runner
    _daemon_runner = fn


def set_log_file_provider(fn):
    global _log_file_provider
    _log_file_provider = fn


def set_pid_file_provider(fn):
    global _pid_file_provider
    _pid_file_provider = fn


def set_port_provider(fn):
    global _port_provider
    _port_provider = fn


def set_version_provider(fn):
    global _version_provider
    _version_provider = fn


def set_doctor_runner(fn):
    global _doctor_runner
    _doctor_runner = fn


def set_daemon_info_getter(fn):
    global _daemon_info_getter
    _daemon_info_getter = fn


def set_port_listener(fn):
    global _port_listener
    _port_listener = fn


def set_daemon_outdated_checker(fn):
    global _daemon_outdated_checker
    _daemon_outdated_checker = fn


def set_daemon_stopper(fn):
    global _daemon_stopper
    _daemon_stopper = fn


def set_daemon_starter(fn):
    global _daemon_starter
    _daemon_starter = fn


def set_login_fn(fn):
    global _login_fn
    _login_fn = fn


def set_import_current_fn(fn):
    global _import_current_fn
    _import_current_fn = fn


def set_rename_account_fn(fn):
    global _rename_account_fn
    _rename_account_fn = fn


def set_remove_account_fn(fn):
    global _remove_account_fn
    _remove_account_fn = fn


def set_switch_account_fn(fn):
    global _switch_account_fn
    _switch_account_fn = fn


def set_export_pool_fn(fn):
    global _export_pool_fn
    _export_pool_fn = fn


def set_import_pool_fn(fn):
    global _import_pool_fn
    _import_pool_fn = fn


def set_verify_fn(fn):
    global _verify_fn
    _verify_fn = fn


def _get_load_pool():
    if _load_pool_fn is not None:
        return _load_pool_fn()
    return storage.load_pool()


def _do_safe_quota(account):
    if _safe_quota_fn is not None:
        return _safe_quota_fn(account)
    return quota._safe_quota(account)


def _is_daemon_running():
    if _daemon_runner is not None:
        return _daemon_runner()
    return daemon.is_daemon_running()


def _get_log_file():
    if _log_file_provider is not None:
        return _log_file_provider()
    return config.LOG_FILE


def _get_pid_file():
    if _pid_file_provider is not None:
        return _pid_file_provider()
    return config.PID_FILE


def _get_default_port():
    if _port_provider is not None:
        return _port_provider()
    return config.DEFAULT_PORT


def _get_version():
    if _version_provider is not None:
        return _version_provider()
    return config.VERSION


def _do_run_doctor():
    if _doctor_runner is not None:
        return _doctor_runner()
    return diagnostics.run_doctor()


def _get_daemon_info():
    if _daemon_info_getter is not None:
        return _daemon_info_getter()
    return daemon.get_daemon_info()


def _is_port_listening():
    if _port_listener is not None:
        return _port_listener()
    return daemon.is_port_listening()


def _is_daemon_outdated():
    if _daemon_outdated_checker is not None:
        return _daemon_outdated_checker()
    return daemon.is_daemon_outdated()


def _do_stop_daemon():
    if _daemon_stopper is not None:
        return _daemon_stopper()
    return daemon.stop_proxy_daemon()


def _do_start_daemon(foreground=False):
    if _daemon_starter is not None:
        return _daemon_starter(foreground=foreground)
    return daemon.start_proxy_daemon(foreground=foreground)


def _do_login():
    if _login_fn is not None:
        return _login_fn()
    return accounts.do_login()


def _do_import_current():
    if _import_current_fn is not None:
        return _import_current_fn()
    return accounts.import_current()


def _do_rename_account(target, name):
    if _rename_account_fn is not None:
        return _rename_account_fn(target, name)
    return accounts.rename_account(target, name)


def _do_remove_account(target):
    if _remove_account_fn is not None:
        return _remove_account_fn(target)
    return accounts.remove_account(target)


def _do_switch_account(target):
    if _switch_account_fn is not None:
        return _switch_account_fn(target)
    return accounts.switch_account(target)


def _do_export_pool(file_path=None, encrypt=False, password=None, no_stats=False):
    if _export_pool_fn is not None:
        return _export_pool_fn(file_path=file_path, encrypt=encrypt, password=password, no_stats=no_stats)
    return accounts.export_pool(file_path=file_path, encrypt=encrypt, password=password, no_stats=no_stats)


def _do_import_pool(file_path=None, password=None, replace=False, skip_existing=False):
    if _import_pool_fn is not None:
        return _import_pool_fn(file_path=file_path, password=password, replace=replace, skip_existing=skip_existing)
    return accounts.import_pool(file_path=file_path, password=password, replace=replace, skip_existing=skip_existing)


def _do_verify(target=None):
    if _verify_fn is not None:
        return _verify_fn(target)
    return accounts.do_verify(target)


def init_default_hooks():
    """Initializes standard cross-module hooks between agy_pool submodules."""
    diagnostics.set_agy_binary_finder(lambda: find_real_agy_binary())
    diagnostics.set_agy_version_provider(lambda: INSTALLED_AGY_VER)
    diagnostics.set_daemon_info_getter(lambda: daemon.get_daemon_info())
    diagnostics.set_port_listener(lambda *a, **k: daemon.is_port_listening(*a, **k))
    diagnostics.set_daemon_outdated_checker(lambda: daemon.is_daemon_outdated())
    diagnostics.set_load_pool_fn(lambda: storage.load_pool())
    diagnostics.set_pool_config_file_provider(lambda: config.POOL_CONFIG_FILE)
    diagnostics.set_backend_host_provider(lambda: quota.BACKEND_HOST)
    diagnostics.set_agy_cli_dir_provider(lambda: config.AGY_CLI_DIR)
    diagnostics.set_log_file_provider(lambda: config.LOG_FILE)
    diagnostics.set_max_log_bytes_provider(lambda: config.MAX_LOG_BYTES)
    diagnostics.set_display_account_name_fn(lambda *a, **k: accounts.display_account_name(*a, **k))

    quota.set_token_refresher(lambda *a, **k: auth.refresh_token(*a, **k))
    quota.set_quota_prober(lambda *a, **k: quota.query_quota(*a, **k))
    quota.set_safe_quota_fn(lambda *a, **k: quota._safe_quota(*a, **k))
    quota.set_backend_url_provider(lambda: quota.BACKEND_URL_BASE)
    quota.set_user_agent_provider(lambda: DEFAULT_UA)

    proxy.set_backend_host_provider(lambda: quota.BACKEND_HOST)
    proxy.set_backend_url_provider(lambda: quota.BACKEND_URL_BASE)
    proxy.set_user_agent_provider(lambda: DEFAULT_UA)
    proxy.set_token_refresher(lambda *a, **k: auth.refresh_token(*a, **k))
    proxy.set_quota_refresher(lambda *a, **k: quota.schedule_quota_refresh(*a, **k))
    proxy.set_log_rotator(lambda *a, **k: daemon._maybe_rotate_log(*a, **k))

    daemon.set_pid_file_provider(lambda: config.PID_FILE)
    daemon.set_log_file_provider(lambda: config.LOG_FILE)
    daemon.set_port_provider(lambda: config.DEFAULT_PORT)
    daemon.set_version_provider(lambda: config.VERSION)

    accounts.set_quota_prober(lambda *args, **kwargs: quota.query_quota(*args, **kwargs), quota._format_account_quota_summary)


init_default_hooks()


def list_accounts(target=None):
    """Refreshes quota and displays multi-account pool dashboard."""
    pool = _get_load_pool()
    acc_list = pool.get("accounts", [])
    if not acc_list:
        print(f"\n{config.CLR_YELLOW}No accounts in pool yet.{config.CLR_RESET}")
        print(f"Run {config.CLR_BOLD}agy-pool login{config.CLR_RESET} or {config.CLR_BOLD}agy-pool import-current{config.CLR_RESET} to add accounts.\n")
        return

    if target:
        if str(target).isdigit():
            idx = int(target) - 1
            if 0 <= idx < len(acc_list):
                acc_list = [acc_list[idx]]
            else:
                print(f"{config.CLR_RED}[Error] Account '{target}' not found.{config.CLR_RESET}")
                return
        else:
            found = next((a for a in acc_list if a.get("id") == target or a.get("email") == target or a.get("name") == target), None)
            if found:
                acc_list = [found]
            else:
                print(f"{config.CLR_RED}[Error] Account '{target}' not found.{config.CLR_RESET}")
                return

    print(f"\n{config.CLR_BOLD}{config.CLR_CYAN}Refreshing quota for {len(acc_list)} account(s)...{config.CLR_RESET}")
    # Refresh all quotas concurrently
    threads = []
    for acc in acc_list:
        t = threading.Thread(target=lambda a: _do_safe_quota(a), args=(acc,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=8)

    pool = _get_load_pool()
    if target:
        # Re-fetch filtered account after quota refresh
        all_accs = pool.get("accounts", [])
        if str(target).isdigit():
            idx = int(target) - 1
            acc_list = [all_accs[idx]] if 0 <= idx < len(all_accs) else []
        else:
            found = next((a for a in all_accs if a.get("id") == target or a.get("email") == target or a.get("name") == target), None)
            acc_list = [found] if found else []
    else:
        acc_list = pool.get("accounts", [])

    daemon_running = _is_daemon_running()
    gateway_status = f"{config.CLR_GREEN}RUNNING (127.0.0.1:{_get_default_port()}){config.CLR_RESET}" if daemon_running else f"{config.CLR_DIM}STOPPED{config.CLR_RESET}"

    sep = "=" * 68
    sub_sep = "-" * 68
    print("\n" + sep)
    title = f"Antigravity Multi-Account Pool v{_get_version()}"
    print(f"{config.CLR_BOLD}{title:^68}{config.CLR_RESET}")
    print(sep)

    active_id = pool.get("active_account_id")
    for i, acc in enumerate(acc_list, start=1):
        acc_id = acc.get("id", f"acc_{i}")
        is_active = (acc_id == active_id)
        q = acc.get("last_quota", {})
        g5 = q.get("gemini_5h", {})
        gw = q.get("gemini_weekly", {})
        tp5 = q.get("third_party_5h", {})
        status = acc.get("status")

        g5_frac, gw_frac = quota._display_quota_fractions(q)
        legacy_reset = q.get("reset_time") if "gemini_5h" not in q and "gemini_weekly" not in q else None
        g5_reset = quota.format_remaining_time(g5.get("reset_time", legacy_reset))
        gw_reset = quota.format_remaining_time(gw.get("reset_time", legacy_reset))

        g5_bar = quota.render_progress_bar(g5_frac, width=10)
        gw_bar = quota.render_progress_bar(gw_frac, width=10)

        now_ts = time.time()
        is_cooling = acc.get("rate_limited_until", 0) > now_ts
        is_exhausted = any(fraction is not None and fraction <= 0.005 for fraction in (g5_frac, gw_frac))

        hits = acc.get("gen_count", acc.get("request_count", 0))

        if status == "validation_required":
            marker = f"{config.CLR_YELLOW}⚠ Verify Required{config.CLR_RESET}"
        elif status == "auth_error":
            marker = f"{config.CLR_RED}✖ Auth Error{config.CLR_RESET}"
        elif is_cooling:
            marker = f"{config.CLR_YELLOW}* Active (Cooldown){config.CLR_RESET}" if is_active else f"{config.CLR_YELLOW}Cooldown{config.CLR_RESET}"
        elif is_exhausted:
            marker = f"{config.CLR_RED}* Active (Exhausted){config.CLR_RESET}" if is_active else f"{config.CLR_DIM}Exhausted{config.CLR_RESET}"
        elif is_active:
            marker = f"{config.CLR_GREEN}* Active{config.CLR_RESET}"
        else:
            marker = f"{config.CLR_CYAN}Ready{config.CLR_RESET}"
        disp_name = accounts.display_account_name(acc)

        print(f"{config.CLR_BOLD}[{i}] {disp_name}{config.CLR_RESET}  [{marker}]  Hits: {hits}")

        if status == "validation_required":
            print(f"    • Gemini 5-Hour: {config.CLR_YELLOW}[░░░░░░░░░░]  Action Required (Blocked){config.CLR_RESET}")
            print(f"    • Gemini Weekly: {config.CLR_YELLOW}[░░░░░░░░░░]  Action Required (Blocked){config.CLR_RESET}")
            print(f"    {config.CLR_YELLOW}↳ Google requires security verification for this account.{config.CLR_RESET}")
            print(f"      Run {config.CLR_BOLD}agy-pool verify {i}{config.CLR_RESET} to unlock in browser.")
        elif status == "auth_error":
            print(f"    • Gemini 5-Hour: {config.CLR_RED}[░░░░░░░░░░]  Authentication Failure{config.CLR_RESET}")
            print(f"    • Gemini Weekly: {config.CLR_RED}[░░░░░░░░░░]  Authentication Failure{config.CLR_RESET}")
            print(f"    {config.CLR_RED}↳ Token expired or revoked. Re-authenticate account.{config.CLR_RESET}")
        else:
            print(f"    • Gemini 5-Hour: {g5_bar}  (Resets {g5_reset})")
            print(f"    • Gemini Weekly: {gw_bar}  (Resets {gw_reset})")
            print(f"    • Quota age: {quota.format_quota_age(acc)}")

            if tp5 and tp5.get("fraction") is not None and tp5.get("fraction") < 1.0:
                tp_bar = quota.render_progress_bar(tp5.get("fraction"), width=10)
                tp_reset = quota.format_remaining_time(tp5.get("reset_time"))
                print(f"    • Claude/GPT 5h: {tp_bar}  (Resets {tp_reset})")

        if i < len(acc_list):
            print()

    print(sub_sep)
    print(f" {config.CLR_GREEN}[* Active]{config.CLR_RESET} {config.CLR_DIM}CLI Base Token{config.CLR_RESET}    {config.CLR_CYAN}[Ready]{config.CLR_RESET} {config.CLR_DIM}In Rotation Pool{config.CLR_RESET}")
    print(f" {config.CLR_YELLOW}[Cooldown]{config.CLR_RESET} {config.CLR_DIM}Rate Limited{config.CLR_RESET}      {config.CLR_DIM}[Exhausted] Quota Depleted{config.CLR_RESET}")
    print(f" Strategy: {config.CLR_BOLD}{pool.get('strategy', 'max_quota')}{config.CLR_RESET} | Gateway Proxy: {gateway_status}")
    print(sep + "\n")


def manage_strategy(target_strategy=None):
    """Displays or updates the pool load balancing strategy."""
    valid_strategies = scheduler.VALID_STRATEGIES
    pool = _get_load_pool()
    current = pool.get("strategy", "max_quota")
    if not target_strategy:
        print(f"\nCurrent load balancing strategy: {config.CLR_BOLD}{config.CLR_GREEN}{current}{config.CLR_RESET}\n")
        print("Available strategies:")
        print(f"  • {config.CLR_BOLD}max_quota{config.CLR_RESET}   - Prioritizes accounts with safest remaining 5-hour and weekly quota, taking reset times into account (default)")
        print(f"  • {config.CLR_BOLD}least_used{config.CLR_RESET}  - Distributes load evenly to accounts with lowest AI generation count (Hits)")
        print(f"  • {config.CLR_BOLD}round_robin{config.CLR_RESET} - Cycles through ready accounts in sequential rotation\n")
        print(f"To update strategy: {config.CLR_BOLD}agy-pool strategy <name>{config.CLR_RESET}\n")
        return current

    target_strategy = target_strategy.lower().strip()
    if target_strategy not in valid_strategies:
        print(f"{config.CLR_RED}[Error] Invalid strategy '{target_strategy}'. Choose from: {', '.join(valid_strategies)}{config.CLR_RESET}")
        return False

    def update(pool_obj):
        pool_obj["strategy"] = target_strategy
    storage.pool_transaction(update)
    print(f"{config.CLR_GREEN}✓ Load balancing strategy set to: {config.CLR_BOLD}{target_strategy}{config.CLR_RESET}")
    return True


def show_logs(lines=20, follow=False, clear=False, rotate=False):
    """Views, follows, rotates, or clears the gateway log file."""
    log_file = _get_log_file()
    max_log_bytes = config.MAX_LOG_BYTES
    backup_count = config.BACKUP_LOG_COUNT

    if clear:
        daemon.clear_log(log_file, backup_count)
        print(f"{config.CLR_GREEN}✓ Successfully cleared gateway log ({log_file}){config.CLR_RESET}")
        return

    if rotate:
        if not os.path.exists(log_file) or os.path.getsize(log_file) == 0:
            print(f"{config.CLR_YELLOW}Log file is empty or does not exist, rotation skipped.{config.CLR_RESET}")
            return
        success = daemon.rotate_log_if_needed(log_file, max_bytes=max_log_bytes, backup_count=backup_count, force=True)
        if success:
            print(f"{config.CLR_GREEN}✓ Rotated active log: {log_file} -> {log_file}.1{config.CLR_RESET}")
        else:
            print(f"{config.CLR_RED}[Error] Log rotation failed.{config.CLR_RESET}")
        return

    if not os.path.exists(log_file):
        print(f"{config.CLR_YELLOW}Log file does not exist yet: {log_file}{config.CLR_RESET}")
        return

    try:
        size = os.path.getsize(log_file)
    except OSError:
        size = 0

    backup_info = ""
    backup_file = f"{log_file}.1"
    if os.path.exists(backup_file):
        try:
            b_size = os.path.getsize(backup_file)
            backup_info = f" | Backup: {daemon._format_size(b_size)}"
        except OSError:
            pass

    all_lines = []
    try:
        with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
            all_lines = f.readlines()
    except Exception as e:
        print(f"{config.CLR_RED}[Error] Failed to read log file: {e}{config.CLR_RESET}")
        return

    total_lines = len(all_lines)
    print("=" * 68)
    print("                    Antigravity Gateway Log                         ")
    print("=" * 68)
    print(f"Path: {log_file}")
    print(f"Size: {daemon._format_size(size)} ({total_lines:,} lines){backup_info}")
    print(f"Policy: Auto-rotate at {daemon._format_size(max_log_bytes)} (retaining {backup_count} backup)")
    print("-" * 68)

    tail_lines = all_lines[-lines:] if lines > 0 else all_lines
    for line in tail_lines:
        sys.stdout.write(line)
    sys.stdout.flush()

    if follow:
        print("-" * 68)
        print(f"{config.CLR_CYAN}Streaming live logs (Ctrl+C to stop)...{config.CLR_RESET}")
        try:
            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                f.seek(0, os.SEEK_END)
                while True:
                    line = f.readline()
                    if line:
                        sys.stdout.write(line)
                        sys.stdout.flush()
                    else:
                        time.sleep(0.2)
        except KeyboardInterrupt:
            print(f"\n{config.CLR_DIM}Stream stopped.{config.CLR_RESET}")


def find_latest_conversation_for_dir(start_dir=None):
    """
    Finds the most recent conversation ID matching start_dir (or its parent project),
    ordered strictly by last_modified_time DESC, across ALL Google accounts.
    Returns (cid, title, matched_dir).
    """
    if not start_dir:
        start_dir = os.getcwd()
    start_dir = os.path.realpath(start_dir)
    home_dir = os.path.realpath(os.path.expanduser("~"))
    db_path = os.path.join(config.AGY_CLI_DIR, "conversation_summaries.db")
    if not os.path.exists(db_path):
        return None, None, None

    try:
        db_uri = "file:" + urllib.parse.quote(db_path, safe="/") + "?mode=ro"
        with contextlib.closing(sqlite3.connect(db_uri, uri=True, timeout=2)) as con:
            con.execute("PRAGMA query_only=ON")
            con.execute("PRAGMA busy_timeout=2000")
            rows = con.execute("""
                SELECT conversation_id, title, workspace_uris, last_modified_time
                FROM conversation_summaries
                ORDER BY last_modified_time DESC
            """).fetchall()
    except Exception:
        return None, None, None

    convs = []
    for cid, title, uris_raw, mtime in rows:
        if not cid:
            continue
        try:
            uris = json.loads(uris_raw) if uris_raw else []
        except Exception:
            uris = []
        dirs = [os.path.realpath(urllib.parse.unquote(u.replace("file://", "")).rstrip("/")) for u in uris if u]
        convs.append((cid, title, dirs, mtime))

    curr = start_dir
    while True:
        target_path = curr.rstrip("/")
        for cid, title, dirs, mtime in convs:
            if target_path in dirs:
                return cid, title, target_path
        if curr == home_dir or curr == "/" or os.path.dirname(curr) == curr:
            break
        curr = os.path.dirname(curr)

    return None, None, None


def resolve_continue_arg(extra_args):
    """
    Intercepts -c / --continue and resolves it directly to --conversation <cid>
    for the current directory's most recent conversation across all accounts,
    ensuring account-agnostic continuity.
    """
    has_continue = False
    new_args = []
    skip_next = False
    for i, arg in enumerate(extra_args):
        if skip_next:
            skip_next = False
            continue
        if arg in ("-c", "--continue"):
            has_continue = True
        elif arg == "--conversation" or arg.startswith("--conversation="):
            # If user explicitly passed --conversation, do not interfere
            return extra_args
        else:
            new_args.append(arg)

    if not has_continue:
        return extra_args

    cid, title, matched_dir = find_latest_conversation_for_dir(os.getcwd())
    if cid:
        lock_file = os.path.join(config.AGY_CLI_DIR, "presence", f"{cid}.lock")
        is_locked = False
        if os.path.exists(lock_file) and config.HAS_FCNTL and config.fcntl is not None:
            try:
                fd = os.open(lock_file, os.O_RDWR)
                try:
                    config.fcntl.flock(fd, config.fcntl.LOCK_EX | config.fcntl.LOCK_NB)
                    config.fcntl.flock(fd, config.fcntl.LOCK_UN)
                except BlockingIOError:
                    is_locked = True
                finally:
                    os.close(fd)
            except OSError:
                pass

        dir_name = os.path.basename(matched_dir) if matched_dir else "workspace"
        if not dir_name:
            dir_name = "home"
        display_title = title.strip() if title and title.strip() else cid[:8]

        if is_locked:
            sys.stderr.write(f"{config.CLR_YELLOW}[agy-pool] Notice: Conversation '{display_title}' ({cid[:8]}...) is active in another process.{config.CLR_RESET}\n")
            sys.stderr.flush()
        else:
            sys.stderr.write(f"{config.CLR_GREEN}✓ [agy-pool] Resuming last conversation in {dir_name}: {config.CLR_BOLD}{display_title}{config.CLR_RESET}\n")
            sys.stderr.flush()

        return ["--conversation", cid] + new_args

    return extra_args


def run_agy_with_lb(extra_args):
    """Ensures LB daemon is running and executes agy with dynamic load balancing."""
    extra_args = resolve_continue_arg(extra_args)
    daemon.ensure_daemon_running()

    env = os.environ.copy()
    env["CLOUD_CODE_URL"] = f"http://127.0.0.1:{_get_default_port()}"

    # Native agy still needs compatibility state at startup. Request routing uses
    # the proxy's per-request Authorization and never rewrites this file.
    try:
        accounts.sync_active_agy_token_file()
    except Exception:
        pass

    # Locate real agy binary
    agy_path = find_real_agy_binary()
    if not agy_path:
        print(f"{config.CLR_RED}[Error] Could not locate 'agy' executable in PATH.{config.CLR_RESET}")
        sys.exit(1)

    cmd = [agy_path] + extra_args
    try:
        os.execvpe(agy_path, cmd, env)
    except Exception as e:
        print(f"{config.CLR_RED}[Error] Failed to execute agy: {e}{config.CLR_RESET}")
        sys.exit(1)


def run_agy_direct(extra_args):
    """Executes original native agy directly without proxy gateway."""
    extra_args = resolve_continue_arg(extra_args)
    env = os.environ.copy()
    env.pop("CLOUD_CODE_URL", None)

    agy_path = find_real_agy_binary()
    if not agy_path:
        print(f"{config.CLR_RED}[Error] Could not locate 'agy' executable in PATH.{config.CLR_RESET}")
        sys.exit(1)

    cmd = [agy_path] + extra_args
    try:
        os.execvpe(agy_path, cmd, env)
    except Exception as e:
        print(f"{config.CLR_RED}[Error] Failed to execute agy: {e}{config.CLR_RESET}")
        sys.exit(1)


def main():
    """Main CLI entrypoint for agy-pool."""
    version_str = _get_version()

    # Fast dispatch for CLI run / direct commands preserving exact args
    if len(sys.argv) > 1:
        if sys.argv[1] == "run":
            run_agy_with_lb(sys.argv[2:])
            return
        elif sys.argv[1] in ["raw", "orig", "direct"]:
            run_agy_direct(sys.argv[2:])
            return
        elif sys.argv[1] in ["-v", "--version", "version"]:
            print(f"agy-pool {version_str}")
            return
        elif sys.argv[1] in ["doctor", "check", "health"]:
            if not _do_run_doctor():
                sys.exit(1)
            return

    parser = argparse.ArgumentParser(
        prog="agy-pool",
        description="Antigravity Multi-Account Pool & Load Balancer for Termux & Linux",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  agy-pool login            Add/login a new Google account
  agy-pool list             Refresh and show cached account quotas
  agy-pool strategy         View or change load balancing strategy
  agy-pool rename 1 "Work"  Set friendly label for an account
  agy-pool doctor           Run comprehensive system diagnostic check
  agy-pool switch auto      Switch active account to the one with highest quota
  agy-pool export           Export accounts pool to a backup JSON file
  agy-pool import file.json Restore accounts pool from a backup file
  agy-pool start            Start the load balancing reverse proxy daemon
  agy-pool run              Launch agy with automated quota load balancing
"""
    )
    parser.add_argument("-v", "--version", action="version", version=f"agy-pool {version_str}")

    subparsers = parser.add_subparsers(dest="command")

    # login / add
    subparsers.add_parser("login", aliases=["add"], help="Login and add a Google account to the pool")
    # import-current
    subparsers.add_parser("import-current", help="Import current account from existing antigravity token")
    # list / quota / status
    list_p = subparsers.add_parser("list", aliases=["ls", "quota"], help="List accounts and display quota dashboard")
    list_p.add_argument("target", nargs="?", default=None, help="Optional account index, name, or email")
    # strategy
    strat_p = subparsers.add_parser("strategy", aliases=["strat"], help="View or update load balancing strategy (max_quota, least_used, round_robin)")
    strat_p.add_argument("name", nargs="?", default=None, help="Strategy name ('max_quota', 'least_used', 'round_robin')")
    # rename
    ren_p = subparsers.add_parser("rename", help="Rename an account display name or friendly label")
    ren_p.add_argument("target", help="Account index, name, or email")
    ren_p.add_argument("name", help="New account label / name")
    # doctor
    subparsers.add_parser("doctor", aliases=["check", "health"], help="Run comprehensive system and environment diagnostics")
    # remove
    rm_p = subparsers.add_parser("remove", aliases=["rm"], help="Remove an account by index, name, or email")
    rm_p.add_argument("target", help="Account index, name, or email")
    # switch
    sw_p = subparsers.add_parser("switch", help="Switch current active account (auto or index/name/email)")
    sw_p.add_argument("target", nargs="?", default="auto", help="Account index, name, email, or 'auto'")
    # export / backup
    exp_p = subparsers.add_parser("export", aliases=["backup"], help="Export and backup accounts pool to JSON or encrypted bundle")
    exp_p.add_argument("file", nargs="?", default=None, help="Output file path (default: agy-pool-backup-<timestamp>.json, or '-' for stdout)")
    exp_p.add_argument("-e", "--encrypt", action="store_true", help="Encrypt backup bundle with a passphrase")
    exp_p.add_argument("-p", "--password", default=None, help="Passphrase for encryption (prompts securely if omitted with --encrypt)")
    exp_p.add_argument("--no-stats", action="store_true", help="Omit request and generation count statistics")
    # import / restore
    imp_p = subparsers.add_parser("import", aliases=["restore"], help="Import and restore accounts pool from backup")
    imp_p.add_argument("file", help="Input backup file path (or '-' for stdin)")
    imp_p.add_argument("-p", "--password", default=None, help="Passphrase for encrypted backup (prompts if required)")
    imp_p.add_argument("--replace", action="store_true", help="Replace all existing accounts instead of merging")
    imp_p.add_argument("--skip-existing", action="store_true", help="Skip existing accounts instead of updating them")
    # verify
    ver_p = subparsers.add_parser("verify", help="Open Google security verification URL for restricted account")
    ver_p.add_argument("target", nargs="?", default=None, help="Account index, name, or email (defaults to first restricted account)")
    # log / logs
    log_p = subparsers.add_parser("log", aliases=["logs"], help="View, follow, or manage gateway proxy logs")
    log_p.add_argument("-n", "--lines", type=int, default=20, help="Number of lines to display (default: 20)")
    log_p.add_argument("-f", "--follow", action="store_true", help="Follow log output in real-time (like tail -f)")
    log_p.add_argument("--clear", "--clean", action="store_true", help="Clear and truncate active log and backups")
    log_p.add_argument("--rotate", action="store_true", help="Force immediate log rotation")
    # daemon control
    subparsers.add_parser("start", help="Start background load balancer proxy daemon")
    subparsers.add_parser("stop", help="Stop background load balancer proxy daemon")
    subparsers.add_parser("restart", help="Restart background load balancer proxy daemon")
    subparsers.add_parser("status", help="Check status of proxy daemon and pool")
    subparsers.add_parser("version", help="Show version information")
    subparsers.add_parser("daemon-run", help=argparse.SUPPRESS)  # Internal daemon worker
    # run wrapper
    run_p = subparsers.add_parser("run", help="Run agy through the load balancer")
    run_p.add_argument("args", nargs=argparse.REMAINDER, help="Arguments to pass to agy")

    args, unknown = parser.parse_known_args()

    cmd = args.command
    if not cmd:
        list_accounts()
        return

    if cmd in ["login", "add"]:
        _do_login()
    elif cmd == "import-current":
        _do_import_current()
    elif cmd in ["list", "ls", "quota"]:
        list_accounts(getattr(args, "target", None))
    elif cmd in ["strategy", "strat"]:
        if manage_strategy(args.name) is False:
            sys.exit(1)
    elif cmd == "rename":
        if not _do_rename_account(args.target, args.name):
            sys.exit(1)
    elif cmd in ["doctor", "check", "health"]:
        if not _do_run_doctor():
            sys.exit(1)
    elif cmd in ["remove", "rm"]:
        _do_remove_account(args.target)
    elif cmd == "switch":
        _do_switch_account(args.target)
    elif cmd in ["export", "backup"]:
        _do_export_pool(file_path=args.file, encrypt=args.encrypt, password=args.password, no_stats=args.no_stats)
    elif cmd in ["import", "restore"]:
        _do_import_pool(file_path=args.file, password=args.password, replace=args.replace, skip_existing=args.skip_existing)
    elif cmd == "verify":
        _do_verify(args.target)
    elif cmd in ["log", "logs"]:
        show_logs(lines=args.lines, follow=args.follow, clear=args.clear, rotate=args.rotate)
    elif cmd == "start":
        _do_start_daemon(foreground=False)
    elif cmd == "stop":
        _do_stop_daemon()
    elif cmd == "restart":
        _do_stop_daemon()
        time.sleep(1)
        _do_start_daemon(foreground=False)
    elif cmd == "status":
        info = _get_daemon_info()
        if info and _is_port_listening():
            ver_note = f" [v{info['version']}]" if info.get("version") else ""
            if _is_daemon_outdated():
                print(f"{config.CLR_YELLOW}⚠ Gateway Proxy is RUNNING (PID: {info['pid']}{ver_note}, Outdated Code). Run 'agy-pool restart' to reload.{config.CLR_RESET}")
            else:
                print(f"{config.CLR_GREEN}✓ Gateway Proxy is RUNNING (PID: {info['pid']}{ver_note}) on http://127.0.0.1:{_get_default_port()}{config.CLR_RESET}")
        else:
            print(f"{config.CLR_YELLOW}○ Gateway Proxy is STOPPED{config.CLR_RESET}")
        list_accounts()
    elif cmd == "version":
        print(f"agy-pool {version_str}")
    elif cmd == "daemon-run":
        _do_start_daemon(foreground=True)
    elif cmd == "run":
        all_args = (args.args or []) + unknown
        run_agy_with_lb(all_args)


__all__ = [
    "find_real_agy_binary",
    "get_installed_agy_version",
    "INSTALLED_AGY_VER",
    "ARCH",
    "OS_TYPE",
    "DEFAULT_UA",
    "list_accounts",
    "manage_strategy",
    "show_logs",
    "find_latest_conversation_for_dir",
    "resolve_continue_arg",
    "run_agy_with_lb",
    "run_agy_direct",
    "main",
    "set_load_pool_fn",
    "set_safe_quota_fn",
    "set_daemon_runner",
    "set_log_file_provider",
    "set_pid_file_provider",
    "set_port_provider",
    "set_version_provider",
    "set_doctor_runner",
    "set_daemon_info_getter",
    "set_port_listener",
    "set_daemon_outdated_checker",
    "set_daemon_stopper",
    "set_daemon_starter",
    "set_login_fn",
    "set_import_current_fn",
    "set_rename_account_fn",
    "set_remove_account_fn",
    "set_switch_account_fn",
    "set_export_pool_fn",
    "set_import_pool_fn",
    "set_verify_fn",
]

"""
agy_pool.daemon: Process management, daemon lifecycle, and log rotation for agy-pool.

Extracted in CP5B of Python modularization.
"""

from datetime import datetime
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

from agy_pool import config
from agy_pool import storage
from agy_pool import quota
from agy_pool import proxy


# ----------------- Log Management & Rotation -----------------
_LAST_LOG_ROTATE_CHECK = 0
_LOG_ROTATE_INTERVAL = 30  # seconds between size checks in active request loop

_entrypoint_path = None
_pid_file_provider = None
_log_file_provider = None
_port_provider = None
_version_provider = None

_port_listener = None
_daemon_runner = None
_daemon_outdated_checker = None
_daemon_pid_getter = None
_daemon_stopper = None
_daemon_starter = None


def set_entrypoint_path(path):
    global _entrypoint_path
    _entrypoint_path = path


def get_entrypoint_path():
    global _entrypoint_path
    if _entrypoint_path and os.path.exists(_entrypoint_path):
        return _entrypoint_path
    repo_bin = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "bin", "agy-pool"))
    if os.path.exists(repo_bin):
        return repo_bin
    if sys.argv and sys.argv[0] and os.path.exists(sys.argv[0]):
        return os.path.abspath(sys.argv[0])
    return __file__


def set_pid_file_provider(fn):
    global _pid_file_provider
    _pid_file_provider = fn


def _get_pid_file():
    if _pid_file_provider is not None:
        return _pid_file_provider()
    return config.PID_FILE


def set_log_file_provider(fn):
    global _log_file_provider
    _log_file_provider = fn


def _get_log_file():
    if _log_file_provider is not None:
        return _log_file_provider()
    return config.LOG_FILE


def set_port_provider(fn):
    global _port_provider
    _port_provider = fn


def _get_default_port():
    if _port_provider is not None:
        return _port_provider()
    return config.DEFAULT_PORT


def set_version_provider(fn):
    global _version_provider
    _version_provider = fn


def _get_version():
    if _version_provider is not None:
        return _version_provider()
    return config.VERSION


def set_port_listener(fn):
    global _port_listener
    _port_listener = fn


def _get_port_listener():
    return _port_listener or is_port_listening


def set_daemon_runner(fn):
    global _daemon_runner
    _daemon_runner = fn


def _get_daemon_runner():
    return _daemon_runner or is_daemon_running


def set_daemon_outdated_checker(fn):
    global _daemon_outdated_checker
    _daemon_outdated_checker = fn


def _get_daemon_outdated_checker():
    return _daemon_outdated_checker or is_daemon_outdated


def set_daemon_pid_getter(fn):
    global _daemon_pid_getter
    _daemon_pid_getter = fn


def _get_daemon_pid_getter():
    return _daemon_pid_getter or get_daemon_pid


def set_daemon_stopper(fn):
    global _daemon_stopper
    _daemon_stopper = fn


def _get_daemon_stopper():
    return _daemon_stopper or stop_proxy_daemon


def set_daemon_starter(fn):
    global _daemon_starter
    _daemon_starter = fn


def _get_daemon_starter():
    return _daemon_starter or start_proxy_daemon


def _format_size(bytes_val):
    if bytes_val < 1024:
        return f"{bytes_val} B"
    elif bytes_val < 1024 * 1024:
        return f"{bytes_val / 1024:.1f} KB"
    else:
        return f"{bytes_val / (1024 * 1024):.1f} MB"


def rotate_log_if_needed(log_path=None, max_bytes=None, backup_count=None, force=False):
    """
    Performs copytruncate log rotation if the file exceeds max_bytes (or force=True).
    Retains open file descriptors without disruption to running background daemons.
    Returns True if rotated, False otherwise.
    """
    if log_path is None:
        log_path = _get_log_file()
    if max_bytes is None:
        max_bytes = config.MAX_LOG_BYTES
    if backup_count is None:
        backup_count = config.BACKUP_LOG_COUNT
    config._assert_safe_write_path(log_path)
    if not os.path.exists(log_path):
        return False
    if not force:
        try:
            if os.path.getsize(log_path) < max_bytes:
                return False
        except OSError:
            return False

    lock_file = log_path + ".lock"
    try:
        with storage._file_lock(lock_file):
            if not os.path.exists(log_path):
                return False
            if not force:
                try:
                    if os.path.getsize(log_path) < max_bytes:
                        return False
                except OSError:
                    return False

            # Shift existing backups: .2 -> .3, .1 -> .2, etc.
            for i in range(backup_count - 1, 0, -1):
                src = f"{log_path}.{i}"
                dst = f"{log_path}.{i + 1}"
                if os.path.exists(src):
                    try:
                        os.replace(src, dst)
                    except OSError:
                        pass

            if backup_count > 0:
                backup_dst = f"{log_path}.1"
                try:
                    shutil.copyfile(log_path, backup_dst)
                except OSError:
                    pass

            # In-place truncation preserving open file descriptors (copytruncate)
            try:
                with open(log_path, "r+", encoding="utf-8", errors="ignore") as f:
                    f.truncate(0)
                    f.flush()
            except OSError:
                try:
                    with open(log_path, "w", encoding="utf-8", errors="ignore"):
                        pass
                except OSError:
                    pass
            return True
    except Exception as e:
        try:
            sys.stderr.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [LOG ROTATE ERROR] {e}\n")
            sys.stderr.flush()
        except Exception:
            pass
        return False


def clear_log(log_path=None, backup_count=None):
    """Safely clears/truncates active log file and removes backup logs."""
    if log_path is None:
        log_path = _get_log_file()
    if backup_count is None:
        backup_count = config.BACKUP_LOG_COUNT
    config._assert_safe_write_path(log_path)
    lock_file = log_path + ".lock"
    with storage._file_lock(lock_file):
        if os.path.exists(log_path):
            try:
                with open(log_path, "r+", encoding="utf-8", errors="ignore") as f:
                    f.truncate(0)
                    f.flush()
            except OSError:
                try:
                    with open(log_path, "w", encoding="utf-8", errors="ignore"):
                        pass
                except OSError:
                    pass
        for i in range(1, backup_count + 2):
            b_path = f"{log_path}.{i}"
            if os.path.exists(b_path):
                try:
                    os.remove(b_path)
                except OSError:
                    pass


def _maybe_rotate_log():
    global _LAST_LOG_ROTATE_CHECK
    now = time.time()
    if now - _LAST_LOG_ROTATE_CHECK >= _LOG_ROTATE_INTERVAL:
        _LAST_LOG_ROTATE_CHECK = now
        rotate_log_if_needed()


def get_daemon_info():
    pid_file = _get_pid_file()
    if not os.path.exists(pid_file):
        return None
    try:
        with open(pid_file, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if content.startswith("{"):
            data = json.loads(content)
            pid = int(data.get("pid", 0))
            version = data.get("version")
            script_mtime = data.get("script_mtime")
        else:
            pid = int(content)
            version = None
            script_mtime = None
        os.kill(pid, 0)
        cmdline_path = f"/proc/{pid}/cmdline"
        if os.path.exists(cmdline_path):
            with open(cmdline_path, "rb") as f:
                cmd = f.read().decode(errors="ignore")
                if "agy-pool" not in cmd and "python" not in cmd:
                    return None
        return {"pid": pid, "version": version, "script_mtime": script_mtime}
    except Exception:
        return None


def get_daemon_pid():
    info = get_daemon_info()
    return info["pid"] if info else None


def is_port_listening(port=None):
    if port is None:
        port = _get_default_port()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


def is_daemon_running():
    pid_getter = _get_daemon_pid_getter()
    pid = pid_getter()
    if not pid:
        return False
    port_listener = _get_port_listener()
    return port_listener()


def is_daemon_outdated():
    info = get_daemon_info()
    port_listener = _get_port_listener()
    if not info or not port_listener():
        return False
    running_ver = info.get("version")
    if not running_ver or running_ver != _get_version():
        return True
    try:
        curr_script = os.path.realpath(get_entrypoint_path())
        if os.path.exists(curr_script):
            curr_mtime = int(os.path.getmtime(curr_script))
            daemon_mtime = info.get("script_mtime")
            if daemon_mtime and curr_mtime > daemon_mtime:
                return True
    except Exception:
        pass
    return False


def start_proxy_daemon(foreground=False):
    rotate_log_if_needed()
    runner = _get_daemon_runner()
    outdated_checker = _get_daemon_outdated_checker()
    pid_getter = _get_daemon_pid_getter()
    stopper = _get_daemon_stopper()

    if not foreground:
        if runner():
            if outdated_checker():
                pid = pid_getter()
                print(f"{config.CLR_CYAN}Proxy daemon (PID: {pid}) is running outdated code. Restarting...{config.CLR_RESET}")
                stopper()
                time.sleep(0.5)
            else:
                print(f"{config.CLR_YELLOW}Proxy daemon is already running (PID: {pid_getter()}).{config.CLR_RESET}")
                return

        # Launch background process
        log_file = _get_log_file()
        pid_file = _get_pid_file()
        config._assert_safe_write_path(log_file)
        config._assert_safe_write_path(pid_file)
        cmd = [sys.executable, os.path.abspath(get_entrypoint_path()), "daemon-run"]
        storage.ensure_dirs()
        log_fd = os.open(log_file, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        os.fchmod(log_fd, 0o600)
        with os.fdopen(log_fd, "a") as logf:
            p = subprocess.Popen(cmd, stdout=logf, stderr=logf, start_new_session=True)
        for _ in range(20):
            if runner() or p.poll() is not None:
                break
            time.sleep(0.1)
        pid = pid_getter()
        if pid:
            print(f"{config.CLR_GREEN}✓ Antigravity load balancer gateway started on http://127.0.0.1:{_get_default_port()} (PID: {pid}){config.CLR_RESET}")
        elif p.poll() is not None and runner():
            return
        else:
            print(f"{config.CLR_RED}[Error] Failed to start gateway. Check {log_file}{config.CLR_RESET}")
        return

    # Foreground runner
    pid_file = _get_pid_file()
    try:
        with storage._file_lock(pid_file + ".lock", blocking=False):
            def stop_server(signum, frame):
                raise SystemExit(0)

            signal.signal(signal.SIGTERM, stop_server)
            signal.signal(signal.SIGINT, stop_server)
            try:
                curr_script = os.path.realpath(get_entrypoint_path())
                curr_mtime = int(os.path.getmtime(curr_script)) if os.path.exists(curr_script) else int(time.time())
                storage._atomic_json_write(pid_file, {
                    "pid": os.getpid(),
                    "version": _get_version(),
                    "script_mtime": curr_mtime,
                })

                def background_refresher():
                    while True:
                        time.sleep(180)
                        for acc in storage.load_pool().get("accounts", []):
                            if quota.quota_refresh_needed(acc):
                                quota.schedule_quota_refresh(acc)

                threading.Thread(target=background_refresher, daemon=True).start()
                server = proxy.ThreadedHTTPServer(("127.0.0.1", _get_default_port()), proxy.SmartProxyHandler)
                try:
                    server.serve_forever()
                finally:
                    server.server_close()
            finally:
                try:
                    with open(pid_file, "r", encoding="utf-8") as f:
                        raw = f.read().strip()
                    file_pid = None
                    if raw.startswith("{"):
                        file_pid = int(json.loads(raw).get("pid", 0))
                    elif raw:
                        file_pid = int(raw)
                    if file_pid == os.getpid():
                        config._assert_safe_write_path(pid_file)
                        os.unlink(pid_file)
                except (OSError, ValueError, json.JSONDecodeError):
                    pass
    except BlockingIOError:
        return


def stop_proxy_daemon():
    pid_getter = _get_daemon_pid_getter()
    pid = pid_getter()
    if not pid:
        print(f"{config.CLR_YELLOW}Proxy daemon is not running.{config.CLR_RESET}")
        return
    pid_file = _get_pid_file()
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(30):
            if pid_getter() is None:
                break
            time.sleep(0.1)
        if pid_getter() is None:
            print(f"{config.CLR_GREEN}✓ Proxy daemon (PID: {pid}) stopped.{config.CLR_RESET}")
        else:
            print(f"{config.CLR_YELLOW}Proxy daemon (PID: {pid}) is still stopping.{config.CLR_RESET}")
    except ProcessLookupError:
        print(f"{config.CLR_YELLOW}Process {pid} already dead.{config.CLR_RESET}")
    except Exception as e:
        print(f"{config.CLR_RED}[Error] Failed to stop daemon: {e}{config.CLR_RESET}")
    finally:
        if pid_getter() is None and os.path.exists(pid_file):
            config._assert_safe_write_path(pid_file)
            try:
                os.remove(pid_file)
            except OSError:
                pass


def ensure_daemon_running():
    """
    Ensures proxy daemon is running AND running the current version/script code.
    If daemon is running outdated code or stopped, automatically restarts it.
    """
    runner = _get_daemon_runner()
    outdated_checker = _get_daemon_outdated_checker()
    pid_getter = _get_daemon_pid_getter()
    stopper = _get_daemon_stopper()
    starter = _get_daemon_starter()

    if not runner():
        starter(foreground=False)
    elif outdated_checker():
        pid = pid_getter()
        sys.stderr.write(f"{config.CLR_CYAN}[agy-pool] Hot-reloading gateway daemon with latest updates (PID {pid})...{config.CLR_RESET}\n")
        sys.stderr.flush()
        stopper()
        time.sleep(0.5)
        starter(foreground=False)


__all__ = [
    "_LAST_LOG_ROTATE_CHECK",
    "_LOG_ROTATE_INTERVAL",
    "_format_size",
    "rotate_log_if_needed",
    "clear_log",
    "_maybe_rotate_log",
    "get_daemon_info",
    "get_daemon_pid",
    "is_port_listening",
    "is_daemon_running",
    "is_daemon_outdated",
    "start_proxy_daemon",
    "stop_proxy_daemon",
    "ensure_daemon_running",
    "set_entrypoint_path",
    "get_entrypoint_path",
    "set_pid_file_provider",
    "set_log_file_provider",
    "set_port_provider",
    "set_version_provider",
    "set_port_listener",
    "set_daemon_runner",
    "set_daemon_outdated_checker",
    "set_daemon_pid_getter",
    "set_daemon_stopper",
    "set_daemon_starter",
]

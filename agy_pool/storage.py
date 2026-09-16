"""
Storage primitives and persistence operations for agy-pool.
"""

import os
import sys
import json
import tempfile
import threading
import contextlib
from agy_pool import config


POOL_LOCK = threading.RLock()
FILE_LOCKS = {}
FILE_LOCKS_LOCK = threading.Lock()


def ensure_dirs():
    config._assert_safe_write_path(config.GEMINI_DIR)
    config._assert_safe_write_path(config.AGY_CLI_DIR)
    os.makedirs(config.GEMINI_DIR, mode=0o700, exist_ok=True)
    os.makedirs(config.AGY_CLI_DIR, mode=0o700, exist_ok=True)


def _empty_pool():
    return {
        "version": 1,
        "strategy": "max_quota",  # max_quota | round_robin
        "active_account_id": None,
        "accounts": []
    }


def _thread_file_lock(path):
    with FILE_LOCKS_LOCK:
        return FILE_LOCKS.setdefault(path, threading.RLock())


@contextlib.contextmanager
def _file_lock(path, exclusive=True, blocking=True):
    """Lock a stable sidecar inode across both threads and POSIX processes."""
    config._assert_safe_write_path(path)
    thread_lock = _thread_file_lock(path)
    if not thread_lock.acquire(blocking=blocking):
        raise BlockingIOError("lock is already held")
    try:
        os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            os.fchmod(fd, 0o600)
            if config.HAS_FCNTL and config.fcntl is not None:
                operation = config.fcntl.LOCK_EX if exclusive else config.fcntl.LOCK_SH
                if not blocking:
                    operation |= config.fcntl.LOCK_NB
                config.fcntl.flock(fd, operation)
            yield
        finally:
            if config.HAS_FCNTL and config.fcntl is not None:
                config.fcntl.flock(fd, config.fcntl.LOCK_UN)
            os.close(fd)
    finally:
        thread_lock.release()


def _read_pool_unlocked():
    if not os.path.exists(config.POOL_CONFIG_FILE):
        return _empty_pool()
    with open(config.POOL_CONFIG_FILE, "r", encoding="utf-8") as f:
        pool = json.load(f)
    if not isinstance(pool, dict) or not isinstance(pool.get("accounts"), list):
        raise ValueError("pool state must be an object containing an accounts list")
    return pool


def _atomic_json_write(path, data):
    config._assert_safe_write_path(path)
    directory = os.path.dirname(path)
    os.makedirs(directory, mode=0o700, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        try:
            dir_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def load_pool():
    ensure_dirs()
    try:
        with POOL_LOCK, _file_lock(config.POOL_CONFIG_FILE + ".lock", exclusive=False):
            return _read_pool_unlocked()
    except Exception as e:
        sys.stderr.write(f"{config.CLR_RED}[Error] Failed to read {config.POOL_CONFIG_FILE}: {e}{config.CLR_RESET}\n")
        return _empty_pool()


def pool_transaction(mutator):
    """Run one read-modify-write transaction against the JSON pool."""
    ensure_dirs()
    with POOL_LOCK, _file_lock(config.POOL_CONFIG_FILE + ".lock"):
        pool = _read_pool_unlocked()  # Corrupt state aborts; it is never replaced with an empty pool.
        result = mutator(pool)
        _atomic_json_write(config.POOL_CONFIG_FILE, pool)
        return result


def save_pool(data):
    """Replace the pool under the transaction lock; retained for compatibility."""
    def replace(pool):
        pool.clear()
        pool.update(data)
    pool_transaction(replace)


def _find_account(pool, account):
    account_id = account.get("id")
    email = account.get("email")
    return next((a for a in pool.get("accounts", [])
                 if (account_id and a.get("id") == account_id) or
                    (email and a.get("email") == email)), None)


def _next_account_id(accounts):
    used = {a.get("id") for a in accounts}
    number = 1
    while f"acc_{number}" in used:
        number += 1
    return f"acc_{number}"

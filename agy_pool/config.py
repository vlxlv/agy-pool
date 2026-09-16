"""
Configuration, constants, runtime path resolution, and persistent state safety
for agy-pool.
"""

import os
import sys

try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    fcntl = None
    HAS_FCNTL = False

VERSION = "0.1.0-beta.1"
QUOTA_FRESH_MAX_AGE = 60
QUOTA_AGING_MAX_AGE = 300
_QUOTA_REFRESH_BACKOFF = (30, 60, 120, 240, 300)

# ANSI Color Codes
CLR_RESET = "\033[0m"
CLR_BOLD = "\033[1m"
CLR_DIM = "\033[2m"
CLR_GREEN = "\033[32m"
CLR_YELLOW = "\033[33m"
CLR_BLUE = "\033[34m"
CLR_MAGENTA = "\033[35m"
CLR_CYAN = "\033[36m"
CLR_RED = "\033[31m"


def resolve_gateway_port(env_val=None):
    """
    Resolves and validates the gateway TCP port.
    Returns 8899 if env_val (or AGY_PORT environment variable) is unset or empty.
    Raises ValueError on invalid string, non-integer, 0, or port > 65535.
    """
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


try:
    DEFAULT_PORT = resolve_gateway_port()
except ValueError as _e:
    sys.stderr.write(f"[Error] {_e}\n")
    sys.exit(1)

DEFAULT_MAX_LOG_BYTES = 5 * 1024 * 1024  # 5 MB
DEFAULT_BACKUP_LOG_COUNT = 1
MAX_LOG_BYTES = int(os.environ.get("AGY_LOG_MAX_BYTES", DEFAULT_MAX_LOG_BYTES))
BACKUP_LOG_COUNT = int(os.environ.get("AGY_LOG_BACKUP_COUNT", DEFAULT_BACKUP_LOG_COUNT))

# ----------------- Path Configuration & Test Isolation Guard -----------------
def _detect_real_production_gemini_dir():
    explicit = os.environ.get("AGY_REAL_GEMINI_DIR")
    if explicit:
        return os.path.realpath(os.path.abspath(os.path.expanduser(explicit)))
    try:
        import pwd
        real_home = pwd.getpwuid(os.getuid()).pw_dir
        if real_home:
            return os.path.realpath(os.path.join(real_home, ".gemini"))
    except Exception:
        pass
    return os.path.realpath(os.path.abspath(os.path.expanduser("~/.gemini")))


_REAL_PRODUCTION_GEMINI_DIR = _detect_real_production_gemini_dir()
_FORBIDDEN_WRITE_DIRS = {
    _REAL_PRODUCTION_GEMINI_DIR,
    os.path.abspath(_REAL_PRODUCTION_GEMINI_DIR),
}
_TEST_MODE = bool(os.environ.get("AGY_TEST_MODE") == "1")


def is_test_mode():
    """Return True if running under isolated test mode."""
    return bool(_TEST_MODE or os.environ.get("AGY_TEST_MODE") == "1")


def set_test_mode(enabled=True):
    """Enable or disable fail-closed test mode."""
    global _TEST_MODE
    _TEST_MODE = bool(enabled)
    if enabled:
        os.environ["AGY_TEST_MODE"] = "1"
    else:
        os.environ.pop("AGY_TEST_MODE", None)


def register_forbidden_path(path):
    """Register an additional path that must never be written to in test mode."""
    if path:
        p = os.path.abspath(os.path.expanduser(str(path)))
        _FORBIDDEN_WRITE_DIRS.add(os.path.realpath(p))
        _FORBIDDEN_WRITE_DIRS.add(p)


def _assert_safe_write_path(path):
    """
    Fail-closed test guard: prevent tests from ever writing to production or protected paths.
    Raises RuntimeError immediately if in test mode and path targets a forbidden directory.
    """
    if not is_test_mode() or not path:
        return
    try:
        real_path = os.path.realpath(os.path.abspath(os.path.expanduser(str(path))))
    except Exception as e:
        raise RuntimeError(
            f"[FAIL-CLOSED TEST GUARD] Failed to resolve path safely: {path}"
        ) from e
    for forbidden in _FORBIDDEN_WRITE_DIRS:
        if real_path == forbidden or real_path.startswith(forbidden + os.sep):
            raise RuntimeError(
                f"[FAIL-CLOSED TEST GUARD] Refusing to write to protected path during test mode: {real_path}"
            )


_ON_CONFIGURE_HOOKS = []


def register_configure_hook(hook):
    """Register a callback to be invoked whenever configure_paths() runs."""
    if hook not in _ON_CONFIGURE_HOOKS:
        _ON_CONFIGURE_HOOKS.append(hook)


def configure_paths(gemini_dir=None):
    """
    Configure runtime storage paths.
    If gemini_dir is provided, paths are rooted at gemini_dir.
    Otherwise, uses AGY_GEMINI_DIR environment variable or defaults to ~/.gemini.
    Returns configured GEMINI_DIR.
    """
    global GEMINI_DIR, POOL_CONFIG_FILE, PID_FILE, LOG_FILE, AGY_CLI_DIR, AGY_TOKEN_FILE
    if gemini_dir is not None:
        base = os.path.abspath(os.path.expanduser(gemini_dir))
    elif os.environ.get("AGY_GEMINI_DIR"):
        base = os.path.abspath(os.path.expanduser(os.environ["AGY_GEMINI_DIR"]))
    else:
        base = os.path.abspath(os.path.expanduser("~/.gemini"))

    if is_test_mode() and gemini_dir is not None:
        _assert_safe_write_path(base)

    GEMINI_DIR = base
    POOL_CONFIG_FILE = os.path.join(GEMINI_DIR, "agy-pool-accounts.json")
    PID_FILE = os.path.join(GEMINI_DIR, "agy-pool.pid")
    LOG_FILE = os.path.join(GEMINI_DIR, "agy-pool.log")
    AGY_CLI_DIR = os.path.join(GEMINI_DIR, "antigravity-cli")
    AGY_TOKEN_FILE = os.path.join(AGY_CLI_DIR, "antigravity-oauth-token")

    for hook in _ON_CONFIGURE_HOOKS:
        hook()

    return GEMINI_DIR


# Initialize default paths from environment or default location
configure_paths()

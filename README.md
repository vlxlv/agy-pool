# agy-pool: Antigravity Multi-Account Quota Pool & Intelligent Load Balancer Suite

[![Version](https://img.shields.io/badge/version-0.1.0--alpha.10-blue.svg)](CHANGELOG.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Termux%20%7C%20Linux%20%7C%20macOS-green.svg)](#)
[![Python: 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](#)
[![Dependencies](https://img.shields.io/badge/dependencies-0%20(standard%20library)-brightgreen.svg)](#)
[![100% Autonomous AI](https://img.shields.io/badge/built%20by-100%25%20Autonomous%20AI-purple.svg)](#)

A zero-dependency multi-account quota pool and local reverse proxy for **Antigravity CLI (`agy`)** on Termux / Android Linux and standard POSIX environments.

> [!NOTE]
> ### 🤖 100% Autonomous AI Artifact & Engineering
> **Every single line of code, system architecture, troubleshooting workflow, unit test suite, and even the project repository name (`agy-pool`) was conceived, designed, implemented, and maintained 100% autonomously by AI (Antigravity). Not a single line of code in this repository was written by a human.**

---

## Key Capabilities

- **Zero External Dependencies**: Built 100% on the Python 3 standard library (`urllib`, `http.server`, `sqlite3`, `fcntl`, `hashlib`, `hmac`). Runs instantly on any Termux or Linux system with no `pip` or wheel compilation.
- **Intelligent Load Balancing & Fast Failover**: Dynamically routes CLI generation requests based on configurable strategies (`max_quota`, `least_used`, `round_robin`). Automatically fails over in-flight requests (<100ms) upon hitting HTTP 429 or quota exhaustion, dynamically respecting upstream `Retry-After` headers.
- **Automated Hot-Reload & Self-Healing Daemon**: Tracks in-memory daemon bytecode version and script mtime, seamlessly hot-reloading background proxy instances (~0.5s) upon disk code updates or CLI invocations to prevent stale metric drift.
- **Built-in System Diagnostics (`agy-pool doctor`)**: Instant self-test verifying runtime environment, native binary detection, daemon status, pool integrity, and live Google Cloud Code TLS connectivity.
- **Cross-Device Migration & Encrypted Backup**: Easily backup and restore entire account pools across Termux, VPS, or desktop machines via `export` and `import`. Supports tamper-proof passphrase encryption (PBKDF2-HMAC-SHA256 + CTR keystream), automatic deduplication/merging, and Unix pipe streaming (`-`).
- **Account-Agnostic Session Continuity (`agy -c`)**: Automatically queries `conversation_summaries.db` to identify the most recent session for the current workspace directory, allowing seamless workflow resumption across different accounts.
- **Security Isolation & Self-Healing**: Detects Google Cloud Code verification challenges (`VALIDATION_REQUIRED` / 403) and token revocations, isolates restricted accounts to prevent quota deadlocks, and provides one-click browser verification (`agy-pool verify`).
- **Zero-Maintenance Footprint**: Real-time uncompressed SSE token streaming with official User-Agent preservation, automatic in-place log rotation (`copytruncate` strictly capping log size under 10 MB), and dual-mode fallback (`agy` for load-balanced proxy, `agy-raw` for direct Google connection).

---

## Quick Installation

### Option A: Install from Standalone Tarball (Recommended)
```bash
# Extract into your home directory
tar -xzvf agy-pool.tar.gz -C ~

# Run the automated installer
cd ~/agy-pool && bash install.sh
```

### Option B: Clone from Source
```bash
git clone https://github.com/midori01/agy-pool.git ~/agy-pool
cd ~/agy-pool && bash install.sh
```

The installer automatically validates the Python 3 runtime, installs global symlinks (`agy-pool`, `agy-raw`, `agy-orig`), configures shell aliases (for both Bash and Zsh), and imports existing Antigravity credentials as Account #1.

---

## Command Cheat Sheet

### 1. Daily CLI Usage

| Command | Description | Best For |
| :--- | :--- | :--- |
| **`agy`** | Launch session with automatic multi-account load balancing & failover | **Daily default use** |
| **`agy -c`** | Automatically resume previous session in current workspace across accounts | Continuing ongoing work |
| **`agy-raw`** | Connect directly to Google Cloud Code (bypasses proxy gateway) | Diagnostics & network fallback |
| **`agy-orig`** | Alias for `agy-raw` | Same as above |

> Native `agy` arguments and options pass through transparently.

---

### 2. Account Pool & Quota Management

| Command | Alias / Parameters | Description |
| :--- | :--- | :--- |
| `agy-pool status` | - | Display gateway daemon status, active account, and cached quotas |
| `agy-pool quota` | `agy-pool list` | Refresh and display quota progress bars, reset countdowns, and hits |
| `agy-pool strategy [name]` | `strat` | View or switch load balancing strategy (`max_quota`, `least_used`, `round_robin`) |
| `agy-pool doctor` | `check`, `health` | Run end-to-end system, network, and account diagnostic suite |
| `agy-pool rename <ID>` | e.g. `rename 1 "Work"` | Assign a custom friendly name or label to an account |
| `agy-pool login` | - | Authenticate and add a new Google account via system browser |
| `agy-pool import-current` | - | Import current active `~/.gemini/` credentials into the pool |
| `agy-pool switch auto` | - | Switch active account to the one with highest available quota |
| `agy-pool switch <ID/Email>` | e.g. `switch 2` | Manually designate active account |
| `agy-pool verify [ID/Email]` | e.g. `verify 3` | Open Google Cloud Code security verification flow in browser |
| `agy-pool remove <ID/Email>` | e.g. `remove 2` | Remove account from pool |

> **Load balancing strategies**:
> - `max_quota` (default): Prioritizes accounts with the safest usable 5-hour and weekly quota headroom, taking both reset times into account.
> - `least_used`: Distributes load evenly to accounts with lowest AI generation count (Hits).
> - `round_robin`: Cycles through ready accounts in sequential rotation.

> **Note on targeting**: Numeric index targeting (e.g. `rename 1 "Work"`, `switch 2`, `remove 2`) is provided for interactive CLI convenience, but index numbers may shift after account removal, import, or replacement. For automated scripts and workflows, always prefer using the stable account ID or exact email address.

```text
====================================================================
                 Antigravity Multi-Account Pool
====================================================================
[1] user1@gmail.com (Alice)   [* Active]   Hits: 45
    • Gemini 5-Hour: [███████░░░]  67.3%  (Resets in 3h 59m)
    • Gemini Weekly: [█████████░]  94.6%  (Resets in 6d 9h)

[2] user2@gmail.com (Bob)     [Ready]      Hits: 30
    • Gemini 5-Hour: [█████████░]  94.9%  (Resets in 3h 45m)
    • Gemini Weekly: [████████░░]  81.3%  (Resets in 3d 19h)

[3] user3@gmail.com (Charlie) [Exhausted]  Hits: 12
    • Gemini 5-Hour: [██████████] 100.0%  (Resets in 5h 0m)
    • Gemini Weekly: [░░░░░░░░░░]   0.0%  (Resets in 2d 20h)
--------------------------------------------------------------------
 [* Active] CLI Base Token    [Ready] In Rotation Pool
 [Cooldown] Rate Limited      [Exhausted] Quota Depleted
 Strategy: max_quota | Gateway Proxy: RUNNING (127.0.0.1:8899)
====================================================================
```

---

### 3. Gateway Daemon & Log Management

```bash
# Daemon Control (starts automatically on 'agy' if stopped)
agy-pool start          # Start background gateway daemon
agy-pool stop           # Stop background gateway daemon
agy-pool restart        # Restart gateway daemon

# Log Management (auto-rotated at 5 MB, max 10 MB footprint)
agy-pool log            # View log status and recent proxy entries
agy-pool log -f         # Follow log in real-time (live stream)
agy-pool log --rotate   # Force immediate rotation
agy-pool log --clear    # Truncate active log and clear backups
```

---

### 4. Cross-Device Backup & Migration

| Command | Options | Description |
| :--- | :--- | :--- |
| `agy-pool export [file]` | `-e, --encrypt`, `-p`, `--no-stats` | Export pool to JSON or encrypted bundle (default: `0600` permissions) |
| `agy-pool import <file>` | `-p`, `--replace`, `--skip-existing` | Restore and merge accounts from backup (supports `-` for stdin) |

```bash
# 1. Quick plain backup & restore
agy-pool export backup.json
agy-pool import backup.json

# 2. Tamper-proof encrypted backup (PBKDF2 + HMAC-CTR stream cipher)
agy-pool export -e backup.enc
agy-pool import backup.enc

# 3. Direct device-to-device migration over SSH pipe
ssh phone1 agy-pool export - | agy-pool import -
```

---

## FAQ & Diagnostics

### Q1: How do I customize the proxy gateway port?
Set the `AGY_PORT` environment variable (e.g. `export AGY_PORT=9000`). The gateway defaults to `8899` and strictly validates port numbers between `1` and `65535`.

### Q2: How do I verify whether my session is routed through the gateway?
- **Live Logs**: Run `agy-pool log -f` in another tab to observe real-time proxy dispatch.
- **Hits Counter**: Run `agy-pool status` before and after a prompt to see the request counter increment.
- **Process Environment**: Inspect `CLOUD_CODE_URL`:
  ```bash
  tr '\0' '\n' < /proc/$(pgrep -f "agy" | head -n 1)/environ 2>/dev/null | grep CLOUD_CODE_URL
  ```

### Q2: Which account state is authoritative during a proxied session?
Each proxied HTTP request uses the account selected by the gateway and its request-level `Authorization` header. The native token file (`~/.gemini/antigravity-cli/antigravity-oauth-token`) serves as compatibility state synced at session launch and manual account switches. In-flight failovers do not rewrite disk credentials to prevent lock contention across concurrent sessions.

---

## Live Integration & Scheduler Testing

`agy-pool` includes a dedicated live integration smoke-test harness (`scripts/live-test.sh`) to validate gateway routing, process concurrency, and scheduler rotation on real Linux/VPS environments with native `agy` and live Google accounts.

> [!IMPORTANT]
> Real live tests are manual-only. They consume real account quota. They are never executed automatically by GitHub Actions.

### Manual Live Testing (Over SSH)

When validating on a real VPS or Linux host over SSH, execute the test harness directly:

```bash
# Diagnostic environment check (0 quota consumed)
./scripts/live-test.sh doctor

# Verify single generation through the gateway proxy
./scripts/live-test.sh gateway

# Verify sequential cyclic rotation across ready accounts
./scripts/live-test.sh round-robin -n 9

# Verify dispatch prioritization for lowest-hit accounts
./scripts/live-test.sh least-used -n 6

# Verify dispatch priority to highest remaining quota accounts
./scripts/live-test.sh max-quota -n 3
```

### Key Options & Safety
- `-n, --runs <N>`: Specify number of generation requests (e.g. `-n 9`, `-n 6`, `-n 3`).
- `-t, --timeout <sec>`: Per-request timeout in seconds (default: 30s).
- `-k, --keep-artifacts`: Preserve temporary test logs in `/tmp/agy-live-test.*` for post-mortem debugging.
- `-j, --json`: Produce machine-readable JSON reports.
- `-y, --yes`: Auto-confirm live quota consumption warning.
- **Safety**: Uses POSIX `trap` on `EXIT`, `INT`, and `TERM` to restore original load balancing strategy.
- **Verification**: Uses gateway log delta tracking for authoritative routing proof, complemented by Hits delta GCD analysis and 3-tier concurrent traffic detection (`CLEAN`, `INCONCLUSIVE`, `CONCURRENT`).
- **Zero Runtime Dependencies**: Requires only Bash and Python 3.8+ standard library.

---

## Uninstallation

To cleanly remove `agy-pool` and restore original environment:
```bash
cd ~/agy-pool && bash uninstall.sh
```

---

## Changelog & Versioning

`agy-pool` follows [Semantic Versioning](https://semver.org/). See [CHANGELOG.md](CHANGELOG.md) for detailed version release notes and feature history.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

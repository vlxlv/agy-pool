# Changelog

All notable changes to the `agy-pool` project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-beta.1] - 2026-09-16

### Added
- **Account Privacy & Friendly Display Names**:
  - Replaced account email exposure across CLI tables, doctor diagnostics, logs, and live-test outputs with custom friendly names (`agy-pool rename`) or safe anonymous fallbacks (`Account 1`, `Account 2`, ...).
  - Preserved internal account ID stability and email matching for authentication, token refresh, and manual CLI targeting.
- **Reset-Aware 5H + Weekly Quota Scheduling**:
  - Unified capacity scoring incorporating both Gemini 5-hour and 7-day weekly reset horizons (`worst_pace`, `total_pace`, `raw_floor`).
  - Quota freshness classification (`fresh`, `aging`, `stale`, `unknown`) to prevent stale snapshot distortion during candidate ranking.
  - Partial and unknown quota correctness ensuring legacy and single-window accounts participate safely without ungrounded assumption of full capacity.
- **Quota Freshness, Asynchronous Refresh & Retry Backoff**:
  - Background asynchronous quota refresh with single-flight deduplication and exponential retry backoff under upstream failures.
- **Multi-Strategy Load Balancing Engine**:
  - Comprehensive support for `max_quota` (reset-aware pace headroom), `least_used` (Hits-first distribution with quota capacity tie-breaking), and `round_robin` (cyclic rotation).
- **Manual Live Scheduler Validation Harness**:
  - Offline parser regression test suite and manual VPS/Linux live-test harness (`scripts/live-test.sh`, `scripts/live_test.py`) validating real gateway routing, concurrent multi-dispatch, and failover without automated CI quota consumption.
- **Installer, Upgrade & Diagnostics Improvements**:
  - Idempotent `install.sh` and `uninstall.sh` lifecycle managing global symlinks and shell integration (`~/.bashrc`, `~/.zshrc`).
  - System diagnostics (`agy-pool doctor`) validating runtime environment, process concurrency, credentials, and TLS connectivity.
  - Backward-compatibility and lifecycle tests guaranteeing safe migration from alpha states and state preservation across daemon hot-reloads.

### Changed
- Promoted `agy-pool` to its first beta release candidate (`v0.1.0-beta.1`).
- Hardened `format_remaining_time` to reliably handle numeric epoch timestamps and ISO 8601 strings without type errors.

### Fixed
- Fixed race conditions during concurrent request selection in `round_robin` by advancing cursor atomically during transaction.
- Fixed premature score truncation in `max_quota` scheduling.
- Fixed sensitive email exposure in CLI listings, daemon logs, doctor outputs, and test logs.

### Known Beta Limitations
- Quota metrics reflect upstream snapshot timestamps rather than local in-flight consumption estimation.
- Live scheduler integration tests require live Google credentials and manual invocation; standard CI remains strictly offline.

## [0.1.0-alpha.10] - 2026-09-15

### Added
- Reset-aware quota capacity scoring using both 5-hour and weekly reset horizons.
- Live scheduler validation harness (`scripts/live-test.sh`, `scripts/live_test.py`) for gateway routing and concurrency verification.
- Comprehensive offline parser and verifier regression coverage for multidispatch, auxiliary traffic, and rotation handling.

### Changed
- `max_quota` now prioritizes reset-aware usable headroom (`worst_pace`, `total_pace`, `raw_floor`, full float precision) rather than static quota percentage alone.
- `least_used` uses reset-aware capacity as tie-break information while maintaining Hits-first primary ordering.
- Round-robin cursor semantics changed to atomic reservation-at-selection within the pool transaction.
- Live scheduler reporting distinguishes internal multidispatch and generation failover from external concurrency.

### Fixed
- Prevented duplicate account selection under concurrent generation requests in `round_robin`.
- Eliminated premature quota score rounding and unintended over-weighting of Hits in `max_quota`.
- Corrected auxiliary gateway traffic misclassification in live scheduler tests.
- Handled log rotation and truncation safely during live-test delta parsing.
- Corrected installer and uninstaller script executable modes (`100755`).
- Removed redundant `agy-raw` shell self-alias.

## [0.1.0-alpha.9] - 2026-09-15

### Added
- **Universal Portability & Multi-Shell Integration**:
  - Replaced Termux-specific shebangs (`/data/data/com.termux/...`) in `bin/agy-pool` and `bin/agy-raw` with POSIX-standard `#!/usr/bin/env` for out-of-the-box execution across Linux, macOS, WSL, and BSD.
  - Enhanced `install.sh` and `uninstall.sh` to automatically detect and configure aliases in Zsh (`~/.zshrc`) alongside Bash (`~/.bashrc`).
- **Dynamic HTTP 429 `Retry-After` Header Parsing**:
  - Automatically extracts and respects upstream `Retry-After` headers (both integer seconds and RFC HTTP dates) when accounts hit rate limits, replacing the static 300s penalty and preventing excessive lockouts on brief throttles.
- **Configurable Multi-Strategy Load Balancing Engine (`strategy`)**:
  - Implemented dynamic request routing strategies:
    - `max_quota` (default): Prioritizes accounts with highest remaining quota, breaking ties with lowest AI generation count (`Hits`).
    - `least_used`: Distributes inference generations evenly across healthy accounts by prioritizing lowest generation count (`gen_count`).
    - `round_robin`: Cycles sequentially among healthy pool accounts using least-recently-used timestamps.
  - Added dedicated `agy-pool strategy [name]` CLI command to inspect and update load-balancing policies.
- **Built-in System Diagnostics (`agy-pool doctor`)**:
  - Added comprehensive `agy-pool doctor` CLI command (with fast dispatch) checking Python runtime, native `agy` binary detection and version, gateway daemon state, pool token validity, SQLite database accessibility, and TLS connectivity to Google Cloud Code (`daily-cloudcode-pa.googleapis.com:443`).
- **Account Renaming & Custom Aliases (`agy-pool rename`)**:
  - Added `agy-pool rename <ID/Email> <Name>` to allow designating friendly labels (e.g. "Work", "Personal", "Backup") in quota dashboards.
- **Configurable Port via `AGY_PORT`**:
  - Allows overriding default port `8899` via the `AGY_PORT` environment variable to prevent local port collisions.
- **Session Continuity Path Unquoting (`-c`)**:
  - Added URL unquoting in `find_latest_conversation_for_dir` to ensure workspaces containing spaces or special characters match SQLite records seamlessly.
  - Supported `--conversation=...` syntax in `resolve_continue_arg`.

## [0.1.0-alpha8] - 2026-09-15

### Added
- **Automated Gateway Daemon Hot-Reload Guard (`ensure_daemon_running`)**:
  - Automatically verifies in-memory daemon bytecode version and script file modification timestamp (`script_mtime`) stored in `PID_FILE` against active CLI code on disk.
  - Transparently and gracefully hot-restarts the gateway proxy daemon (~0.5s) upon running `agy` or `agy-pool start` whenever code updates occur, permanently preventing stale in-memory execution or frozen metrics across upgrades.
- **Daemon Metadata & Health Observability**:
  - `PID_FILE` structured JSON serialization storing PID, loaded code version, and launch timestamp with full backward-compatibility for legacy integer PID files.
  - Enhanced `agy-pool status` to display running daemon version (e.g., `PID: 5263 [v0.1.0-alpha8]`) and explicitly warn when the running process is executing outdated disk code (`⚠ Outdated Code`).
  - Added native `agy-pool version` CLI subcommand and fast dispatch.
- **Robust Daemon Cleanup on Exit**:
  - Hardened daemon process exit handlers to directly inspect PID file ownership on shutdown, ensuring reliable PID file cleanup across environments and test runners.

### Fixed
- **Pure AI Generation Metric Tracking (`Hits`)**:
  - Fixed an issue where a long-running proxy daemon instance in memory could retain pre-alpha6 bytecode, causing `gen_count` to stay frozen while quota decreased during user requests.

## [0.1.0-alpha7] - 2026-09-15

### Added
- **Cross-Device Account Pool Backup & Migration (`export` / `import`)**:
  - `agy-pool export`: Dumps all accounts, OAuth refresh tokens, active credential designation, and scheduler configurations to an atomic JSON backup file (strictly enforcing `0600` permissions).
  - `agy-pool import`: Ingests backup files into local pool storage with automatic account deduplication and non-destructive merging (updates existing tokens and appends new accounts).
  - Supports `--replace` to overwrite local pool entirely, and `--skip-existing` to protect existing local tokens.
  - Supports standard input/output streaming (`-`) allowing direct pipeline migration over SSH (`ssh remote agy-pool export - | agy-pool import -`).
- **Zero-Dependency Authenticated Passphrase Encryption**:
  - Implemented standard-library-only authenticated encryption for backup bundles via PBKDF2-HMAC-SHA256 (100,000 rounds) key derivation, HMAC-SHA256 CTR stream cipher, and Encrypt-then-MAC authentication tag verification.
  - Securely encrypts sensitive OAuth tokens with `agy-pool export -e` (or `-p / --password`), preventing plaintext credential leakage when backups are transferred across unsecure media.
  - Automatically identifies encrypted backups on `agy-pool import` and securely prompts for passphrase.
- **Comprehensive Unit Testing**:
  - Added 4 test suites in `tests/test_agy_pool.py` covering cryptographic round-trips, tampering detection, merge/replace conflict handling, and Unix permissions (35 tests total).

## [0.1.0-alpha6] - 2026-09-15

### Added
- **Pure AI Generation Metric Tracking (`Hits`)**:
  - Differentiates between actual model reasoning/generation requests (`streamGenerateContent`, `generateContent`) and lightweight control-plane metadata calls (`listExperiments`, `loadCodeAssist`, `fetchUserInfo`, etc.).
  - Tracks `gen_count` separately from overall `request_count`, displaying pure inference calls under `Hits: <N>` in the dashboard to eliminate confusion and quota ambiguity.
- **Generation-Aware Dynamic Load Balancing**:
  - Generation request scheduler now breaks quota ties using actual AI generation count (`gen_count`) rather than raw request count, ensuring fairer compute balancing.
- **Accurate Status Indicators & Quota Exhaustion Detection**:
  - Automatically identifies depleted quota (`g5_frac <= 0.005` or `gw_frac <= 0.005`) and marks accounts as `[Exhausted]` (or `* Active (Exhausted)` for active compatibility base) instead of misleading `[Ready]`.
  - Accurately reflects temporary rate limits with `[Cooldown]`.
  - Added mobile-optimized 2x2 badge legend: `[* Active] CLI Base Token`, `[Ready] In Rotation Pool`, `[Cooldown] Rate Limited`, `[Exhausted] Quota Depleted`.

## [0.1.0-alpha5] - 2026-09-14

### Added
- **Automatic In-Place Log Rotation (`copytruncate`)**:
  - Implemented zero-dependency automated log rotation preserving open file descriptors across background daemons and child processes.
  - Automatically triggers when active log reaches 5 MB (configurable via `AGY_LOG_MAX_BYTES`), rotating to `agy-pool.log.1` and truncating the active log to cap total disk usage strictly under 10 MB.
  - Added periodic rate-limited size monitoring during proxy requests and at daemon startup.
- **New `log` / `logs` Management CLI Subcommand**:
  - `agy-pool log`: Displays current log path, file size, line count, backup status, and recent log entries.
  - Supports `-n / --lines <N>` to customize output lines.
  - Supports `-f / --follow` for live streaming log output (`tail -f` behavior).
  - Supports `--rotate` to force immediate rotation.
  - Supports `--clear / --clean` to safely truncate the active log and remove backups.

## [0.1.0-alpha4] - 2026-09-14

### Added
- **Security Validation Detection & Automatic Failover**:
  - Automatically identifies Google Cloud Code security challenges (`VALIDATION_REQUIRED` / 403 `Verify your account to continue`) and auth token revocations across both quota probes and active generation requests.
  - Instantly isolates restricted accounts and fails over in-flight requests (<100ms) to other healthy pool accounts before response commitment, preventing client deadlocks.
- **Dedicated `verify` CLI Subcommand**:
  - Added `agy-pool verify [target]` to query the latest Cloud Code security verification URL and launch it directly in the system browser (`termux-open-url`, `xdg-open`, etc.).
- **Visual Restriction & Actionable Diagnostics**:
  - Renders explicit `[⚠ Verify Required]` and `[✖ Auth Error]` status markers in `agy-pool quota` / `agy-pool list`, preventing deceptive 100% quota readings and instructing users on exact recovery commands.

### Fixed & Hardened
- **Quota Probe Error Isolation**: Prevents invalid fallback to `fetchAvailableModels` when accounts are blocked with 403 `VALIDATION_REQUIRED`, eliminating misleading `(Resets N/A)` progress bars.
- **Auto-Selection Isolation**: `switch auto` and generation request selection strictly filter out and deprioritize restricted accounts.
- **Quota Refresh Transaction Isolation**: Separated `REFRESH_PERSIST_FIELDS` from `request_count` and `error_count` to eliminate race conditions between background quota refreshers and concurrent request handlers.

## [0.1.0-alpha3] - 2026-09-14

### Added
- **Comprehensive Automated Test Suite**: Integrated 23 rigorous unit tests in `tests/test_agy_pool.py` covering multi-process file locking, single-flight token refresh, pre-stream failover, and request body framing.
- **Automated CI Workflow**: GitHub Actions workflow running tests across commits and pull requests.

### Fixed & Hardened
- **Process State Concurrency & Atomic Transactions**: Introduced `pool_transaction` with transactional read-modify-write semantics and sidecar file locks, preventing lost updates and data corruption under high concurrent load.
- **Single-Flight Per-Account Token Refresh**: Added fine-grained per-account locks during token refresh to eliminate thundering herd requests to Google OAuth.
- **HTTP/1.1 RFC 7230 Compliant Proxying**: Hardened chunked request body parsing with size and trailer validation; dynamic hop-by-hop header removal derived from `Connection`.
- **Committed Stream Truncation Protection**: Prevents replaying requests to subsequent accounts if a generation stream fails after response headers have already been committed to the client.
- **SQLite Read-Only Session Continuity**: Querying conversation summaries uses strictly read-only connections (`?mode=ro`, `PRAGMA query_only=ON`) with busy timeout to avoid write lock contention with native `agy`.

## [0.1.0-alpha2] - 2026-09-14

### Added
- **Account-Agnostic Workspace Session Continuity (`agy -c`)**:
  - Automatically queries `~/.gemini/antigravity-cli/conversation_summaries.db` to locate the true latest conversation for the current workspace directory (with hierarchical parent-directory walk-up support).
  - Resolves `-c` / `--continue` directly into `--conversation <cid>` when a matching record is available.
  - Detects active process presence locks and warns about parallel use.

## [0.1.0-alpha] - 2026-09-14

### Added
- **Intelligent Quota Load Balancer Gateway**: Zero-dependency reverse proxy daemon (`http://127.0.0.1:8899`) distributing CLI requests across multiple Google accounts.
- **Pre-Stream 429 Failover**: Intercepts HTTP 429 (ResourceExhausted) or recognized quota errors before response commit and retries with a healthy account.
- **Active Account Auto-Promotion**: Promotes succeeding failover accounts as the primary active account to avoid redundant failover overhead on future turns.
- **Token Streaming**: Pass-through of Server-Sent Events (`/v1internal:streamGenerateContent?alt=sse`) using HTTP/1.1 chunked transfer encoding and uncompressed upstream requests.
- **Native User-Agent Preservation**: Preserves an official `antigravity/cli/...` User-Agent while leaving model availability to the native client and upstream service.
- **Process-Level Concurrency Protection**: Uses `fcntl.flock` around pool-state access.
- **Multi-Account OAuth Management**: One-click system browser authentication (`termux-open-url`, `termux-open`, `xdg-open`, `open`) with headless fallback for manual code pasting in SSH/remote environments.
- **Visual Terminal Dashboard**: Cached quota progress bars for Gemini 5-hour, weekly, and third-party models with reset countdown timers (`agy-pool quota` / `agy-pool list`).
- **Direct Native Fallback Mode**: `agy-raw` / `agy-orig` scripts to bypass the gateway proxy whenever direct connection to Google is required.
- **CLI Commands Suite**: Complete control commands for `login`, `import-current`, `list`/`quota`, `switch`, `remove`, `start`, `stop`, `restart`, `status`, and `-v`/`--version`.
- **Packaging & Portability**: Automated Termux/Linux installer (`install.sh`), uninstaller (`uninstall.sh`), and standalone distribution archive.

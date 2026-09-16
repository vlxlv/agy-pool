# agy-pool Project Instructions & Engineering Skill

## Persistent State Safety

### Incident Background & Technical Cause
During earlier Python modularization, test-harness path isolation broke because unit tests monkey-patched legacy entrypoint globals while extracted modules read from configuration constants. Consequently, a test-isolation regression caused synthetic fixtures to overwrite the real agy-pool account file and destroy OAuth account state.

This failure mechanism must never recur. State isolation, pre-test backup protocols, and fail-closed path guards are permanent release gates for all tests, migrations, and refactoring work.

### Mandatory Rules
- **Pre-Flight Inspection & Backup**:
  Before any local test, refactor, migration, storage test, or full test-suite run:
  - Identify writable persistent state across all entrypoints and modules.
  - Back up critical files with a timestamped snapshot (`~/.gemini/agy-pool-accounts.json.<timestamp>.bak`).
  - Verify backup permissions are strictly user-private (`0o600` / `chmod 600`).
  - Record original file hash (SHA-256) and modification timestamp (`mtime`).

- **Strict Production Path Ban**:
  Tests must never use the real user state directory. For `agy-pool`, this includes at minimum:
  - `~/.gemini/agy-pool-accounts.json`
  - `~/.gemini/agy-pool.log`
  - `~/.gemini/agy-pool.pid`
  - `~/.gemini/*.lock`
  - `~/.gemini/agy-pool-refresh-*.lock`
  - conversation/session databases (`~/.gemini/antigravity-cli/conversation_summaries.db`, presence locks)
  - any future persistent agy-pool state

- **Isolated State Root**:
  All state-mutating tests must use an isolated temporary HOME/state root (e.g. via `tempfile.TemporaryDirectory()`).

- **Fail-Closed Protection**:
  Test safety must fail closed: if a test-mode write resolves to the real production state directory, the system must abort before the first write.

- **Authoritative Configuration**:
  Never rely only on monkey-patching one module-global path. All components must derive persistent paths from one authoritative configuration source.

- **Pre-Test Checklist**:
  Before full tests:
  - [ ] Critical data backed up
  - [ ] Backup permissions verified
  - [ ] Original hash recorded
  - [ ] Temporary HOME/state root active
  - [ ] Production paths unreachable
  - [ ] Fail-closed guard active

- **Post-Test Verification**:
  After full tests:
  - Verify production file hash and mtime are unchanged.
  - If changed unexpectedly, stop immediately and investigate.

---

## Scope & Engineering Discipline

- **Smallest Correct Change**: Keep changes minimal, focused, and free of speculative abstractions.
- **Root-Cause Focus**: Fix root causes rather than patching symptoms in callers.
- **Preserve Invariants**: Keep public CLI behavior, proxying, and storage formats backwards-compatible.
- **Zero External Dependencies**: Standard library only (`urllib`, `http.server`, `sqlite3`, `fcntl`, `hashlib`, `json`).

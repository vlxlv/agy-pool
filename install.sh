#!/usr/bin/env bash
# agy-pool installer for Termux / Android Linux and standard POSIX environments

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_SRC="$SCRIPT_DIR/bin/agy-pool"
RAW_SRC="$SCRIPT_DIR/bin/agy-raw"

echo -e "\033[1;36m===================================================="
echo -e "       Installing agy-pool (Antigravity Pool)       "
echo -e "====================================================\033[0m"

# 1. Environment & Dependency checks
if [ -n "$PREFIX" ] && [ -d "$PREFIX/bin" ]; then
    TARGET_DIR="$PREFIX/bin"
    echo -e "\033[32m[✓] Detected Termux environment: $TARGET_DIR\033[0m"
elif [ -w "/usr/local/bin" ]; then
    TARGET_DIR="/usr/local/bin"
    echo -e "\033[32m[✓] Standard Linux/macOS detected: $TARGET_DIR\033[0m"
elif mkdir -p "$HOME/.local/bin" 2>/dev/null && [ -w "$HOME/.local/bin" ]; then
    TARGET_DIR="$HOME/.local/bin"
    echo -e "\033[33m[!] /usr/local/bin not writable, installing to user directory: $TARGET_DIR\033[0m"
else
    TARGET_DIR="/usr/local/bin"
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo -e "\033[33m[!] python3 not found. Installing via package manager...\033[0m"
    if command -v pkg >/dev/null 2>&1; then
        pkg update -y && pkg install -y python
    elif command -v apt >/dev/null 2>&1; then
        apt update -y && apt install -y python3
    elif command -v brew >/dev/null 2>&1; then
        brew install python3
    else
        echo -e "\033[31m[Error] Please install python3 manually and re-run.\033[0m"
        exit 1
    fi
fi

# 2. Fix shebang and permissions
chmod +x "$BIN_SRC" "$RAW_SRC"
if command -v termux-fix-shebang >/dev/null 2>&1; then
    termux-fix-shebang "$BIN_SRC" "$RAW_SRC"
fi

# 3. Create symlinks in bin directory
ln -sf "$BIN_SRC" "$TARGET_DIR/agy-pool"
ln -sf "$RAW_SRC" "$TARGET_DIR/agy-raw"
ln -sf "$RAW_SRC" "$TARGET_DIR/agy-orig"
echo -e "\033[32m[✓] Installed executables: agy-pool, agy-raw, agy-orig in $TARGET_DIR\033[0m"

# 4. Configure shell aliases (~/.bashrc and ~/.zshrc)
SHELL_FILES=()
[ -f "$HOME/.bashrc" ] && SHELL_FILES+=("$HOME/.bashrc")
[ -f "$HOME/.zshrc" ] && SHELL_FILES+=("$HOME/.zshrc")

# If neither file exists, default to creating ~/.bashrc (preserves existing installer behavior)
if [ ${#SHELL_FILES[@]} -eq 0 ]; then
    SHELL_FILES=("$HOME/.bashrc")
fi

MARKER="# >>> agy-pool integration >>>"
for rc in "${SHELL_FILES[@]}"; do
    touch "$rc"
    if ! grep -Fq "$MARKER" "$rc"; then
        cat >> "$rc" << 'EOF'

# >>> agy-pool integration >>>
alias agy='agy-pool run'
alias agy-raw='agy-raw'
alias agy-orig='agy-raw'
# <<< agy-pool integration <<<
EOF
        echo -e "\033[32m[✓] Added 'alias agy=agy-pool run' and 'agy-raw / agy-orig' to $rc\033[0m"
    fi
done

# 5. Auto-import current antigravity credentials if present
echo -e "\033[36m[*] Checking for existing Antigravity login token...\033[0m"
"$TARGET_DIR/agy-pool" import-current || true

echo -e "\n\033[1;32m===================================================="
echo -e "             Installation Successful!               "
echo -e "====================================================\033[0m"
echo -e "You can now use:"
echo -e "  \033[1magy\033[0m            - Runs Antigravity with auto load balancer & failover"
echo -e "  \033[1magy -c\033[0m         - Resumes session with auto load balancer"
echo -e "  \033[1magy-raw\033[0m        - Directly runs original native agy (NO proxy)"
echo -e "  \033[1magy-orig\033[0m       - Alias for agy-raw (original direct mode)"
echo -e "  \033[1magy-pool quota\033[0m - Real-time quota dashboard for all accounts"
echo -e "  \033[1magy-pool login\033[0m - Add new Google accounts to pool"
echo -e ""

#!/usr/bin/env bash
# agy-pool uninstaller

set -e

if [ -n "$PREFIX" ] && [ -d "$PREFIX/bin" ]; then
    TARGET_DIR="$PREFIX/bin"
elif [ -d "$HOME/.local/bin" ] && [ -f "$HOME/.local/bin/agy-pool" ]; then
    TARGET_DIR="$HOME/.local/bin"
else
    TARGET_DIR="/usr/local/bin"
fi

echo -e "\033[1;33mUninstalling agy-pool...\033[0m"

# 1. Stop daemon
if command -v agy-pool >/dev/null 2>&1; then
    agy-pool stop || true
fi

# 2. Remove symlinks
rm -f "$TARGET_DIR/agy-pool" "$TARGET_DIR/agy-raw" "$TARGET_DIR/agy-orig"
rm -f "$HOME/.local/bin/agy-pool" "$HOME/.local/bin/agy-raw" "$HOME/.local/bin/agy-orig" 2>/dev/null || true
echo -e "\033[32m[✓] Removed agy-pool binaries and symlinks\033[0m"

# 3. Clean shell rc files
for rc in "$HOME/.bashrc" "$HOME/.zshrc"; do
    if [ -f "$rc" ]; then
        sed -i '/# >>> agy-pool integration >>>/,/# <<< agy-pool integration <<</d' "$rc"
        echo -e "\033[32m[✓] Cleaned agy-pool alias from $rc\033[0m"
    fi
done

echo -e "\033[1;32mUninstallation complete.\033[0m"
echo -e "(Your account tokens in ~/.gemini/agy-pool-accounts.json were preserved. Delete manually if desired.)"

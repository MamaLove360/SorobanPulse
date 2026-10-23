#!/bin/bash
# Install shell completions and man page for spulse

set -e

SPULSE_BIN="${1:-spulse}"

if ! command -v "$SPULSE_BIN" &> /dev/null; then
    echo "Error: spulse not found in PATH"
    exit 1
fi

echo "Installing shell completions for spulse..."

# Bash completion
if [[ -d "/usr/local/etc/bash_completion.d" ]]; then
    $SPULSE_BIN completions bash > /usr/local/etc/bash_completion.d/spulse
    echo "✓ Bash completion installed"
fi

# Zsh completion
if [[ -d "/usr/local/share/zsh/site-functions" ]]; then
    $SPULSE_BIN completions zsh > /usr/local/share/zsh/site-functions/_spulse
    echo "✓ Zsh completion installed"
fi

# Fish completion
if [[ -d "/usr/local/share/fish/vendor_completions.d" ]]; then
    $SPULSE_BIN completions fish > /usr/local/share/fish/vendor_completions.d/spulse.fish
    echo "✓ Fish completion installed"
fi

# Man page (if available)
if [[ -f "cli/spulse.1" || -f "target/release/spulse.1" ]]; then
    MAN_PATH=$(find . -name "spulse.1" -type f 2>/dev/null | head -1)
    if [[ -n "$MAN_PATH" ]]; then
        cp "$MAN_PATH" /usr/local/share/man/man1/spulse.1
        echo "✓ Man page installed"
    fi
fi

echo "Installation complete!"

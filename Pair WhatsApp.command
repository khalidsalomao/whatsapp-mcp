#!/bin/zsh
set -eu
binary="${0:A:h}/bin/whatsapp-mcp"
if [[ ! -x "$binary" ]]; then
  binary="$HOME/Library/Application Support/WhatsAppMCP/bin/whatsapp-mcp"
fi
"$binary" pair --open
printf '\nPress Return to close this window.\n'
read -r

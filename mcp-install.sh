#!/bin/sh
# Usage:
#   curl -s https://pipeviz.org/mcp-install | sh -s <path-to-pipeviz.json>
set -e

JSON="${1:-$PIPEVIZ_JSON}"

if [ -z "$JSON" ]; then
  echo "usage: curl -s https://pipeviz.org/mcp-install | sh -s <path-to-pipeviz.json>"
  exit 1
fi

ABS_JSON="$(cd "$(dirname "$JSON")" && pwd)/$(basename "$JSON")"

if [ ! -f "$ABS_JSON" ]; then
  echo "error: $ABS_JSON not found"
  exit 1
fi

CLAUDE_JSON="$HOME/.claude.json"

if [ ! -f "$CLAUDE_JSON" ]; then
  echo "error: $CLAUDE_JSON not found — is Claude Code installed?"
  exit 1
fi

node -e "
  const fs = require('fs');
  const cfg = JSON.parse(fs.readFileSync('$CLAUDE_JSON', 'utf8'));
  if (!cfg.mcpServers) cfg.mcpServers = {};
  cfg.mcpServers.pipeviz = { command: 'npx', args: ['pipeviz-mcp', '$ABS_JSON'] };
  fs.writeFileSync('$CLAUDE_JSON', JSON.stringify(cfg, null, 2) + '\n');
"

echo "pipeviz MCP installed --> $CLAUDE_JSON"
echo "  graph: $ABS_JSON"
echo ""
echo "Restart Claude Code to pick it up."

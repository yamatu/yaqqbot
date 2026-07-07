#!/usr/bin/env bash
set -euo pipefail

PORT="${GEMINI_SEARCH_PORT:-8080}"
HOST="${GEMINI_SEARCH_HOST:-}"
PROFILE_DIR="${GEMINI_SEARCH_USER_DATA_DIR:-$HOME/.local/share/gemini-search-mcp/chrome-profile}"

export GEMINI_SEARCH_USER_DATA_DIR="$PROFILE_DIR"
mkdir -p "$GEMINI_SEARCH_USER_DATA_DIR"

if [[ -d /opt/homebrew/opt/expat/lib ]]; then
  export DYLD_LIBRARY_PATH="/opt/homebrew/opt/expat/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
fi

PROXY_SERVER="${GEMINI_SEARCH_PROXY_SERVER:-}"
if [[ -z "$PROXY_SERVER" && -n "${SOCKS5_PROXY:-}" ]]; then
  PROXY_SERVER="$SOCKS5_PROXY"
fi

GEMINI_SEARCH_BIN="${GEMINI_SEARCH_BIN:-}"
if [[ -z "$GEMINI_SEARCH_BIN" ]]; then
  if command -v gemini-search >/dev/null 2>&1; then
    GEMINI_SEARCH_BIN="$(command -v gemini-search)"
  elif [[ -x "$HOME/.local/share/gemini-search-mcp/venv/bin/gemini-search" ]]; then
    GEMINI_SEARCH_BIN="$HOME/.local/share/gemini-search-mcp/venv/bin/gemini-search"
  fi
fi

if [[ -z "$GEMINI_SEARCH_BIN" || ! -x "$GEMINI_SEARCH_BIN" ]]; then
  cat >&2 <<'EOF'
未找到 gemini-search 命令。
建议安装到独立 venv:
  brew install python@3.12 expat
  /opt/homebrew/bin/python3.12 -m venv ~/.local/share/gemini-search-mcp/venv
  DYLD_LIBRARY_PATH=/opt/homebrew/opt/expat/lib ~/.local/share/gemini-search-mcp/venv/bin/pip install git+https://github.com/Sophomoresty/gemini-search-mcp.git
EOF
  exit 127
fi

args=(--port "$PORT" --user-data-dir "$GEMINI_SEARCH_USER_DATA_DIR")

if [[ -n "$HOST" ]]; then
  args+=(--host "$HOST")
fi
if [[ -n "${CDP_URL:-}" ]]; then
  args+=(--cdp-url "$CDP_URL")
fi
if [[ -n "${BROWSER_CHANNEL:-}" ]]; then
  args+=(--channel "$BROWSER_CHANNEL")
fi
if [[ "${HEADLESS:-1}" == "0" || "${GEMINI_SEARCH_NO_HEADLESS:-0}" == "1" ]]; then
  args+=(--no-headless)
fi
if [[ -n "${GEMINI_SEARCH_BROWSER_BACKEND:-}" ]]; then
  args+=(--browser-backend "$GEMINI_SEARCH_BROWSER_BACKEND")
fi
if [[ -n "$PROXY_SERVER" ]]; then
  args+=(--proxy-server "$PROXY_SERVER")
fi
if [[ -n "${GEMINI_SEARCH_CHROMEDRIVER:-}" ]]; then
  args+=(--chromedriver-path "$GEMINI_SEARCH_CHROMEDRIVER")
fi

exec "$GEMINI_SEARCH_BIN" "${args[@]}"

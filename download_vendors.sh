#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# FlowForge — One-time vendor download script
#
# Run this ONCE on any machine that has internet access.
# It downloads the three required JS files into frontend/vendor/
# After that, FlowForge runs with zero external network requests.
#
# Usage:  ./download_vendors.sh
# ─────────────────────────────────────────────────────────────────
set -e

VENDOR_DIR="$(cd "$(dirname "$0")/frontend" && pwd)/vendor"
mkdir -p "$VENDOR_DIR"

echo ""
echo "FlowForge — Downloading frontend vendor files"
echo "Target: $VENDOR_DIR"
echo ""

ok=0; fail=0

download() {
  local url="$1" file="$2"
  local dest="$VENDOR_DIR/$file"
  if [ -f "$dest" ]; then
    echo "  ✓ $file  (already present)"
    ok=$((ok+1)); return
  fi
  printf "  ↓ %-45s" "$file"
  if curl -sSf --max-time 30 "$url" -o "$dest"; then
    size=$(wc -c < "$dest" | tr -d ' ')
    echo "  ${size} bytes"
    ok=$((ok+1))
  else
    echo "  FAILED"; fail=$((fail+1)); rm -f "$dest"
  fi
}

download \
  "https://cdnjs.cloudflare.com/ajax/libs/react/18.2.0/umd/react.production.min.js" \
  "react.production.min.js"

download \
  "https://cdnjs.cloudflare.com/ajax/libs/react-dom/18.2.0/umd/react-dom.production.min.js" \
  "react-dom.production.min.js"

download \
  "https://cdnjs.cloudflare.com/ajax/libs/babel-standalone/7.23.2/babel.min.js" \
  "babel.min.js"

echo ""
if [ "$fail" -eq 0 ]; then
  echo "  ✓ All $ok vendor files ready. FlowForge now runs fully offline."
else
  echo "  ✗ $fail file(s) failed. Check internet connection and retry."
  exit 1
fi
echo ""

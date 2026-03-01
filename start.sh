#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# FlowForge — Local startup script (no Docker required)
# Usage: ./start.sh
# ─────────────────────────────────────────────────────────────────
set -e

BACKEND_DIR="$(cd "$(dirname "$0")/backend" && pwd)"
FRONTEND_DIR="$(cd "$(dirname "$0")/frontend" && pwd)"
PORT="${PORT:-8000}"

echo ""
echo "  ███████╗██╗      ██████╗ ██╗    ██╗███████╗ ██████╗ ██████╗  ██████╗ ███████╗"
echo "  ██╔════╝██║     ██╔═══██╗██║    ██║██╔════╝██╔═══██╗██╔══██╗██╔════╝ ██╔════╝"
echo "  █████╗  ██║     ██║   ██║██║ █╗ ██║█████╗  ██║   ██║██████╔╝██║  ███╗█████╗  "
echo "  ██╔══╝  ██║     ██║   ██║██║███╗██║██╔══╝  ██║   ██║██╔══██╗██║   ██║██╔══╝  "
echo "  ██║     ███████╗╚██████╔╝╚███╔███╔╝██║     ╚██████╔╝██║  ██║╚██████╔╝███████╗"
echo "  ╚═╝     ╚══════╝ ╚═════╝  ╚══╝╚══╝ ╚═╝      ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝"
echo ""
echo "  Starting FlowForge locally on http://localhost:${PORT}"
echo ""

# ── Check Python ──────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "ERROR: python3 not found. Install Python 3.10+ and retry."
  exit 1
fi

PYTHON=$(command -v python3)
PYVER=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "  Python: $PYTHON ($PYVER)"

# ── Check vendor JS files (required — no CDN fallback) ────────────
VENDOR_DIR="$(dirname "$0")/frontend/vendor"
MISSING_VENDORS=0
for f in react.production.min.js react-dom.production.min.js babel.min.js; do
  if [ ! -f "$VENDOR_DIR/$f" ]; then
    echo "  MISSING vendor file: frontend/vendor/$f"
    MISSING_VENDORS=1
  fi
done

if [ "$MISSING_VENDORS" -eq 1 ]; then
  echo ""
  echo "  ────────────────────────────────────────────────────────"
  echo "  ERROR: Frontend vendor files are missing."
  echo "  Run this once from a machine with internet access:"
  echo ""
  echo "    ./download_vendors.sh"
  echo ""
  echo "  Then copy the populated frontend/vendor/ folder here."
  echo "  ────────────────────────────────────────────────────────"
  echo ""
  exit 1
fi
echo "  Vendor JS: ✓"

# ── Create / activate virtual environment ─────────────────────────
VENV_DIR="$(dirname "$0")/.venv"
if [ ! -d "$VENV_DIR" ]; then
  echo "  Creating virtual environment…"
  "$PYTHON" -m venv "$VENV_DIR"
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
echo "  Virtualenv: $VENV_DIR"

# ── Install dependencies ──────────────────────────────────────────
echo "  Installing / updating dependencies…"
pip install --quiet --upgrade pip
pip install --quiet -r "$BACKEND_DIR/requirements.txt"

# ── Environment defaults ──────────────────────────────────────────
export FLOWFORGE_SECRET_KEY="${FLOWFORGE_SECRET_KEY:-change-me-in-production}"
export FLOWFORGE_SALT="${FLOWFORGE_SALT:-flowforge-salt}"
export FLOWFORGE_DEV_MODE="${FLOWFORGE_DEV_MODE:-true}"
export DATABASE_URL="${DATABASE_URL:-sqlite:///./flowforge.db}"
# Set ANTHROPIC_API_KEY in your shell or .env before running:
# export ANTHROPIC_API_KEY=sk-ant-...

# Resolve frontend path so FastAPI serves it
export FLOWFORGE_FRONTEND_PATH="$FRONTEND_DIR"

echo ""
echo "  ✓ FlowForge API  →  http://localhost:${PORT}"
echo "  ✓ UI             →  http://localhost:${PORT}"
echo "  ✓ Swagger docs   →  http://localhost:${PORT}/api/docs"
echo ""

# ── Start backend ─────────────────────────────────────────────────
cd "$BACKEND_DIR"
exec python -m uvicorn main:app \
  --host 0.0.0.0 \
  --port "$PORT" \
  --reload \
  --log-level info

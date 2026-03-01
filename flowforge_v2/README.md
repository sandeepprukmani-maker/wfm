# ⬡ FlowForge v2 — Production-Ready

## What was fixed in v2 (from v1)

| # | Bug | Fix |
|---|-----|-----|
| 1 | `SYNTAX ERROR` in `pytest_export.py` line 109 — misaligned triple-quote string | Rewrote using `.format()` to avoid f-string brace conflicts |
| 2 | `build_connector_by_id()` called in credentials router but **didn't exist** | Added method to `CredentialManager` |
| 3 | `mgr.update()` argument order was reversed (`owner_id, cred_id` instead of `cred_id, owner_id`) | Fixed + added keyword args to prevent future mistakes |
| 4 | `from connectors.http_mcp import HTTPConnector` missing in `workflow_engine.py` | Added to top-level imports |
| 5 | `allow_origins=["*"]` unconditional in `main.py` | Now reads from `ALLOWED_ORIGINS` env var; wildcard only in dev mode |
| 6 | SQLite `onupdate` doesn't auto-trigger | Replaced with explicit `model.updated_at = datetime.utcnow()` everywhere |
| 7 | All routers hardcoded `"demo-user"` — no real auth | Added `auth.py` with JWT (PyJWT + bcrypt); `get_current_user` dependency on all routes |
| 8 | No auth module at all | Added `auth.py`: login, register, refresh, `/api/auth/me` |

## n8n-Inspired Features Added

- **Webhook triggers** — `POST /webhook/trigger/{path}` fires any workflow
- **IF/ELSE branching node** — expression-based conditional routing
- **Expression resolver** — `{{$node.NodeName.output.field}}` references upstream output
- **Retry + backoff** — per-node `retries` field with exponential backoff
- **Undo/Redo** — full canvas history stack (20 levels)
- **Workflow import/export** — JSON download/upload
- **Execution cancel** — `POST /api/executions/{id}/cancel`
- **Workflow versioning** — every save creates immutable snapshot
- **Request logging middleware** — every request logged with timing
- **Rate limiting** — 120 req/min per IP (configurable)
- **`/health` endpoint** — returns DB status, version, dev mode

## Quick Start

```bash
# Set environment variables
export FLOWFORGE_SECRET_KEY="your-32-char-secret-key-here!!"
export JWT_SECRET="your-jwt-secret"
export FLOWFORGE_DEV_MODE=false       # set true for local dev (disables strict CORS + auth)
export ALLOWED_ORIGINS="https://yourdomain.com"
export ANTHROPIC_API_KEY="sk-ant-..."  # optional

docker-compose up -d
open http://localhost
```

## File Structure

```
flowforge_v2/
├── frontend/index.html          ← Complete React SPA (no build step)
├── backend/
│   ├── main.py                  ← FastAPI app, CORS, rate limiting, WS
│   ├── auth.py                  ← JWT auth + bcrypt (NEW in v2)
│   ├── database.py              ← 8 SQLAlchemy models (fixed onupdate)
│   ├── credential_manager.py   ← AES-256-GCM, fixed update() + build_connector_by_id()
│   ├── workflow_engine.py      ← Fixed HTTPConnector import, expressions, retries
│   ├── requirements.txt
│   ├── connectors/             ← 6 MCP connectors (unchanged from v1)
│   └── routers/
│       ├── auth → /api/auth
│       ├── chat → /api/chat
│       ├── workflows → /api/workflows (+ import/export)
│       ├── executions → /api/executions (+ cancel)
│       ├── credentials → /api/credentials (fixed)
│       ├── pytest_export → /api/pytest (fixed syntax error)
│       └── webhooks → /api/webhooks (NEW)
└── docker/
    ├── Dockerfile.api
    └── nginx.conf
```

## Default Login (dev)
```
Email: admin@flowforge.local
Password: admin123
```
Or set `FLOWFORGE_DEV_MODE=true` to skip auth entirely.

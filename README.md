# FlowForge

Prompt-driven enterprise workflow automation. Runs fully offline after one-time setup — no Docker, no CDN, no telemetry.

---

## Setup (do this once)

### Step 1 — Download vendor JS files

Run this **once on any machine with internet access**, then copy the whole project folder to your corporate machine:

```bash
./download_vendors.sh
```

This downloads React, ReactDOM and Babel into `frontend/vendor/`. After that the app makes **zero external network requests**.

### Step 2 — Start FlowForge

**macOS / Linux:**
```bash
export ANTHROPIC_API_KEY=sk-ant-...   # optional — enables AI chat
chmod +x start.sh
./start.sh
```

**Windows:**
```bat
set ANTHROPIC_API_KEY=sk-ant-...
start.bat
```

Open **http://localhost:8000** — default login: `admin` / `admin`

---

## Network requests — full inventory

| Request | When | How to eliminate |
|---|---|---|
| `cdnjs.cloudflare.com` (React, ReactDOM, Babel) | Browser, page load | Run `./download_vendors.sh` once |
| `fonts.googleapis.com` (IBM Plex Mono, Syne) | **Removed** | System fonts used instead |
| `api.anthropic.com` | Server, AI chat only | Unset `ANTHROPIC_API_KEY` to use rule-based mode |
| Your AWS endpoints | Server, only when S3 connector used | Normal — your own infra |
| Your Azure endpoints | Server, only when Azure connector used | Normal — your own infra |
| Your Airflow/SQL/SFTP/HTTP | Server, only when those connectors used | Normal — your own infra |

**No telemetry packages** (no Sentry, Segment, Datadog, Amplitude, Mixpanel, etc.)

The Anthropic SDK sends `X-Stainless-*` headers (OS, runtime, arch info) by default. These are suppressed — all fields are set to empty strings.

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8000` | HTTP port |
| `ANTHROPIC_API_KEY` | *(none)* | Enables AI chat; omit to use rule-based mode with no outbound calls |
| `FLOWFORGE_SECRET_KEY` | `change-me-in-production` | JWT signing key — **change in production** |
| `FLOWFORGE_SALT` | `flowforge-salt` | Credential encryption salt |
| `DATABASE_URL` | `sqlite:///./flowforge.db` | SQLAlchemy DB URL |
| `FLOWFORGE_DEV_MODE` | `true` | Enables hot-reload and open CORS |

---

## MCP Servers

Open `mcp_config.json` for ready-to-paste configuration for Claude Desktop, Cursor, Zed, Continue, VS Code Copilot, and any other MCP-compatible client.

---

## Project Structure

```
flowforge/
├── start.sh / start.bat      Startup scripts
├── download_vendors.sh       One-time JS vendor downloader
├── mcp_config.json           MCP config for any MCP client
├── README.md
├── frontend/
│   ├── index.html            Single-page app (no external deps)
│   └── vendor/               React + ReactDOM + Babel (populated by download_vendors.sh)
└── backend/
    ├── main.py
    ├── requirements.txt
    ├── connectors/
    ├── mcp_servers/
    └── routers/
```

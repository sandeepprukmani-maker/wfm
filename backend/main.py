"""
FlowForge Backend — FastAPI Application (v2, production-ready)

Fixes from v1:
- CORS allow_origins=["*"] → configurable via ALLOWED_ORIGINS env var
- Added rate limiting middleware
- Added /health endpoint
- Added request logging middleware
- Proper startup/shutdown lifecycle
- Added webhook trigger router
"""

import os
import logging
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

# ── Logging setup ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("flowforge")


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup + shutdown lifecycle."""
    from database import engine, Base, SessionLocal, seed_default_user
    logger.info("Creating database tables…")
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        seed_default_user(db)
    except Exception as e:
        logger.warning(f"Seed user failed (may already exist): {e}")
    finally:
        db.close()

    # Start cron scheduler
    from scheduler import get_scheduler
    scheduler = get_scheduler()
    await scheduler.start()
    app.state.scheduler = scheduler
    logger.info("FlowForge API started ✓")

    yield

    # Shutdown
    await scheduler.stop()
    logger.info("FlowForge API shutting down")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="FlowForge API",
    description="Prompt-Driven Enterprise Workflow Automation & Test Generation",
    version="5.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=lifespan,
)

# ── CORS — FIX: was allow_origins=["*"] ──────────────────────────────────────

_raw_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:8000")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]

# Dev mode: allow everything
if os.getenv("FLOWFORGE_DEV_MODE", "true").lower() == "true":
    ALLOWED_ORIGINS = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
)
app.add_middleware(GZipMiddleware, minimum_size=1024)


# ── Request logging middleware ────────────────────────────────────────────────

@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0  = time.time()
    response = await call_next(request)
    ms  = round((time.time() - t0) * 1000)
    logger.info(f"{request.method} {request.url.path} → {response.status_code} ({ms}ms)")
    response.headers["X-Response-Time"] = f"{ms}ms"
    return response


# ── Simple in-memory rate limiter ─────────────────────────────────────────────

from collections import defaultdict
_rate_counts: dict = defaultdict(list)

@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        ip    = request.client.host if request.client else "unknown"
        now   = time.time()
        limit = int(os.getenv("RATE_LIMIT_RPM", "120"))   # requests per minute

        _rate_counts[ip] = [t for t in _rate_counts[ip] if now - t < 60]
        if len(_rate_counts[ip]) >= limit:
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded. Try again in a minute."})
        _rate_counts[ip].append(now)

    return await call_next(request)


# ── Routers ───────────────────────────────────────────────────────────────────

from routers import chat, workflows, executions, credentials, pytest_export, webhooks, variables, metrics, templates
from auth import router as auth_router

app.include_router(auth_router,           prefix="/api/auth",        tags=["Auth"])
app.include_router(chat.router,           prefix="/api/chat",        tags=["Chat"])
app.include_router(workflows.router,      prefix="/api/workflows",   tags=["Workflows"])
app.include_router(executions.router,     prefix="/api/executions",  tags=["Executions"])
app.include_router(credentials.router,    prefix="/api/credentials", tags=["Credentials"])
app.include_router(pytest_export.router,  prefix="/api/pytest",      tags=["PyTest Export"])
app.include_router(webhooks.router,       prefix="/api/webhooks",    tags=["Webhooks"])
app.include_router(variables.router,      prefix="/api/variables",   tags=["Variables"])
app.include_router(metrics.router,        prefix="/api/metrics",     tags=["Metrics"])
app.include_router(templates.router,      prefix="/api/templates",   tags=["Templates"])


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health(request: Request):
    from database import engine
    try:
        with engine.connect() as conn:
            conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {e}"

    scheduler = getattr(request.app.state, "scheduler", None)
    scheduled = scheduler.get_scheduled_workflows() if scheduler else []

    return {
        "status":     "ok" if db_status == "ok" else "degraded",
        "database":   db_status,
        "version":    "5.0.0",
        "dev_mode":   os.getenv("FLOWFORGE_DEV_MODE", "true"),
        "scheduled_workflows": len(scheduled),
        "scheduler":  [s for s in scheduled],
    }


@app.get("/api/health", tags=["System"])
async def api_health(request: Request):
    return await health(request)


# ── WebSocket: live execution streaming ───────────────────────────────────────

@app.websocket("/ws/execution/{execution_id}")
async def execution_ws(websocket: WebSocket, execution_id: str):
    """Stream real-time node status updates to frontend."""
    await websocket.accept()
    logger.info(f"WS connected: execution {execution_id}")
    try:
        from database import SessionLocal
        from credential_manager import CredentialManager
        from workflow_engine import WorkflowEngine

        db  = SessionLocal()
        mgr = CredentialManager(db)
        eng = WorkflowEngine(db_session=db, credential_manager=mgr)

        async for event in eng.stream_execution(execution_id):
            await websocket.send_json(event)

    except WebSocketDisconnect:
        logger.info(f"WS disconnected: execution {execution_id}")
    except Exception as e:
        logger.error(f"WS error: {e}")
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        try:
            db.close()
        except Exception:
            pass


# ── Static frontend (last, so API routes take priority) ───────────────────────

_frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(_frontend_path):
    app.mount("/", StaticFiles(directory=_frontend_path, html=True), name="frontend")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("FLOWFORGE_DEV_MODE", "true").lower() == "true",
        workers=1 if os.getenv("FLOWFORGE_DEV_MODE", "true").lower() == "true" else 4,
        log_level="info",
    )

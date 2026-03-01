"""
FlowForge — Webhook Triggers Router (n8n-inspired)
Any external service can POST to /webhook/{path} to trigger a workflow.
"""

import uuid
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from database import get_db, WebhookTrigger, WorkflowRun
from auth import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)


class WebhookCreate(BaseModel):
    workflow_id: str
    method:      str = "POST"
    auth_token:  Optional[str] = None


@router.post("/")
async def create_webhook(
    body:         WebhookCreate,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """Register a webhook URL for a workflow."""
    path    = str(uuid.uuid4())[:16]
    webhook = WebhookTrigger(
        workflow_id=body.workflow_id,
        path=path,
        method=body.method.upper(),
        auth_token=body.auth_token,
    )
    db.add(webhook)
    db.commit()
    db.refresh(webhook)
    return {
        "id":         webhook.id,
        "path":       path,
        "url":        f"/webhook/{path}",
        "method":     webhook.method,
        "workflow_id":body.workflow_id,
    }


@router.get("/")
async def list_webhooks(
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    hooks = db.query(WebhookTrigger).all()
    return {"webhooks": [{"id": h.id, "path": h.path, "method": h.method, "workflow_id": h.workflow_id, "is_active": h.is_active} for h in hooks]}


@router.delete("/{webhook_id}", status_code=204)
async def delete_webhook(
    webhook_id:   str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wh = db.query(WebhookTrigger).filter_by(id=webhook_id).first()
    if not wh:
        raise HTTPException(404, "Webhook not found")
    db.delete(wh)
    db.commit()


# ── Public webhook receiver (no auth required — token verified inline) ────────

@router.api_route("/trigger/{path}", methods=["GET", "POST", "PUT"])
async def receive_webhook(
    path:              str,
    request:           Request,
    background_tasks:  BackgroundTasks,
    db:                Session = Depends(get_db),
):
    """
    Public endpoint — called by external services.
    Triggers the associated workflow with request payload as input data.
    """
    hook = db.query(WebhookTrigger).filter_by(path=path, is_active=True).first()
    if not hook:
        raise HTTPException(404, "Webhook not found or inactive")

    # Verify method
    if hook.method != "ANY" and request.method != hook.method:
        raise HTTPException(405, f"Method {request.method} not allowed. Expected {hook.method}")

    # Verify token if set
    if hook.auth_token:
        provided = request.headers.get("X-FlowForge-Token") or request.query_params.get("token")
        if provided != hook.auth_token:
            raise HTTPException(401, "Invalid webhook token")

    # Parse payload
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    # Create run record
    run_id = f"run-{str(uuid.uuid4())[:8]}"
    run    = WorkflowRun(
        id=run_id,
        workflow_id=hook.workflow_id,
        triggered_by="webhook",
        trigger_data={"payload": payload, "path": path, "method": request.method},
        status="pending",
        started_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()

    # Fire execution in background
    background_tasks.add_task(_run_workflow_bg, hook.workflow_id, run_id, payload)

    return {
        "received": True,
        "run_id":   run_id,
        "message":  "Workflow execution started",
    }


async def _run_workflow_bg(workflow_id: str, run_id: str, trigger_data: dict):
    """Background task: execute workflow triggered by webhook."""
    import asyncio
    from database import SessionLocal
    from credential_manager import CredentialManager
    from workflow_engine import WorkflowEngine

    db  = SessionLocal()
    try:
        mgr = CredentialManager(db)
        eng = WorkflowEngine(db_session=db, credential_manager=mgr)
        await eng.execute(workflow_id, "demo-user", run_id, trigger_data=trigger_data)
    except Exception as e:
        logger.error(f"Webhook workflow execution failed: {e}")
    finally:
        db.close()

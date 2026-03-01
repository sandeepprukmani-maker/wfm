"""FlowForge — Executions Router (v2, fixed)"""

import uuid
import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from database import get_db, Workflow, WorkflowRun, NodeRun
from auth import get_current_user

router  = APIRouter()
logger  = logging.getLogger(__name__)


class ExecuteRequest(BaseModel):
    triggered_by: str = "manual"
    input_data:   dict = {}


@router.post("/{workflow_id}/run", status_code=202)
async def run_workflow(
    workflow_id:      str,
    body:             ExecuteRequest,
    background_tasks: BackgroundTasks,
    db:               Session = Depends(get_db),
    current_user:     dict    = Depends(get_current_user),
):
    wf = db.query(Workflow).filter_by(id=workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")

    run_id = f"run-{str(uuid.uuid4())[:8]}"
    run    = WorkflowRun(
        id=run_id,
        workflow_id=workflow_id,
        triggered_by=body.triggered_by,
        trigger_data=body.input_data,
        status="pending",
        started_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()

    background_tasks.add_task(
        _execute_bg,
        workflow_id=workflow_id,
        run_id=run_id,
        owner_id=current_user["sub"],
    )

    return {
        "run_id":  run_id,
        "status":  "pending",
        "message": "Execution queued. Connect WebSocket /ws/execution/{run_id} for live updates.",
        "ws_url":  f"/ws/execution/{run_id}",
    }


async def _execute_bg(workflow_id: str, run_id: str, owner_id: str):
    """
    Background execution task.
    FIX: imports moved inside function to avoid circular import at module load time.
    """
    from database import SessionLocal
    from credential_manager import CredentialManager
    from workflow_engine import WorkflowEngine

    db = SessionLocal()
    try:
        mgr = CredentialManager(db)
        eng = WorkflowEngine(db_session=db, credential_manager=mgr)
        result = await eng.execute(workflow_id, owner_id, run_id)
        logger.info(f"Run {run_id} completed: {result['status']} in {result['duration']:.1f}s")
    except Exception as e:
        logger.error(f"Run {run_id} crashed: {e}", exc_info=True)
        run = db.query(WorkflowRun).filter_by(id=run_id).first()
        if run:
            run.status        = "failed"
            run.error_message = str(e)
            run.completed_at  = datetime.utcnow()
            if run.started_at:
                run.duration_seconds = (run.completed_at - run.started_at).total_seconds()
            db.commit()
    finally:
        db.close()


@router.get("/")
async def list_executions(
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
    limit:        int     = 50,
    status:       str     = None,
):
    q = (
        db.query(WorkflowRun)
        .join(Workflow, WorkflowRun.workflow_id == Workflow.id, isouter=True)
        .filter(Workflow.owner_id == current_user["sub"])
        .order_by(WorkflowRun.started_at.desc())
        .limit(limit)
    )
    if status:
        q = q.filter(WorkflowRun.status == status)

    runs   = q.all()
    result = []
    for r in runs:
        wf = db.query(Workflow).filter_by(id=r.workflow_id).first()
        result.append({
            "id":               r.id,
            "workflow_id":      r.workflow_id,
            "workflow_name":    wf.name if wf else "Deleted",
            "status":           r.status,
            "triggered_by":     r.triggered_by,
            "started_at":       r.started_at.isoformat() if r.started_at else None,
            "completed_at":     r.completed_at.isoformat() if r.completed_at else None,
            "duration_seconds": r.duration_seconds,
            "retry_count":      r.retry_count,
        })
    return {"executions": result, "total": len(result)}


@router.get("/{run_id}")
async def get_execution(
    run_id:       str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    run = db.query(WorkflowRun).filter_by(id=run_id).first()
    if not run:
        raise HTTPException(404, "Execution not found")

    node_runs = (
        db.query(NodeRun)
        .filter_by(workflow_run_id=run_id)
        .order_by(NodeRun.started_at)
        .all()
    )
    return {
        "id":               run.id,
        "workflow_id":      run.workflow_id,
        "status":           run.status,
        "triggered_by":     run.triggered_by,
        "trigger_data":     run.trigger_data,
        "started_at":       run.started_at.isoformat() if run.started_at else None,
        "completed_at":     run.completed_at.isoformat() if run.completed_at else None,
        "duration_seconds": run.duration_seconds,
        "error_message":    run.error_message,
        "node_runs": [
            {
                "node_id":          nr.node_id,
                "node_title":       nr.node_title,
                "node_type":        nr.node_type,
                "status":           nr.status,
                "attempt":          nr.attempt,
                "duration_seconds": nr.duration_seconds,
                "log":              (nr.stdout_log or nr.stderr_log or "")[:2000],
                "output_data":      nr.output_data,
                "error":            nr.error_message,
            }
            for nr in node_runs
        ],
    }


@router.post("/{run_id}/cancel", status_code=202)
async def cancel_execution(
    run_id:       str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    run = db.query(WorkflowRun).filter_by(id=run_id).first()
    if not run:
        raise HTTPException(404, "Execution not found")
    if run.status not in ("pending", "running"):
        raise HTTPException(400, f"Cannot cancel execution with status {run.status!r}")
    run.status        = "cancelled"
    run.completed_at  = datetime.utcnow()
    if run.started_at:
        run.duration_seconds = (run.completed_at - run.started_at).total_seconds()
    db.commit()
    return {"cancelled": True, "run_id": run_id}


@router.get("/{run_id}/logs")
async def download_logs(
    run_id:       str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    run = db.query(WorkflowRun).filter_by(id=run_id).first()
    if not run:
        raise HTTPException(404, "Execution not found")

    node_runs = db.query(NodeRun).filter_by(workflow_run_id=run_id).order_by(NodeRun.started_at).all()
    lines = [
        "=" * 60,
        f"FlowForge Execution Log",
        f"Run ID   : {run.id}",
        f"Status   : {run.status}",
        f"Duration : {run.duration_seconds:.1f}s" if run.duration_seconds else "Duration : N/A",
        f"Started  : {run.started_at}",
        f"Completed: {run.completed_at}",
        "=" * 60,
        "",
    ]
    for nr in node_runs:
        lines += [
            f"[{nr.node_type.upper():10}] {nr.node_title} → {nr.status} ({nr.duration_seconds or 0:.2f}s)",
        ]
        if nr.stdout_log:
            for l in nr.stdout_log.splitlines():
                lines.append(f"  {l}")
        if nr.stderr_log:
            for l in nr.stderr_log.splitlines():
                lines.append(f"  ERR: {l}")
        lines.append("")

    return PlainTextResponse(
        "\n".join(lines),
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="run_{run_id}.log"'},
    )

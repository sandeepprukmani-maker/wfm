"""
FlowForge — Variables Router (Sprint 2)

Persistent key-value store for workflow variables.
Variables are read by Get Variable nodes and written by Set Variable nodes.
They persist across executions (unlike node output_data which is per-run).

Scopes:
  workflow  — visible only within one specific workflow
  global    — visible across all workflows owned by the user
"""

import logging
from datetime import datetime
from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db, WorkflowVariable

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Schemas ───────────────────────────────────────────────────────────────────

class VariableCreate(BaseModel):
    key:         str
    value:       Any
    value_type:  str  = "string"    # string | number | boolean | json
    description: str  = ""
    scope:       str  = "workflow"  # workflow | global
    workflow_id: Optional[str] = None


class VariableUpdate(BaseModel):
    value:       Optional[Any]  = None
    description: Optional[str] = None
    value_type:  Optional[str] = None


def _var_dict(v: WorkflowVariable) -> dict:
    return {
        "id":          v.id,
        "key":         v.key,
        "value":       v.value,
        "value_type":  v.value_type,
        "description": v.description,
        "scope":       "global" if v.workflow_id is None else "workflow",
        "workflow_id": v.workflow_id,
        "created_at":  v.created_at.isoformat(),
        "updated_at":  v.updated_at.isoformat(),
    }


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/")
async def list_variables(
    workflow_id:  Optional[str] = None,
    scope:        Optional[str] = None,    # workflow | global | all
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """
    List variables for the current user.
    - workflow_id + scope=workflow  → only this workflow's vars
    - scope=global                  → only global vars
    - (default) workflow_id given   → workflow vars + global vars
    """
    q = db.query(WorkflowVariable).filter_by(owner_id=current_user["sub"])

    if scope == "global":
        q = q.filter(WorkflowVariable.workflow_id.is_(None))
    elif scope == "workflow" and workflow_id:
        q = q.filter_by(workflow_id=workflow_id)
    elif workflow_id:
        # Default: return both workflow-scoped and global
        q = q.filter(
            (WorkflowVariable.workflow_id == workflow_id) |
            WorkflowVariable.workflow_id.is_(None)
        )

    vars_ = q.order_by(WorkflowVariable.key).all()
    return {"variables": [_var_dict(v) for v in vars_], "total": len(vars_)}


@router.post("/", status_code=201)
async def create_variable(
    body:         VariableCreate,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wf_id = body.workflow_id if body.scope == "workflow" else None

    # Upsert — if key already exists in scope, update it
    existing = db.query(WorkflowVariable).filter_by(
        owner_id=current_user["sub"],
        workflow_id=wf_id,
        key=body.key,
    ).first()

    if existing:
        existing.value       = body.value
        existing.value_type  = body.value_type
        existing.description = body.description
        existing.updated_at  = datetime.utcnow()
        db.commit()
        logger.info(f"Variable upserted: {body.key!r} (scope={body.scope})")
        return _var_dict(existing)

    var = WorkflowVariable(
        owner_id=current_user["sub"],
        workflow_id=wf_id,
        key=body.key,
        value=body.value,
        value_type=body.value_type,
        description=body.description,
    )
    db.add(var)
    db.commit()
    db.refresh(var)
    logger.info(f"Variable created: {body.key!r} (scope={body.scope})")
    return _var_dict(var)


@router.get("/{var_id}")
async def get_variable(
    var_id:       str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    var = db.query(WorkflowVariable).filter_by(id=var_id, owner_id=current_user["sub"]).first()
    if not var:
        raise HTTPException(404, "Variable not found")
    return _var_dict(var)


@router.put("/{var_id}")
async def update_variable(
    var_id:       str,
    body:         VariableUpdate,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    var = db.query(WorkflowVariable).filter_by(id=var_id, owner_id=current_user["sub"]).first()
    if not var:
        raise HTTPException(404, "Variable not found")
    if body.value is not None:       var.value       = body.value
    if body.description is not None: var.description = body.description
    if body.value_type is not None:  var.value_type  = body.value_type
    var.updated_at = datetime.utcnow()
    db.commit()
    return _var_dict(var)


@router.delete("/{var_id}", status_code=204)
async def delete_variable(
    var_id:       str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    var = db.query(WorkflowVariable).filter_by(id=var_id, owner_id=current_user["sub"]).first()
    if not var:
        raise HTTPException(404, "Variable not found")
    db.delete(var)
    db.commit()


@router.delete("/")
async def bulk_delete_variables(
    workflow_id:  Optional[str] = None,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """Delete all variables for a workflow (or all global vars if no workflow_id)."""
    q = db.query(WorkflowVariable).filter_by(owner_id=current_user["sub"])
    if workflow_id:
        q = q.filter_by(workflow_id=workflow_id)
    else:
        q = q.filter(WorkflowVariable.workflow_id.is_(None))
    count = q.count()
    q.delete()
    db.commit()
    return {"deleted": count}

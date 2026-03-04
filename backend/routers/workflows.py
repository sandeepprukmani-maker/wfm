"""FlowForge — Workflows Router (v2, fixed + enhanced)"""

import json
import io
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from database import get_db, Workflow, WorkflowVersion
from auth import get_current_user

router = APIRouter()


class WorkflowCreate(BaseModel):
    name:            str = Field(..., min_length=1, max_length=255)
    description:     str = ""
    dsl:             dict
    execution_mode:  str = "manual"
    tags:            List[str] = []
    prompt_source:   str = ""
    generated_by_ai: bool = False


class WorkflowUpdate(BaseModel):
    name:           Optional[str] = None
    description:    Optional[str] = None
    dsl:            Optional[dict] = None
    execution_mode: Optional[str] = None
    tags:           Optional[List[str]] = None
    change_note:    str = ""


def _wf_to_dict(wf: Workflow) -> dict:
    return {
        "id":             wf.id,
        "name":           wf.name,
        "description":    wf.description,
        "version":        wf.version,
        "execution_mode": wf.execution_mode,
        "tags":           wf.tags or [],
        "generated_by_ai":wf.generated_by_ai,
        "created_at":     wf.created_at.isoformat(),
        "updated_at":     wf.updated_at.isoformat(),
    }


@router.post("/", status_code=201)
async def create_workflow(
    body:         WorkflowCreate,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wf = Workflow(
        owner_id=current_user["sub"],
        name=body.name,
        description=body.description,
        dsl=body.dsl,
        execution_mode=body.execution_mode,
        tags=body.tags,
        prompt_source=body.prompt_source,
        generated_by_ai=body.generated_by_ai,
    )
    db.add(wf)
    db.commit()
    db.refresh(wf)

    ver = WorkflowVersion(
        workflow_id=wf.id, version=1,
        dsl=body.dsl, change_note="Initial version",
        created_by=current_user["sub"],
    )
    db.add(ver)
    db.commit()
    return {**_wf_to_dict(wf), "dsl": wf.dsl}


@router.get("/")
async def list_workflows(
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wfs = (
        db.query(Workflow)
        .filter_by(owner_id=current_user["sub"], is_active=True)
        .order_by(Workflow.updated_at.desc())
        .all()
    )
    return {"workflows": [_wf_to_dict(w) for w in wfs], "total": len(wfs)}


@router.post("/import")
async def import_workflow(
    file:         UploadFile = File(...),
    db:           Session    = Depends(get_db),
    current_user: dict       = Depends(get_current_user),
):
    """Import a workflow from exported JSON."""
    try:
        content = await file.read()
        data    = json.loads(content)
    except Exception as e:
        raise HTTPException(400, f"Invalid JSON file: {e}")

    wf_data = data.get("workflow") or data
    if "dsl" not in wf_data:
        raise HTTPException(400, "Invalid workflow file: missing 'dsl' field")

    wf = Workflow(
        owner_id=current_user["sub"],
        name=wf_data.get("name", "Imported Workflow"),
        description=wf_data.get("description", ""),
        dsl=wf_data["dsl"],
        execution_mode=wf_data.get("execution_mode", "manual"),
        tags=wf_data.get("tags", []),
    )
    db.add(wf)
    db.commit()
    db.refresh(wf)
    db.add(WorkflowVersion(workflow_id=wf.id, version=1, dsl=wf.dsl, change_note="Imported"))
    db.commit()

    return {**_wf_to_dict(wf), "message": "Workflow imported successfully"}


@router.get("/{workflow_id}")
async def get_workflow(
    workflow_id:  str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wf = db.query(Workflow).filter_by(id=workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")
    return {**_wf_to_dict(wf), "dsl": wf.dsl, "prompt_source": wf.prompt_source}


@router.put("/{workflow_id}")
async def update_workflow(
    workflow_id:  str,
    body:         WorkflowUpdate,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wf = db.query(Workflow).filter_by(id=workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")

    if body.name is not None:        wf.name           = body.name
    if body.description is not None: wf.description    = body.description
    if body.execution_mode is not None: wf.execution_mode = body.execution_mode
    if body.tags is not None:        wf.tags           = body.tags

    if body.dsl is not None:
        wf.dsl     = body.dsl
        wf.version += 1
        db.add(WorkflowVersion(
            workflow_id=wf.id,
            version=wf.version,
            dsl=body.dsl,
            change_note=body.change_note or f"v{wf.version}",
            created_by=current_user["sub"],
        ))

    # FIX: explicit updated_at (SQLite onupdate doesn't auto-trigger)
    wf.updated_at = datetime.utcnow()
    db.commit()
    return _wf_to_dict(wf)


@router.delete("/{workflow_id}", status_code=204)
async def delete_workflow(
    workflow_id:  str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wf = db.query(Workflow).filter_by(id=workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")
    wf.is_active  = False
    wf.updated_at = datetime.utcnow()
    db.commit()


@router.get("/{workflow_id}/versions")
async def list_versions(
    workflow_id:  str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wf = db.query(Workflow).filter_by(id=workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")
    versions = (
        db.query(WorkflowVersion)
        .filter_by(workflow_id=workflow_id)
        .order_by(WorkflowVersion.version.desc())
        .all()
    )
    return {
        "workflow_id": workflow_id,
        "current_version": wf.version,
        "versions": [
            {"version": v.version, "change_note": v.change_note, "created_at": v.created_at.isoformat()}
            for v in versions
        ],
    }


@router.get("/{workflow_id}/export")
async def export_workflow(
    workflow_id:  str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """Export workflow as downloadable JSON (n8n-style)."""
    wf = db.query(Workflow).filter_by(id=workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")

    export_data = {
        "flowforge_version": "2.0.0",
        "exported_at": datetime.utcnow().isoformat(),
        "workflow": {
            "name":           wf.name,
            "description":    wf.description,
            "execution_mode": wf.execution_mode,
            "tags":           wf.tags,
            "dsl":            wf.dsl,
        }
    }
    filename = wf.name.replace(" ", "_").lower() + ".json"
    content  = json.dumps(export_data, indent=2)
    return StreamingResponse(
        io.BytesIO(content.encode()),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )



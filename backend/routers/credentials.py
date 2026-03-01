"""
FlowForge — Credentials Router (v2, fixed)

Fixes from v1:
- mgr.update() was called with reversed argument order → fixed
- build_connector_by_id() was called but didn't exist → now exists in manager
- Hardcoded "demo-user" replaced with real JWT auth
- Added proper Pydantic validation
- Added mask_fields helper (never return secrets)
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Any, Dict, Optional
from sqlalchemy.orm import Session
from database import get_db
from auth import get_current_user

router = APIRouter()


class CredentialCreate(BaseModel):
    name:         str = Field(..., min_length=1, max_length=100, description="Credential reference name e.g. PROD_AIRFLOW")
    service_type: str = Field(..., description="airflow|mssql|http|s3|azure|sftp")
    fields:       Dict[str, Any] = Field(..., description="Service-specific fields (will be encrypted)")
    description:  str = ""

    class Config:
        json_schema_extra = {
            "example": {
                "name": "PROD_AIRFLOW",
                "service_type": "airflow",
                "fields": {"base_url": "http://airflow:8080", "username": "admin", "password": "secret"},
            }
        }


class CredentialUpdate(BaseModel):
    fields:      Optional[Dict[str, Any]] = None
    description: Optional[str] = None


VALID_TYPES = {"airflow", "mssql", "http", "s3", "azure", "sftp"}


def _mask_fields(fields: dict, service_type: str) -> dict:
    """Return fields with sensitive values masked. Never expose secrets."""
    sensitive = {"password", "secret", "key", "token", "pwd", "pass", "connection_string"}
    return {
        k: ("••••••••" if any(s in k.lower() for s in sensitive) else v)
        for k, v in fields.items()
    }


@router.post("/", status_code=201)
async def create_credential(
    body:         CredentialCreate,
    db:           Session          = Depends(get_db),
    current_user: dict             = Depends(get_current_user),
):
    if body.service_type not in VALID_TYPES:
        raise HTTPException(400, f"Invalid service_type. Must be one of: {sorted(VALID_TYPES)}")

    from credential_manager import CredentialManager
    mgr     = CredentialManager(db)
    cred_id = mgr.store(
        owner_id=current_user["sub"],
        name=body.name,
        service_type=body.service_type,
        fields=body.fields,
        description=body.description,
    )
    return {"id": cred_id, "name": body.name.upper(), "service_type": body.service_type, "message": "Credential stored (AES-256 encrypted)"}


@router.get("/")
async def list_credentials(
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    from credential_manager import CredentialManager
    mgr = CredentialManager(db)
    return {"credentials": mgr.list_credentials(current_user["sub"])}


@router.get("/{cred_id}")
async def get_credential_metadata(
    cred_id:      str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """Return credential metadata + masked fields. Never returns plaintext secrets."""
    from database import Credential
    from credential_manager import decrypt_fields
    cred = db.query(Credential).filter_by(id=cred_id, owner_id=current_user["sub"]).first()
    if not cred:
        raise HTTPException(404, "Credential not found")

    raw    = decrypt_fields(cred.encrypted_data)
    masked = _mask_fields(raw, cred.service_type)

    return {
        "id":           cred.id,
        "name":         cred.name,
        "service_type": cred.service_type,
        "description":  cred.description,
        "fields":       masked,          # ← masked, never plaintext
        "created_at":   cred.created_at.isoformat(),
        "updated_at":   cred.updated_at.isoformat(),
    }


@router.put("/{cred_id}")
async def update_credential(
    cred_id:      str,
    body:         CredentialUpdate,
    db:           Session          = Depends(get_db),
    current_user: dict             = Depends(get_current_user),
):
    if body.fields is None and body.description is None:
        raise HTTPException(400, "Provide at least one of: fields, description")

    from credential_manager import CredentialManager
    mgr = CredentialManager(db)
    try:
        # FIX: correct argument order — cred_id FIRST, owner_id SECOND
        mgr.update(
            cred_id=cred_id,
            owner_id=current_user["sub"],
            fields=body.fields,
            description=body.description,
        )
    except KeyError as e:
        raise HTTPException(404, str(e))
    return {"updated": True, "id": cred_id}


@router.delete("/{cred_id}", status_code=204)
async def delete_credential(
    cred_id:      str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    from credential_manager import CredentialManager
    mgr = CredentialManager(db)
    try:
        mgr.delete(cred_id, current_user["sub"])
    except KeyError as e:
        raise HTTPException(404, str(e))


@router.post("/{cred_id}/test")
async def test_credential(
    cred_id:      str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """Test connectivity for a credential. FIX: now calls build_connector_by_id() which exists."""
    from credential_manager import CredentialManager
    mgr = CredentialManager(db)

    # Verify ownership
    from database import Credential
    cred = db.query(Credential).filter_by(id=cred_id, owner_id=current_user["sub"]).first()
    if not cred:
        raise HTTPException(404, "Credential not found")

    try:
        connector = mgr.build_connector_by_id(cred_id)  # ← FIX: method now exists
        result    = connector.health_check()
        return {"status": result.get("status", "unknown"), "details": result, "credential": cred.name}
    except Exception as e:
        return {"status": "error", "error": str(e), "credential": cred.name}

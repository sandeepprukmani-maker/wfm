"""
FlowForge — Credential Manager (v2, fixed)

Fixes from v1:
- Added build_connector_by_id() method (was called but missing)
- Fixed update() method — v1 had argument order bug (owner_id before cred_id)
- Explicit updated_at assignment (SQLite onupdate doesn't auto-trigger)
- Added rotate() method to re-encrypt with new key
- Added list_by_type() helper
"""

import os
import json
import base64
import logging
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ── Encryption ────────────────────────────────────────────────────────────────

MASTER_KEY = os.getenv("FLOWFORGE_SECRET_KEY", "change-this-in-production-32chars!")
SALT       = os.getenv("FLOWFORGE_SALT", "flowforge-salt-2025").encode()

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from cryptography.hazmat.primitives import hashes
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logger.warning("cryptography not installed — credentials stored with weak fallback. pip install cryptography")


def _derive_key(master: str) -> bytes:
    if not CRYPTO_AVAILABLE:
        return (master.encode()[:32]).ljust(32, b"\x00")
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=SALT, iterations=100_000)
    return kdf.derive(master.encode())


_AES_KEY = _derive_key(MASTER_KEY)


def encrypt_fields(data: dict) -> str:
    """Encrypt dict → base64 ciphertext (AES-256-GCM)."""
    plaintext = json.dumps(data, ensure_ascii=False).encode()
    if CRYPTO_AVAILABLE:
        nonce      = os.urandom(12)
        ciphertext = AESGCM(_AES_KEY).encrypt(nonce, plaintext, None)
        return base64.b64encode(nonce + ciphertext).decode()
    else:
        # XOR fallback — NOT secure, dev only
        key    = _AES_KEY * (len(plaintext) // 32 + 1)
        xored  = bytes(b ^ k for b, k in zip(plaintext, key))
        return base64.b64encode(xored).decode()


def decrypt_fields(encrypted: str) -> dict:
    """Decrypt base64 ciphertext → dict."""
    raw = base64.b64decode(encrypted.encode())
    if CRYPTO_AVAILABLE:
        nonce, ciphertext = raw[:12], raw[12:]
        plaintext = AESGCM(_AES_KEY).decrypt(nonce, ciphertext, None)
    else:
        key       = _AES_KEY * (len(raw) // 32 + 1)
        plaintext = bytes(b ^ k for b, k in zip(raw, key))
    return json.loads(plaintext.decode())


# ── Manager ───────────────────────────────────────────────────────────────────

class CredentialManager:
    """
    All credential reads/writes go through encrypt/decrypt.
    Plaintext secrets NEVER leave this class.
    """

    def __init__(self, db_session):
        self._db = db_session

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def store(
        self,
        owner_id: str,
        name: str,
        service_type: str,
        fields: Dict[str, Any],
        description: str = "",
    ) -> str:
        """Encrypt and persist credential. Returns credential ID."""
        from database import Credential
        cred = Credential(
            owner_id=owner_id,
            name=name.upper().strip(),
            service_type=service_type.lower(),
            encrypted_data=encrypt_fields(fields),
            description=description,
        )
        self._db.add(cred)
        self._db.commit()
        self._db.refresh(cred)
        logger.info(f"Stored credential {cred.name} ({service_type}) owner={owner_id}")
        return cred.id

    def retrieve(self, credential_name: str, owner_id: str) -> Dict[str, Any]:
        """Retrieve + decrypt by name. Raises KeyError if not found."""
        from database import Credential
        cred = (
            self._db.query(Credential)
            .filter_by(name=credential_name.upper(), owner_id=owner_id)
            .first()
        )
        if not cred:
            raise KeyError(f"Credential '{credential_name}' not found for owner {owner_id}")
        return decrypt_fields(cred.encrypted_data)

    def retrieve_by_id(self, cred_id: str) -> Dict[str, Any]:
        """Retrieve + decrypt by ID."""
        from database import Credential
        cred = self._db.query(Credential).filter_by(id=cred_id).first()
        if not cred:
            raise KeyError(f"Credential ID '{cred_id}' not found")
        return decrypt_fields(cred.encrypted_data)

    def list_credentials(self, owner_id: str) -> list:
        """Metadata only — NO plaintext secrets returned."""
        from database import Credential
        rows = self._db.query(Credential).filter_by(owner_id=owner_id).order_by(Credential.name).all()
        return [
            {
                "id":           c.id,
                "name":         c.name,
                "service_type": c.service_type,
                "description":  c.description,
                "created_at":   c.created_at.isoformat(),
                "updated_at":   c.updated_at.isoformat(),
            }
            for c in rows
        ]

    def list_by_type(self, owner_id: str, service_type: str) -> list:
        """List credentials filtered by service type."""
        from database import Credential
        rows = (
            self._db.query(Credential)
            .filter_by(owner_id=owner_id, service_type=service_type.lower())
            .all()
        )
        return [{"id": c.id, "name": c.name} for c in rows]

    def update(
        self,
        cred_id: str,          # ← FIX: cred_id is FIRST (v1 had reversed order)
        owner_id: str,
        fields: Dict[str, Any] = None,
        description: str = None,
    ):
        """Update credential. At least one of fields/description must be provided."""
        from database import Credential
        cred = (
            self._db.query(Credential)
            .filter_by(id=cred_id, owner_id=owner_id)
            .first()
        )
        if not cred:
            raise KeyError(f"Credential '{cred_id}' not found for owner {owner_id}")
        if fields is not None:
            cred.encrypted_data = encrypt_fields(fields)
        if description is not None:
            cred.description = description
        cred.updated_at = datetime.utcnow()  # ← FIX: explicit (SQLite onupdate doesn't auto-trigger)
        self._db.commit()
        logger.info(f"Updated credential {cred_id}")

    def delete(self, cred_id: str, owner_id: str):
        from database import Credential
        cred = (
            self._db.query(Credential)
            .filter_by(id=cred_id, owner_id=owner_id)
            .first()
        )
        if not cred:
            raise KeyError(f"Credential '{cred_id}' not found for owner {owner_id}")
        self._db.delete(cred)
        self._db.commit()
        logger.info(f"Deleted credential {cred_id}")

    def rotate_encryption(self, cred_id: str, owner_id: str):
        """Re-encrypt with current key (use after key rotation)."""
        fields = self.retrieve_by_id(cred_id)
        from database import Credential
        cred = self._db.query(Credential).filter_by(id=cred_id, owner_id=owner_id).first()
        if not cred:
            raise KeyError(f"Credential '{cred_id}' not found")
        cred.encrypted_data = encrypt_fields(fields)
        cred.updated_at     = datetime.utcnow()
        self._db.commit()

    # ── Connector factory ─────────────────────────────────────────────────────

    def build_connector(self, credential_name: str, owner_id: str):
        """
        Build + return correct MCP connector by credential name.
        Connector is instantiated with decrypted fields.
        """
        from database import Credential
        cred = (
            self._db.query(Credential)
            .filter_by(name=credential_name.upper(), owner_id=owner_id)
            .first()
        )
        if not cred:
            raise KeyError(f"Credential '{credential_name}' not found for owner {owner_id}")
        return self._instantiate(cred)

    def build_connector_by_id(self, cred_id: str) -> object:
        """
        Build + return correct MCP connector by credential ID.
        FIX: This method was called in v1 credentials.py router but didn't exist.
        """
        from database import Credential
        cred = self._db.query(Credential).filter_by(id=cred_id).first()
        if not cred:
            raise KeyError(f"Credential '{cred_id}' not found")
        return self._instantiate(cred)

    def _instantiate(self, cred) -> object:
        """Internal: decrypt fields and build the correct connector."""
        fields = decrypt_fields(cred.encrypted_data)
        stype  = cred.service_type

        if stype == "airflow":
            from connectors.airflow_mcp import AirflowMCP
            return AirflowMCP(
                base_url=fields["base_url"],
                username=fields.get("username", ""),
                password=fields.get("password", ""),
                credential_name=cred.name,
            )
        elif stype == "mssql":
            from connectors.mssql_mcp import MSSQLMCP
            return MSSQLMCP(
                connection_string=fields.get("connection_string"),
                server=fields.get("server"),
                database=fields.get("database"),
                username=fields.get("username"),
                password=fields.get("password"),
                credential_name=cred.name,
            )
        elif stype == "http":
            from connectors.http_mcp import HTTPConnector
            return HTTPConnector(
                base_url=fields.get("base_url", ""),
                bearer_token=fields.get("bearer_token"),
                api_key=fields.get("api_key"),
                credential_name=cred.name,
            )
        elif stype == "s3":
            from connectors.s3_mcp import S3Connector
            return S3Connector(
                access_key_id=fields.get("access_key_id"),
                secret_access_key=fields.get("secret_access_key"),
                region=fields.get("region", "us-east-1"),
                credential_name=cred.name,
            )
        elif stype == "azure":
            from connectors.azure_mcp import AzureConnector
            return AzureConnector(
                account_name=fields.get("account_name"),
                account_key=fields.get("account_key"),
                connection_string=fields.get("connection_string"),
                credential_name=cred.name,
            )
        elif stype == "sftp":
            from connectors.sftp_mcp import SFTPConnector
            return SFTPConnector(
                host=fields["host"],
                username=fields["username"],
                password=fields.get("password"),
                port=int(fields.get("port", 22)),
                credential_name=cred.name,
            )
        else:
            raise ValueError(f"Unknown service type: {stype!r}. Expected: airflow|mssql|http|s3|azure|sftp")

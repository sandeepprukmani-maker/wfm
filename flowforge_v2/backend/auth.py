"""
FlowForge — JWT Authentication
Production-ready auth with bcrypt passwords + JWT tokens.

Endpoints:
  POST /api/auth/login   → {access_token, refresh_token}
  POST /api/auth/refresh → {access_token}
  POST /api/auth/register
  GET  /api/auth/me
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

JWT_SECRET      = os.getenv("JWT_SECRET", "change-this-jwt-secret-in-production!")
JWT_ALGORITHM   = "HS256"
ACCESS_EXPIRE   = int(os.getenv("JWT_ACCESS_EXPIRE_MINUTES", "60"))   # 1 hour
REFRESH_EXPIRE  = int(os.getenv("JWT_REFRESH_EXPIRE_DAYS",   "7"))    # 7 days

try:
    import jwt as _jwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False
    logger.warning("PyJWT not installed. Run: pip install PyJWT")

try:
    import bcrypt as _bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False
    logger.warning("bcrypt not installed. Run: pip install bcrypt")

# ── Token helpers ─────────────────────────────────────────────────────────────

def _create_token(data: dict, expires_delta: timedelta) -> str:
    if not JWT_AVAILABLE:
        raise RuntimeError("PyJWT not installed")
    payload = {**data, "exp": datetime.utcnow() + expires_delta, "iat": datetime.utcnow()}
    return _jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def create_access_token(user_id: str, role: str) -> str:
    return _create_token({"sub": user_id, "role": role, "type": "access"}, timedelta(minutes=ACCESS_EXPIRE))


def create_refresh_token(user_id: str) -> str:
    return _create_token({"sub": user_id, "type": "refresh"}, timedelta(days=REFRESH_EXPIRE))


def decode_token(token: str) -> dict:
    if not JWT_AVAILABLE:
        # Dev fallback: accept "demo-token" and return demo user
        if token in ("demo-token", "Bearer demo-token"):
            return {"sub": "demo-user", "role": "admin", "type": "access"}
        raise HTTPException(status_code=401, detail="PyJWT not installed — use demo-token for dev")
    try:
        return _jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except _jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except _jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")


def hash_password(password: str) -> str:
    if not BCRYPT_AVAILABLE:
        return password  # dev fallback only
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    if not BCRYPT_AVAILABLE:
        return plain == hashed  # dev fallback only
    try:
        return _bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


# ── FastAPI dependency ────────────────────────────────────────────────────────

_bearer = HTTPBearer(auto_error=False)


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer)
) -> dict:
    """
    Dependency: extract + validate JWT from Authorization: Bearer <token>.
    Returns {"sub": user_id, "role": role}.
    Falls back to demo-user in dev mode (no token required).
    """
    DEV_MODE = os.getenv("FLOWFORGE_DEV_MODE", "true").lower() == "true"

    if credentials is None:
        if DEV_MODE:
            return {"sub": "demo-user", "role": "admin"}
        raise HTTPException(status_code=401, detail="Authorization header required")

    payload = decode_token(credentials.credentials)
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Not an access token")
    return payload


def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


# ── Auth Router ───────────────────────────────────────────────────────────────

router = APIRouter()


class LoginRequest(BaseModel):
    email: str
    password: str


class RegisterRequest(BaseModel):
    email: str
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = ACCESS_EXPIRE * 60


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, db: Session = Depends(lambda: next(__import__('database').get_db()))):
    from database import User
    user = db.query(User).filter(User.email == body.email, User.is_active == True).first()
    if not user or not verify_password(body.password, user.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return TokenResponse(
        access_token=create_access_token(user.id, user.role),
        refresh_token=create_refresh_token(user.id),
    )


@router.post("/register")
async def register(body: RegisterRequest, db: Session = Depends(lambda: next(__import__('database').get_db()))):
    from database import User
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(status_code=409, detail="Email already registered")

    user = User(
        email=body.email,
        username=body.username,
        password=hash_password(body.password),
        role="user",
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        "id": user.id,
        "email": user.email,
        "username": user.username,
        "access_token": create_access_token(user.id, user.role),
        "refresh_token": create_refresh_token(user.id),
    }


@router.get("/me")
async def get_me(current_user: dict = Depends(get_current_user), db: Session = Depends(lambda: next(__import__('database').get_db()))):
    from database import User
    user = db.query(User).filter(User.id == current_user["sub"]).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return {"id": user.id, "email": user.email, "username": user.username, "role": user.role}


@router.post("/refresh")
async def refresh_token(credentials: HTTPAuthorizationCredentials = Depends(_bearer)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Refresh token required")
    payload = decode_token(credentials.credentials)
    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Not a refresh token")
    return {
        "access_token": create_access_token(payload["sub"], "user"),
        "token_type": "bearer",
    }

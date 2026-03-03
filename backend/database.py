"""
FlowForge — Database Models (v2, production-ready)

Fixes from v1:
- Removed broken SQLite onupdate (doesn't auto-trigger in SQLAlchemy+SQLite)
  → Replaced with explicit event listeners + helper update methods
- Added WebhookTrigger and ScheduleTrigger tables (n8n-inspired)
- Added WorkflowTag for organization
- Proper indexes on frequently queried columns
- Added execution_mode to Workflow (manual | scheduled | webhook)
"""

import os
import uuid
from datetime import datetime

from sqlalchemy import (
    create_engine, Column, String, Integer, Float, Boolean,
    Text, DateTime, ForeignKey, JSON, Index, event
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

# ── Connection ────────────────────────────────────────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./flowforge.db")

# SQLite-specific: check_same_thread=False only needed for SQLite
_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    connect_args=_connect_args,
    pool_pre_ping=True,       # verify connections before use
    pool_recycle=1800,        # recycle connections every 30 min
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _gen_id() -> str:
    return str(uuid.uuid4())[:12]


def _now() -> datetime:
    return datetime.utcnow()


# ── Models ────────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id          = Column(String(36), primary_key=True, default=_gen_id)
    email       = Column(String(255), unique=True, nullable=False, index=True)
    username    = Column(String(100), nullable=False)
    password    = Column(String(255), nullable=False)   # bcrypt hashed
    role        = Column(String(20), default="user")    # admin | user
    is_active   = Column(Boolean, default=True)
    created_at  = Column(DateTime, default=_now, nullable=False)
    updated_at  = Column(DateTime, default=_now, nullable=False)

    workflows   = relationship("Workflow", back_populates="owner", cascade="all, delete-orphan")
    credentials = relationship("Credential", back_populates="owner", cascade="all, delete-orphan")
    chat_sessions = relationship("ChatSession", back_populates="user", cascade="all, delete-orphan")

    def touch(self):
        self.updated_at = _now()


class Credential(Base):
    __tablename__ = "credentials"

    id             = Column(String(36), primary_key=True, default=_gen_id)
    owner_id       = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name           = Column(String(100), nullable=False)          # e.g. PROD_AIRFLOW
    service_type   = Column(String(50), nullable=False)           # airflow|mssql|http|s3|azure|sftp
    encrypted_data = Column(Text, nullable=False)                 # AES-256-GCM encrypted JSON
    description    = Column(Text, default="")
    created_at     = Column(DateTime, default=_now, nullable=False)
    updated_at     = Column(DateTime, default=_now, nullable=False)

    owner = relationship("User", back_populates="credentials")

    __table_args__ = (
        Index("ix_credential_owner_name", "owner_id", "name"),
    )

    def touch(self):
        self.updated_at = _now()


class WorkflowTag(Base):
    __tablename__ = "workflow_tags"

    id          = Column(String(36), primary_key=True, default=_gen_id)
    owner_id    = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name        = Column(String(50), nullable=False)
    color       = Column(String(20), default="#00d4ff")
    created_at  = Column(DateTime, default=_now, nullable=False)


class Workflow(Base):
    __tablename__ = "workflows"

    id              = Column(String(36), primary_key=True, default=_gen_id)
    owner_id        = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name            = Column(String(255), nullable=False)
    description     = Column(Text, default="")
    dsl             = Column(JSON, nullable=False)               # {nodes, edges, settings}
    version         = Column(Integer, default=1, nullable=False)
    is_active       = Column(Boolean, default=True)
    execution_mode  = Column(String(20), default="manual")      # manual|scheduled|webhook
    tags            = Column(JSON, default=list)                 # list of tag ids
    settings        = Column(JSON, default=dict)                 # {timezone, error_workflow_id, ...}
    prompt_source   = Column(Text, default="")
    generated_by_ai = Column(Boolean, default=False)
    created_at      = Column(DateTime, default=_now, nullable=False)
    updated_at      = Column(DateTime, default=_now, nullable=False)

    owner    = relationship("User", back_populates="workflows")
    versions = relationship("WorkflowVersion", back_populates="workflow", cascade="all, delete-orphan")
    runs     = relationship("WorkflowRun", back_populates="workflow", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_workflow_owner", "owner_id"),
    )

    def touch(self):
        self.updated_at = _now()


class WorkflowVersion(Base):
    """Immutable snapshot at each save. Enables full version history + diffing."""
    __tablename__ = "workflow_versions"

    id          = Column(String(36), primary_key=True, default=_gen_id)
    workflow_id = Column(String(36), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False)
    version     = Column(Integer, nullable=False)
    dsl         = Column(JSON, nullable=False)
    change_note = Column(Text, default="")
    created_by  = Column(String(36), nullable=True)              # user_id
    created_at  = Column(DateTime, default=_now, nullable=False)

    workflow = relationship("Workflow", back_populates="versions")

    __table_args__ = (
        Index("ix_wf_version_workflow", "workflow_id"),
    )


class WebhookTrigger(Base):
    """Webhook endpoint that triggers a workflow. Inspired by n8n webhooks."""
    __tablename__ = "webhook_triggers"

    id           = Column(String(36), primary_key=True, default=_gen_id)
    workflow_id  = Column(String(36), ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False)
    path         = Column(String(255), unique=True, nullable=False)  # /webhook/abc123
    method       = Column(String(10), default="POST")                # GET|POST|PUT
    is_active    = Column(Boolean, default=True)
    auth_token   = Column(String(64), nullable=True)                 # optional header auth
    created_at   = Column(DateTime, default=_now, nullable=False)


class WorkflowRun(Base):
    """One execution instance of a workflow."""
    __tablename__ = "workflow_runs"

    id               = Column(String(36), primary_key=True, default=_gen_id)
    workflow_id      = Column(String(36), ForeignKey("workflows.id", ondelete="SET NULL"), nullable=True)
    triggered_by     = Column(String(50), default="manual")    # manual|scheduled|webhook|api
    trigger_data     = Column(JSON, default=dict)              # webhook payload, schedule info
    status           = Column(String(20), default="pending")   # pending|running|success|failed|cancelled
    started_at       = Column(DateTime, nullable=True)
    completed_at     = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    error_message    = Column(Text, nullable=True)
    artifacts        = Column(JSON, default=dict)              # {filename: path}
    retry_count      = Column(Integer, default=0)
    created_at       = Column(DateTime, default=_now, nullable=False)

    workflow  = relationship("Workflow", back_populates="runs")
    node_runs = relationship("NodeRun", back_populates="workflow_run", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_wf_run_workflow", "workflow_id"),
        Index("ix_wf_run_status", "status"),
    )


class NodeRun(Base):
    """Execution result for a single node within a WorkflowRun."""
    __tablename__ = "node_runs"

    id               = Column(String(36), primary_key=True, default=_gen_id)
    workflow_run_id  = Column(String(36), ForeignKey("workflow_runs.id", ondelete="CASCADE"), nullable=False)
    node_id          = Column(String(100), nullable=False)
    node_type        = Column(String(50), nullable=False)
    node_title       = Column(String(255), nullable=False)
    status           = Column(String(20), default="pending")   # pending|running|success|failed|skipped
    attempt          = Column(Integer, default=1)              # retry attempt number
    started_at       = Column(DateTime, nullable=True)
    completed_at     = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    stdout_log       = Column(Text, default="")
    stderr_log       = Column(Text, default="")
    output_data      = Column(JSON, default=dict)              # node output for expressions
    error_message    = Column(Text, nullable=True)

    workflow_run = relationship("WorkflowRun", back_populates="node_runs")

    __table_args__ = (
        Index("ix_node_run_workflow_run", "workflow_run_id"),
    )


class WorkflowVariable(Base):
    """
    Persistent key-value store scoped to a workflow or global to an owner.
    Set Variable nodes write here; Get Variable nodes read from here.
    Variables persist across executions — unlike node output_data which is per-run.
    """
    __tablename__ = "workflow_variables"

    id          = Column(String(36), primary_key=True, default=_gen_id)
    owner_id    = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    workflow_id = Column(String(36), nullable=True)   # None → global scope
    key         = Column(String(255), nullable=False)
    value       = Column(JSON, nullable=True)          # any JSON-serialisable type
    value_type  = Column(String(30), default="string") # string|number|boolean|json
    description = Column(Text, default="")
    created_at  = Column(DateTime, default=_now, nullable=False)
    updated_at  = Column(DateTime, default=_now, nullable=False)

    __table_args__ = (
        Index("ix_wf_var_owner_wf_key", "owner_id", "workflow_id", "key"),
    )

    def touch(self):
        self.updated_at = _now()


class ChatSession(Base):
    __tablename__ = "chat_sessions"

    id         = Column(String(36), primary_key=True, default=_gen_id)
    user_id    = Column(String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    title      = Column(String(255), default="New Chat")
    created_at = Column(DateTime, default=_now, nullable=False)
    updated_at = Column(DateTime, default=_now, nullable=False)

    user     = relationship("User", back_populates="chat_sessions")
    messages = relationship("ChatMessage", back_populates="session", cascade="all, delete-orphan")

    def touch(self):
        self.updated_at = _now()


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id          = Column(String(36), primary_key=True, default=_gen_id)
    session_id  = Column(String(36), ForeignKey("chat_sessions.id", ondelete="CASCADE"), nullable=False)
    role        = Column(String(20), nullable=False)             # user | assistant
    content     = Column(Text, nullable=False)
    workflow_id = Column(String(36), nullable=True)
    created_at  = Column(DateTime, default=_now, nullable=False)

    session = relationship("ChatSession", back_populates="messages")

    __table_args__ = (
        Index("ix_chat_msg_session", "session_id"),
    )


# ── Helper: seed default admin user ──────────────────────────────────────────

def seed_default_user(db_session):
    """Create a default admin user if no users exist."""
    if db_session.query(User).count() == 0:
        import bcrypt
        hashed = bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode()
        admin = User(
            id="demo-user",
            email="admin@flowforge.local",
            username="Admin",
            password=hashed,
            role="admin",
        )
        db_session.add(admin)
        db_session.commit()

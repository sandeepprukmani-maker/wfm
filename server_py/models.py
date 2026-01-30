from sqlalchemy import Column, Integer, String, Text, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import json
import os

Base = declarative_base()

def timestamp_to_iso(ts):
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.isoformat()
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts / 1000 if ts > 1e11 else ts).isoformat()
    return str(ts)

class Workflow(Base):
    __tablename__ = 'workflows'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    nodes = Column(Text, nullable=False, default='[]')
    edges = Column(Text, nullable=False, default='[]')
    last_prompt = Column(Text, nullable=True)
    created_at = Column(Integer, nullable=True)
    updated_at = Column(Integer, nullable=True)
    
    executions = relationship("Execution", back_populates="workflow")
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'nodes': json.loads(self.nodes) if isinstance(self.nodes, str) else self.nodes,
            'edges': json.loads(self.edges) if isinstance(self.edges, str) else self.edges,
            'lastPrompt': self.last_prompt,
            'createdAt': timestamp_to_iso(self.created_at),
            'updatedAt': timestamp_to_iso(self.updated_at)
        }

class Credential(Base):
    __tablename__ = 'credentials'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    data = Column(Text, nullable=False)
    created_at = Column(Integer, nullable=True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'type': self.type,
            'data': json.loads(self.data) if isinstance(self.data, str) else self.data,
            'createdAt': timestamp_to_iso(self.created_at)
        }

class Execution(Base):
    __tablename__ = 'executions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    workflow_id = Column(Integer, ForeignKey('workflows.id'), nullable=False)
    status = Column(String, nullable=False, default='pending')
    logs = Column(Text, default='[]')
    results = Column(Text, default='{}')
    started_at = Column(Integer, nullable=True)
    completed_at = Column(Integer, nullable=True)
    
    workflow = relationship("Workflow", back_populates="executions")
    
    def to_dict(self):
        return {
            'id': self.id,
            'workflowId': self.workflow_id,
            'status': self.status,
            'logs': json.loads(self.logs) if isinstance(self.logs, str) else self.logs,
            'results': json.loads(self.results) if isinstance(self.results, str) else self.results,
            'startedAt': timestamp_to_iso(self.started_at),
            'completedAt': timestamp_to_iso(self.completed_at)
        }

db_url = 'sqlite:///local.db'
engine = create_engine(db_url, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

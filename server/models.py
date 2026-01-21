from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import json

db = SQLAlchemy()


class Workflow(db.Model):
    __tablename__ = 'workflows'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    nodes = db.Column(db.Text, default='[]')
    edges = db.Column(db.Text, default='[]')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'nodes': json.loads(self.nodes) if self.nodes else [],
            'edges': json.loads(self.edges) if self.edges else [],
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class WorkflowExecution(db.Model):
    __tablename__ = 'workflow_executions'
    
    id = db.Column(db.Integer, primary_key=True)
    workflow_id = db.Column(db.Integer, db.ForeignKey('workflows.id'), nullable=False)
    status = db.Column(db.String(50), default='pending')
    started_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)
    outputs = db.Column(db.Text, default='{}')
    error = db.Column(db.Text)
    
    workflow = db.relationship('Workflow', backref=db.backref('executions', lazy=True))
    
    def to_dict(self):
        return {
            'id': self.id,
            'workflow_id': self.workflow_id,
            'status': self.status,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'outputs': json.loads(self.outputs) if self.outputs else {},
            'error': self.error
        }


class NodeExecution(db.Model):
    __tablename__ = 'node_executions'
    
    id = db.Column(db.Integer, primary_key=True)
    execution_id = db.Column(db.Integer, db.ForeignKey('workflow_executions.id'), nullable=False)
    node_id = db.Column(db.String(255), nullable=False)
    node_type = db.Column(db.String(50), nullable=False)
    status = db.Column(db.String(50), default='pending')
    started_at = db.Column(db.DateTime)
    completed_at = db.Column(db.DateTime)
    inputs = db.Column(db.Text, default='{}')
    outputs = db.Column(db.Text, default='{}')
    logs = db.Column(db.Text)
    error = db.Column(db.Text)
    
    execution = db.relationship('WorkflowExecution', backref=db.backref('node_executions', lazy=True))
    
    def to_dict(self):
        return {
            'id': self.id,
            'execution_id': self.execution_id,
            'node_id': self.node_id,
            'node_type': self.node_type,
            'status': self.status,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'inputs': json.loads(self.inputs) if self.inputs else {},
            'outputs': json.loads(self.outputs) if self.outputs else {},
            'logs': self.logs,
            'error': self.error
        }

from sqlalchemy.orm import Session
from sqlalchemy import desc
from datetime import datetime
import json
import time
from .models import Workflow, Credential, Execution, SessionLocal

def get_timestamp_ms():
    return int(time.time() * 1000)

class DatabaseStorage:
    def __init__(self):
        self.Session = SessionLocal
    
    def get_db(self):
        return self.Session()
    
    def get_workflows(self):
        with self.get_db() as db:
            workflows = db.query(Workflow).order_by(desc(Workflow.updated_at)).all()
            return [w.to_dict() for w in workflows]
    
    def get_workflow(self, id: int):
        with self.get_db() as db:
            workflow = db.query(Workflow).filter(Workflow.id == id).first()
            return workflow.to_dict() if workflow else None
    
    def create_workflow(self, data: dict):
        with self.get_db() as db:
            now = get_timestamp_ms()
            workflow = Workflow(
                name=data.get('name', ''),
                description=data.get('description'),
                nodes=json.dumps(data.get('nodes', [])),
                edges=json.dumps(data.get('edges', [])),
                last_prompt=data.get('lastPrompt'),
                created_at=now,
                updated_at=now
            )
            db.add(workflow)
            db.commit()
            db.refresh(workflow)
            return workflow.to_dict()
    
    def update_workflow(self, id: int, data: dict):
        with self.get_db() as db:
            workflow = db.query(Workflow).filter(Workflow.id == id).first()
            if not workflow:
                return None
            
            if 'name' in data:
                workflow.name = data['name']
            if 'description' in data:
                workflow.description = data['description']
            if 'nodes' in data:
                workflow.nodes = json.dumps(data['nodes'])
            if 'edges' in data:
                workflow.edges = json.dumps(data['edges'])
            if 'lastPrompt' in data:
                workflow.last_prompt = data['lastPrompt']
            
            workflow.updated_at = get_timestamp_ms()
            db.commit()
            db.refresh(workflow)
            return workflow.to_dict()
    
    def delete_workflow(self, id: int):
        with self.get_db() as db:
            # Delete related executions first to satisfy foreign key constraints
            db.query(Execution).filter(Execution.workflow_id == id).delete(synchronize_session=False)
            
            workflow = db.query(Workflow).filter(Workflow.id == id).first()
            if workflow:
                db.delete(workflow)
                db.commit()
    
    def get_credentials(self):
        with self.get_db() as db:
            credentials = db.query(Credential).order_by(desc(Credential.created_at)).all()
            return [c.to_dict() for c in credentials]
    
    def get_credential(self, id: int):
        with self.get_db() as db:
            credential = db.query(Credential).filter(Credential.id == id).first()
            return credential.to_dict() if credential else None
    
    def create_credential(self, data: dict):
        with self.get_db() as db:
            credential = Credential(
                name=data.get('name', ''),
                type=data.get('type', ''),
                data=json.dumps(data.get('data', {})),
                created_at=get_timestamp_ms()
            )
            db.add(credential)
            db.commit()
            db.refresh(credential)
            return credential.to_dict()
    
    def delete_credential(self, id: int):
        with self.get_db() as db:
            credential = db.query(Credential).filter(Credential.id == id).first()
            if credential:
                db.delete(credential)
                db.commit()
    
    def get_executions(self, workflow_id: int = None):
        with self.get_db() as db:
            query = db.query(Execution).order_by(desc(Execution.started_at))
            if workflow_id:
                query = query.filter(Execution.workflow_id == workflow_id)
            executions = query.all()
            return [e.to_dict() for e in executions]
    
    def get_execution(self, id: int):
        with self.get_db() as db:
            execution = db.query(Execution).filter(Execution.id == id).first()
            return execution.to_dict() if execution else None
    
    def create_execution(self, workflow_id: int):
        with self.get_db() as db:
            execution = Execution(
                workflow_id=workflow_id,
                status='pending',
                logs=json.dumps([]),
                started_at=get_timestamp_ms()
            )
            db.add(execution)
            db.commit()
            db.refresh(execution)
            return execution.to_dict()
    
    def update_execution(self, id: int, status: str, logs: list, results: dict = None):
        with self.get_db() as db:
            execution = db.query(Execution).filter(Execution.id == id).first()
            if not execution:
                return None
            
            execution.status = status
            execution.logs = json.dumps(logs, default=str)
            if results:
                execution.results = json.dumps(results)
            if status in ['completed', 'failed']:
                execution.completed_at = get_timestamp_ms()
            
            db.commit()
            db.refresh(execution)
            return execution.to_dict()

    def delete_executions(self, workflow_id: int = None):
        with self.get_db() as db:
            query = db.query(Execution)
            if workflow_id:
                query = query.filter(Execution.workflow_id == workflow_id)
            query.delete(synchronize_session=False)
            db.commit()

storage = DatabaseStorage()

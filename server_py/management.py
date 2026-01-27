import os
import json
import io
import zipfile
from flask import request, jsonify, send_file
from .storage import storage
from .utils import log

def register_management_routes(app):
    @app.get('/api/credentials')
    def list_credentials():
        credentials = storage.get_credentials()
        return jsonify(credentials)

    @app.post('/api/credentials')
    def create_credential():
        data = request.get_json()
        credential = storage.create_credential(data)
        return jsonify(credential), 201

    @app.delete('/api/credentials/<int:id>')
    def delete_credential(id):
        storage.delete_credential(id)
        return '', 204

    @app.get('/api/executions')
    def list_executions():
        workflow_id = request.args.get('workflowId', type=int)
        executions = storage.get_executions(workflow_id)
        return jsonify(executions)

    @app.delete('/api/executions')
    def delete_executions():
        workflow_id = request.args.get('workflowId', type=int)
        storage.delete_executions(workflow_id)
        return '', 204

    @app.get('/api/executions/<int:id>')
    def get_execution(id):
        execution = storage.get_execution(id)
        if not execution:
            return jsonify({'message': 'Execution not found'}), 404
        return jsonify(execution)

    @app.get('/api/executions/<int:id>/export')
    def export_execution(id):
        execution = storage.get_execution(id)
        if not execution:
            return jsonify({'message': 'Execution not found'}), 404
        
        workflow = storage.get_workflow(execution['workflowId'])
        if not workflow:
            return jsonify({'message': 'Workflow not found'}), 404
        
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            code = f"# Python export\n# Workflow Data:\n{json.dumps(workflow, indent=2, default=str)}"
            zf.writestr('workflow.py', code)
            
            logs = execution.get('logs', [])
            log_content = '\n'.join([f"[{l.get('timestamp', '')}] {l.get('level', '')}: {l.get('message', '')}" for l in logs])
            zf.writestr('execution.log', log_content)
            
            results = execution.get('results', {})
            for node_id, csv_data in results.items():
                zf.writestr(f'results/node_{node_id}.csv', str(csv_data))
        
        buffer.seek(0)
        return send_file(
            buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'execution_{id}_export.zip'
        )

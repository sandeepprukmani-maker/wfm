import os
import time
import base64
import re
import requests
import json
import threading
from datetime import datetime, timedelta
from flask import request, jsonify, send_file
from .storage import storage
from .airflow_api import AirflowAPI, AIRFLOW_OPERATIONS
from .utils import log, resolve_variables, get_ai

def get_airflow_client(credential_id: int):
    cred = storage.get_credential(credential_id)
    if not cred or cred.get('type') != 'airflow':
        return None
    cred_data = cred.get('data', {})
    return AirflowAPI(
        base_url=cred_data.get('baseUrl', ''),
        username=cred_data.get('username', ''),
        password=cred_data.get('password', '')
    )

def register_airflow_routes(app):
    @app.post('/api/airflow/mark-failed')
    def mark_failed():
        data = request.get_json()
        dag_id = data.get('dagId')
        run_id = data.get('runId')
        task_id = data.get('taskId')
        log(f"Marking {f'task {task_id}' if task_id else f'DAG {dag_id}'} as FAILED for run {run_id}")
        return jsonify({'success': True, 'message': 'Marked as failed'})

    @app.post('/api/airflow/clear-task')
    def clear_task():
        data = request.get_json()
        dag_id = data.get('dagId')
        run_id = data.get('runId')
        task_id = data.get('taskId')
        log(f"Clearing task {task_id} for DAG {dag_id} (run {run_id})")
        return jsonify({'success': True, 'message': 'Task cleared'})

    @app.post('/api/airflow/natural-language')
    def airflow_natural_language():
        data = request.get_json()
        prompt = data.get('prompt', '')
        credential_id = data.get('credentialId')
        
        if not prompt:
            return jsonify({'success': False, 'error': 'No prompt provided'}), 400
        
        if not credential_id:
            return jsonify({'success': False, 'error': 'No Airflow credential provided'}), 400
        
        airflow_client = get_airflow_client(int(credential_id))
        if not airflow_client:
            return jsonify({'success': False, 'error': 'Invalid Airflow credential'}), 400
        
        operations_schema = json.dumps({k: {'description': v['description'], 'params': v.get('params', []), 'required': v.get('required', [])} for k, v in AIRFLOW_OPERATIONS.items()}, indent=2)
        
        try:
            openai = get_ai()
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": f"""You are an Airflow operations assistant. Parse natural language commands and convert them to Airflow API operations.

Available operations:
{operations_schema}

CRITICAL: Return ONLY a JSON object with:
- "operation": the operation name from the list above
- "params": an object containing the required and optional parameters for that operation
- "explanation": a brief explanation of what will be done

Examples:
User: "trigger the etl_pipeline dag"
Response: {{"operation": "trigger_dag", "params": {{"dag_id": "etl_pipeline"}}, "explanation": "Triggering a new run of the etl_pipeline DAG"}}

If the user's request cannot be mapped to an operation, return:
{{"operation": null, "error": "explanation of why", "suggestion": "what they could try instead"}}"""
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                max_completion_tokens=1024
            )
            
            raw_content = response.choices[0].message.content or "{}"
            log(f"NL Airflow Response: {raw_content}")
            parsed = json.loads(raw_content)
            
            if parsed.get('operation') is None:
                return jsonify({
                    'success': False,
                    'error': parsed.get('error', 'Could not understand the request'),
                    'suggestion': parsed.get('suggestion'),
                    'parsed': parsed
                })
            
            operation_name = parsed.get('operation')
            params = parsed.get('params', {})
            explanation = parsed.get('explanation', '')
            
            if operation_name not in AIRFLOW_OPERATIONS:
                return jsonify({
                    'success': False,
                    'error': f"Unknown operation: {operation_name}",
                    'parsed': parsed
                })
            
            op_config = AIRFLOW_OPERATIONS[operation_name]
            method_name = op_config['method']
            
            method = getattr(airflow_client, method_name, None)
            if not method:
                return jsonify({
                    'success': False,
                    'error': f"Method not implemented: {method_name}"
                })
            
            result = method(**params)
            
            return jsonify({
                'success': result.get('success', False),
                'operation': operation_name,
                'explanation': explanation,
                'params': params,
                'result': result.get('data') if result.get('success') else None,
                'error': result.get('error') if not result.get('success') else None
            })
            
        except Exception as e:
            log(f"NL Airflow Error: {e}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    @app.get('/api/airflow/operations')
    def list_airflow_operations():
        operations = []
        for key, value in AIRFLOW_OPERATIONS.items():
            operations.append({
                'name': key,
                'description': value['description'],
                'params': value.get('params', []),
                'required': value.get('required', []),
                'keywords': value.get('keywords', [])
            })
        return jsonify(operations)

    @app.post('/api/airflow/execute')
    def execute_airflow_operation():
        data = request.get_json()
        operation = data.get('operation')
        params = data.get('params', {})
        credential_id = data.get('credentialId')
        
        if not operation:
            return jsonify({'success': False, 'error': 'No operation specified'}), 400
        
        if not credential_id:
            return jsonify({'success': False, 'error': 'No Airflow credential provided'}), 400
        
        if operation not in AIRFLOW_OPERATIONS:
            return jsonify({'success': False, 'error': f'Unknown operation: {operation}'}), 400
        
        airflow_client = get_airflow_client(int(credential_id))
        if not airflow_client:
            return jsonify({'success': False, 'error': 'Invalid Airflow credential'}), 400
        
        op_config = AIRFLOW_OPERATIONS[operation]
        method_name = op_config['method']
        
        method = getattr(airflow_client, method_name, None)
        if not method:
            return jsonify({'success': False, 'error': f'Method not implemented: {method_name}'}), 500
        
        try:
            result = method(**params)
            return jsonify({
                'success': result.get('success', False),
                'operation': operation,
                'params': params,
                'result': result.get('data') if result.get('success') else None,
                'error': result.get('error') if not result.get('success') else None
            })
        except Exception as e:
            log(f"Airflow operation error: {e}")
            return jsonify({'success': False, 'error': str(e)}), 500

    @app.post('/api/airflow/batch')
    def batch_airflow_operations():
        data = request.get_json()
        operations = data.get('operations', [])
        credential_id = data.get('credentialId')
        
        if not credential_id:
            return jsonify({'success': False, 'error': 'No Airflow credential provided'}), 400
        
        airflow_client = get_airflow_client(int(credential_id))
        if not airflow_client:
            return jsonify({'success': False, 'error': 'Invalid Airflow credential'}), 400
        
        results = []
        for op in operations:
            operation = op.get('operation')
            params = op.get('params', {})
            
            if operation not in AIRFLOW_OPERATIONS:
                results.append({'success': False, 'error': f'Unknown operation: {operation}'})
                continue
            
            op_config = AIRFLOW_OPERATIONS[operation]
            method_name = op_config['method']
            method = getattr(airflow_client, method_name, None)
            
            if not method:
                results.append({'success': False, 'error': f'Method not implemented: {method_name}'})
                continue
            
            try:
                result = method(**params)
                results.append({
                    'success': result.get('success', False),
                    'operation': operation,
                    'result': result.get('data') if result.get('success') else None,
                    'error': result.get('error') if not result.get('success') else None
                })
            except Exception as e:
                results.append({'success': False, 'error': str(e)})
        
        return jsonify({
            'success': all(r.get('success') for r in results),
            'results': results
        })

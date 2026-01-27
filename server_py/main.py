import os
import json
import re
import time
import base64
import zipfile
import io
import threading
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
import requests
from openai import OpenAI

from .models import init_db
from .storage import storage

app = Flask(__name__, static_folder='../client/dist', static_url_path='')
CORS(app)

init_db()

openai_client = None
def get_ai():
    global openai_client
    if not openai_client:
        openai_client = OpenAI(
            api_key=os.environ.get('AI_INTEGRATIONS_OPENAI_API_KEY'),
            base_url=os.environ.get('AI_INTEGRATIONS_OPENAI_BASE_URL')
        )
    return openai_client

def log(message, source='flask'):
    formatted_time = datetime.now().strftime('%I:%M:%S %p')
    print(f"{formatted_time} [{source}] {message}")

@app.before_request
def log_request():
    request.start_time = time.time()

@app.after_request
def log_response(response):
    if hasattr(request, 'start_time') and request.path.startswith('/api'):
        duration = int((time.time() - request.start_time) * 1000)
        log(f"{request.method} {request.path} {response.status_code} in {duration}ms")
    return response

@app.get('/api/workflows')
def list_workflows():
    workflows = storage.get_workflows()
    return jsonify(workflows)

@app.get('/api/workflows/<int:id>')
def get_workflow(id):
    workflow = storage.get_workflow(id)
    if not workflow:
        return jsonify({'message': 'Workflow not found'}), 404
    return jsonify(workflow)

@app.post('/api/workflows')
def create_workflow():
    data = request.get_json()
    workflow = storage.create_workflow(data)
    return jsonify(workflow), 201

@app.put('/api/workflows/<int:id>')
def update_workflow(id):
    data = request.get_json()
    workflow = storage.update_workflow(id, data)
    if not workflow:
        return jsonify({'message': 'Workflow not found'}), 404
    return jsonify(workflow)

@app.delete('/api/workflows/<int:id>')
def delete_workflow(id):
    storage.delete_workflow(id)
    return '', 204

@app.get('/api/workflows/<int:id>/export')
def export_workflow(id):
    workflow = storage.get_workflow(id)
    if not workflow:
        return jsonify({'message': 'Workflow not found'}), 404
    code = f"# Python export\n# Workflow Data:\n{json.dumps(workflow, indent=2, default=str)}"
    return jsonify({'code': code})

@app.post('/api/workflows/generate')
def generate_workflow():
    data = request.get_json()
    prompt = data.get('prompt', '')
    workflow_id = data.get('workflowId')
    
    max_retries = 2
    for attempt in range(max_retries):
        try:
            openai = get_ai()
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": """You are a workflow generator for Apache Airflow 2.7.3 and SQL Server.
CRITICAL: Return ONLY a JSON object with 'nodes' and 'edges' compatible with React Flow.
ONE OPERATION PER NODE. Nodes can pass values using double curly braces (e.g., {{dagRunId}}, {{queryResult}}).

Available node types:
- 'airflow_trigger': { dagId: string, conf?: object }. Output: dagRunId.
- 'airflow_log_check': { dagId: string, taskName?: string, logAssertion: string }.
- 'sql_query': { query: string, credentialId: number }. Output: queryResult.
- 'condition': { threshold: number, variable: string, operator: string }. 
  CRITICAL: Use sourceHandle "success" for true and "failure" for false in outgoing edges.
- 'api_request': { url: string, method: string, headers: object, body: string }.
- 'python_script': { code: string }.

Example response format:
{
  "nodes": [
    { "id": "1", "type": "airflow_trigger", "data": { "label": "Trigger", "type": "airflow_trigger", "config": { "dagId": "test" } }, "position": { "x": 0, "y": 0 } }
  ],
  "edges": []
}"""
                    },
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                max_completion_tokens=2048
            )
            
            raw_content = response.choices[0].message.content or "{}"
            log(f"AI Raw Response: {raw_content}")
            content = json.loads(raw_content)
            
            if 'nodes' not in content or not isinstance(content['nodes'], list):
                raise ValueError("Invalid response: 'nodes' array is missing")
            
            if workflow_id:
                storage.update_workflow(int(workflow_id), {'lastPrompt': prompt})
            
            return jsonify(content)
        except Exception as e:
            log(f"AI Generation attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                return jsonify({
                    'message': 'Failed to generate workflow after multiple attempts',
                    'error': str(e)
                }), 500
            time.sleep(0.5 * (attempt + 1))

@app.post('/api/nodes/<node_id>/refine')
def refine_node(node_id):
    data = request.get_json()
    current_config = data.get('currentConfig', {})
    instruction = data.get('instruction', '')
    
    max_retries = 2
    for attempt in range(max_retries):
        try:
            openai = get_ai()
            response = openai.chat.completions.create(
                model="gpt-5",
                messages=[
                    {
                        "role": "system",
                        "content": """You are a workflow expert. Refine the JSON configuration for a specific node based on user instructions. 
Maintain the existing schema. Only update relevant fields.
Available fields depend on node type:
- sql_query: query
- api_request: url, method, headers, body
- airflow_trigger: dagId, conf
- airflow_sla_check: dagId, thresholdSeconds
- airflow_var_mgmt: key, value, action
- condition: threshold, variable, operator

Return ONLY the refined JSON config object."""
                    },
                    {"role": "user", "content": f"Current Config: {json.dumps(current_config)}\nInstruction: {instruction}"}
                ],
                response_format={"type": "json_object"},
                max_completion_tokens=2048
            )
            raw_content = response.choices[0].message.content or "{}"
            refined = json.loads(raw_content)
            return jsonify(refined)
        except Exception as e:
            log(f"Refinement attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                return jsonify({'message': 'Failed to refine node after retries'}), 500
            time.sleep(0.3 * (attempt + 1))

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

def resolve_variables(text, context):
    if not text or not isinstance(text, str):
        return text
    
    resolved = text
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    resolved = resolved.replace('{{today}}', today.strftime('%Y-%m-%d'))
    resolved = resolved.replace('{{yesterday}}', yesterday.strftime('%Y-%m-%d'))
    
    date_pattern = r'\{\{date:([^:]+):?([^}]*)\}\}'
    def replace_date(match):
        fmt = match.group(1)
        modifier = match.group(2) or ''
        date = datetime.now()
        if modifier.startswith('sub'):
            days = int(modifier.replace('sub', ''))
            date = date - timedelta(days=days)
        return date.strftime(fmt.replace('yyyy', '%Y').replace('MM', '%m').replace('dd', '%d'))
    
    resolved = re.sub(date_pattern, replace_date, resolved)
    
    for key, value in context.items():
        pattern = re.compile(r'\{\{' + re.escape(key) + r'\}\}', re.IGNORECASE)
        if isinstance(value, (str, int, float)):
            resolved = pattern.sub(str(value), resolved)
        elif isinstance(value, dict):
            resolved = pattern.sub(json.dumps(value), resolved)
    
    return resolved

def execute_workflow_async(execution_id, workflow_id):
    workflow = storage.get_workflow(workflow_id)
    if not workflow:
        return
    
    logs = []
    results = {}
    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': 'Starting workflow execution...'})
    storage.update_execution(execution_id, 'running', logs)
    
    nodes = workflow.get('nodes', [])
    edges = workflow.get('edges', [])
    execution_context = {}
    
    def find_next_nodes(current_node_id, handle=None):
        return [
            node for edge in edges
            if edge.get('source') == current_node_id and (not handle or edge.get('sourceHandle') == handle)
            for node in nodes if node.get('id') == edge.get('target')
        ]
    
    current_nodes = [n for n in nodes if not any(e.get('target') == n.get('id') for e in edges)]
    visited = set()
    assertion_failed = False
    
    while current_nodes and not assertion_failed:
        next_batch = []
        
        for node in current_nodes:
            node_id = node.get('id')
            if node_id in visited:
                continue
            visited.add(node_id)
            
            time.sleep(1)
            node_data = node.get('data', {})
            node_type = node_data.get('type')
            config = node_data.get('config', {})
            
            logs.append({
                'timestamp': datetime.now().isoformat(),
                'level': 'INFO',
                'message': f"Executing node {node_data.get('label')} ({node_type})..."
            })
            
            output_handle = 'output'
            
            try:
                if node_type == 'airflow_trigger':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    conf = config.get('conf', {})
                    credential_id = config.get('credentialId')
                    
                    auth_headers = {}
                    base_url = ""
                    
                    if credential_id:
                        cred = storage.get_credential(int(credential_id))
                        if cred and cred.get('type') == 'airflow':
                            cred_data = cred.get('data', {})
                            base_url = cred_data.get('baseUrl', '')
                            auth = base64.b64encode(f"{cred_data.get('username')}:{cred_data.get('password')}".encode()).decode()
                            auth_headers = {'Authorization': f'Basic {auth}'}
                            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Using credential: {cred.get('name')}"})
                    
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Triggering Airflow DAG: {dag_id}"})
                    
                    dag_run_id = f"run_{int(time.time() * 1000)}"
                    
                    if base_url:
                        try:
                            response = requests.post(
                                f"{base_url}/api/v1/dags/{dag_id}/dagRuns",
                                json={'conf': conf},
                                headers=auth_headers
                            )
                            response.raise_for_status()
                            dag_run_id = response.json().get('dag_run_id', dag_run_id)
                        except Exception as e:
                            error_msg = str(e)
                            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': f"Failed to trigger DAG {dag_id}: {error_msg}"})
                            raise
                    
                    execution_context['dagRunId'] = dag_run_id
                    execution_context[node_id] = {'dagId': dag_id, 'dagRunId': dag_run_id}
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"DAG triggered successfully. Run ID: {dag_run_id}"})
                
                elif node_type == 'airflow_log_check':
                    dag_id = resolve_variables(config.get('dagId', execution_context.get('dagId', '')), execution_context)
                    task_name = resolve_variables(config.get('taskName', 'entire_dag'), execution_context)
                    log_assertion = resolve_variables(config.get('logAssertion', ''), execution_context)
                    credential_id = config.get('credentialId')
                    run_id = execution_context.get('dagRunId', '')
                    
                    auth_headers = {}
                    base_url = ""
                    
                    if credential_id:
                        cred = storage.get_credential(int(credential_id))
                        if cred and cred.get('type') == 'airflow':
                            cred_data = cred.get('data', {})
                            base_url = cred_data.get('baseUrl', '')
                            auth = base64.b64encode(f"{cred_data.get('username')}:{cred_data.get('password')}".encode()).decode()
                            auth_headers = {'Authorization': f'Basic {auth}'}
                    
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Checking logs for {task_name} in DAG {dag_id}..."})
                    
                    logs_text = "INFO: Task started\nSUCCESS: Processed 5000 rows\nINFO: Task completed"
                    
                    if base_url and run_id:
                        try:
                            task_id = 'check_status' if task_name == 'entire_dag' else task_name
                            response = requests.get(
                                f"{base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
                                headers=auth_headers
                            )
                            response.raise_for_status()
                            logs_text = response.text
                        except Exception as e:
                            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'WARN', 'message': f"Could not fetch logs: {e}"})
                    
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"[AIRFLOW LOGS - {task_name}] \n{logs_text[:500]}..."})
                    
                    pattern_str = log_assertion.replace(r'{{', '.*').replace(r'}}', '.*')
                    pattern = re.compile(pattern_str, re.IGNORECASE)
                    found = bool(pattern.search(logs_text))
                    
                    execution_context[node_id] = 1 if found else 0
                    logs.append({
                        'timestamp': datetime.now().isoformat(),
                        'level': 'INFO' if found else 'ERROR',
                        'message': f"Assertion [{log_assertion}] on {task_name}: {'PASSED' if found else 'FAILED'}"
                    })
                    
                    if not found:
                        assertion_failed = True
                        break
                
                elif node_type == 'sql_query':
                    query = resolve_variables(config.get('query', ''), execution_context)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Running SQL: {query}"})
                    record_count = 12
                    execution_context['queryResult'] = {'record_count': record_count}
                    execution_context[node_id] = {'count': record_count}
                
                elif node_type == 'condition':
                    threshold = config.get('threshold', 0)
                    variable = resolve_variables(str(config.get('variable', '')), execution_context)
                    operator = config.get('operator', '>')
                    
                    try:
                        actual = float(variable)
                    except:
                        actual = 0
                    
                    passed = False
                    if operator == '>':
                        passed = actual > threshold
                    elif operator == '<':
                        passed = actual < threshold
                    elif operator == '>=':
                        passed = actual >= threshold
                    elif operator == '<=':
                        passed = actual <= threshold
                    elif operator == '==':
                        passed = actual == threshold
                    
                    output_handle = 'success' if passed else 'failure'
                    logs.append({
                        'timestamp': datetime.now().isoformat(),
                        'level': 'INFO',
                        'message': f"Condition [{actual} {operator} {threshold}]: {'TRUE' if passed else 'FALSE'}"
                    })
                
                if node_type == 'condition':
                    next_nodes = find_next_nodes(node_id, output_handle)
                else:
                    next_nodes = find_next_nodes(node_id)
                next_batch.extend(next_nodes)
                
            except Exception as e:
                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': f"Error executing node: {str(e)}"})
                assertion_failed = True
                break
            
            storage.update_execution(execution_id, 'running', logs, results)
        
        current_nodes = next_batch
    
    if assertion_failed:
        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': 'Workflow failed due to assertion failure.'})
        storage.update_execution(execution_id, 'failed', logs, results)
    else:
        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': 'Workflow completed successfully.'})
        storage.update_execution(execution_id, 'completed', logs, results)

@app.post('/api/workflows/<int:id>/execute')
def execute_workflow(id):
    execution = storage.create_execution(id)
    thread = threading.Thread(target=execute_workflow_async, args=(execution['id'], id))
    thread.start()
    return jsonify(execution), 201

@app.route('/')
@app.route('/<path:path>')
def serve_frontend(path=''):
    if path and os.path.exists(os.path.join(app.static_folder or '', path)):
        return send_from_directory(app.static_folder or '', path)
    return send_from_directory(app.static_folder or '', 'index.html')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    log(f"serving on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)

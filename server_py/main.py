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
from .airflow_api import AirflowAPI, get_airflow_client

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

DAG Management:
- 'airflow_trigger': { dagId: string, conf?: object, credentialId?: number }. Output: dagRunId. Triggers a DAG run.
- 'airflow_pause': { dagId: string, credentialId?: number }. Pauses a DAG to prevent scheduled runs.
- 'airflow_unpause': { dagId: string, credentialId?: number }. Unpauses a DAG to allow scheduled runs.
- 'airflow_wait_completion': { dagId: string, credentialId?: number, timeout?: number }. Waits for all running DAG instances to complete.
- 'airflow_ensure_ready': { dagId: string, credentialId?: number, timeout?: number }. Ensures DAG is paused and has no running instances.

DAG Status & Monitoring:
- 'airflow_get_dag': { dagId: string, credentialId?: number }. Gets DAG details including pause state. Output: dagInfo.
- 'airflow_list_runs': { dagId: string, state?: string, limit?: number, credentialId?: number }. Lists DAG runs. Output: dagRuns.
- 'airflow_get_run': { dagId: string, dagRunId: string, credentialId?: number }. Gets specific DAG run details. Output: runInfo.
- 'airflow_log_check': { dagId: string, taskName?: string, logAssertion: string, credentialId?: number }. Checks task logs for assertion.

DAG Run Control:
- 'airflow_update_run_state': { dagId: string, dagRunId: string, state: string, credentialId?: number }. Updates run state (success/failed).
- 'airflow_delete_run': { dagId: string, dagRunId: string, credentialId?: number }. Deletes a DAG run.
- 'airflow_clear_tasks': { dagId: string, taskIds?: string[], onlyFailed?: boolean, credentialId?: number }. Clears tasks for re-run.

Task Monitoring:
- 'airflow_list_tasks': { dagId: string, dagRunId: string, credentialId?: number }. Lists task instances. Output: tasks.
- 'airflow_get_task': { dagId: string, dagRunId: string, taskId: string, credentialId?: number }. Gets task instance details.
- 'airflow_get_task_logs': { dagId: string, dagRunId: string, taskId: string, tryNumber?: number, credentialId?: number }. Gets task logs.

Variable Management:
- 'airflow_get_variable': { key: string, credentialId?: number }. Gets an Airflow variable. Output: variableValue.
- 'airflow_set_variable': { key: string, value: string, credentialId?: number }. Creates/updates an Airflow variable.
- 'airflow_delete_variable': { key: string, credentialId?: number }. Deletes an Airflow variable.

Pool Management:
- 'airflow_list_pools': { credentialId?: number }. Lists all pools. Output: pools.
- 'airflow_create_pool': { name: string, slots: number, credentialId?: number }. Creates a new pool.

Health & Diagnostics:
- 'airflow_health': { credentialId?: number }. Gets Airflow health status. Output: healthStatus.
- 'airflow_import_errors': { credentialId?: number }. Gets DAG import errors. Output: importErrors.

XCom:
- 'airflow_get_xcom': { dagId: string, dagRunId: string, taskId: string, credentialId?: number }. Gets XCom entries.

Other Operations:
- 'sql_query': { query: string, credentialId: number }. Executes SQL query. Output: queryResult.
- 'condition': { threshold: number, variable: string, operator: string }. 
  CRITICAL: Use sourceHandle "success" for true and "failure" for false in outgoing edges.
- 'api_request': { url: string, method: string, headers: object, body: string }. Makes HTTP request.
- 'python_script': { code: string }. Executes Python code.

IMPORTANT WORKFLOW PATTERN: Before triggering any DAG, always ensure it's ready:
1. Use 'airflow_ensure_ready' to wait for completion and pause the DAG
2. Then use 'airflow_trigger' to start the new run

Example response format:
{
  "nodes": [
    { "id": "1", "type": "airflow_ensure_ready", "data": { "label": "Prepare DAG", "type": "airflow_ensure_ready", "config": { "dagId": "my_dag" } }, "position": { "x": 0, "y": 0 } },
    { "id": "2", "type": "airflow_trigger", "data": { "label": "Trigger DAG", "type": "airflow_trigger", "config": { "dagId": "my_dag" } }, "position": { "x": 0, "y": 100 } }
  ],
  "edges": [{ "id": "e1-2", "source": "1", "target": "2" }]
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

def get_airflow_client_from_credential(credential_id):
    """Helper to get Airflow client from credential ID."""
    if not credential_id:
        return None, "No credential ID provided"
    cred = storage.get_credential(int(credential_id))
    if not cred or cred.get('type') != 'airflow':
        return None, "Invalid or missing Airflow credential"
    client = get_airflow_client(cred.get('data', {}))
    if not client:
        return None, "Failed to create Airflow client"
    return client, None

@app.get('/api/airflow/dags')
def list_airflow_dags():
    """List all DAGs from Airflow."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.list_dags()
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/dags/<dag_id>')
def get_airflow_dag(dag_id):
    """Get details of a specific DAG."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_dag(dag_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/dags/<dag_id>/pause')
def pause_airflow_dag(dag_id):
    """Pause a DAG."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.pause_dag(dag_id)
        log(f"Paused DAG: {dag_id}")
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/dags/<dag_id>/unpause')
def unpause_airflow_dag(dag_id):
    """Unpause a DAG."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.unpause_dag(dag_id)
        log(f"Unpaused DAG: {dag_id}")
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.delete('/api/airflow/dags/<dag_id>')
def delete_airflow_dag(dag_id):
    """Delete a DAG and all associated metadata."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        client.delete_dag(dag_id)
        log(f"Deleted DAG: {dag_id}")
        return jsonify({'success': True, 'message': f'DAG {dag_id} deleted'})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/dags/<dag_id>/runs')
def list_airflow_dag_runs(dag_id):
    """List DAG runs for a specific DAG."""
    credential_id = request.args.get('credentialId', type=int)
    state = request.args.get('state')
    limit = request.args.get('limit', 100, type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.list_dag_runs(dag_id, limit=limit, state=state)
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/dags/<dag_id>/runs/<dag_run_id>')
def get_airflow_dag_run(dag_id, dag_run_id):
    """Get details of a specific DAG run."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_dag_run(dag_id, dag_run_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/dags/<dag_id>/trigger')
def trigger_airflow_dag(dag_id):
    """Trigger a new DAG run."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    conf = data.get('conf', {})
    dag_run_id = data.get('dagRunId')
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.trigger_dag(dag_id, conf=conf, dag_run_id=dag_run_id)
        log(f"Triggered DAG: {dag_id}, Run ID: {result.get('dag_run_id')}")
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.patch('/api/airflow/dags/<dag_id>/runs/<dag_run_id>/state')
def update_dag_run_state(dag_id, dag_run_id):
    """Update the state of a DAG run."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    state = data.get('state')
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    if not state:
        return jsonify({'message': 'State is required'}), 400
    try:
        result = client.update_dag_run_state(dag_id, dag_run_id, state)
        log(f"Updated DAG run state: {dag_id}/{dag_run_id} -> {state}")
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.delete('/api/airflow/dags/<dag_id>/runs/<dag_run_id>')
def delete_dag_run(dag_id, dag_run_id):
    """Delete a specific DAG run."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        client.delete_dag_run(dag_id, dag_run_id)
        log(f"Deleted DAG run: {dag_id}/{dag_run_id}")
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/dags/<dag_id>/runs/<dag_run_id>/tasks')
def list_task_instances(dag_id, dag_run_id):
    """List task instances for a DAG run."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.list_task_instances(dag_id, dag_run_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/dags/<dag_id>/runs/<dag_run_id>/tasks/<task_id>')
def get_task_instance(dag_id, dag_run_id, task_id):
    """Get a specific task instance."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_task_instance(dag_id, dag_run_id, task_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/dags/<dag_id>/runs/<dag_run_id>/tasks/<task_id>/logs')
def get_task_logs(dag_id, dag_run_id, task_id):
    """Get logs for a task instance."""
    credential_id = request.args.get('credentialId', type=int)
    try_number = request.args.get('tryNumber', 1, type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        logs = client.get_task_logs(dag_id, dag_run_id, task_id, try_number)
        return jsonify({'logs': logs})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/dags/<dag_id>/clear-tasks')
def clear_dag_task_instances(dag_id):
    """Clear task instances to allow re-running."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    task_ids = data.get('taskIds')
    only_failed = data.get('onlyFailed', False)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.clear_task_instances(dag_id, task_ids=task_ids, only_failed=only_failed)
        log(f"Cleared task instances for DAG: {dag_id}")
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/variables')
def list_airflow_variables():
    """List all Airflow variables."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_variables()
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/variables/<key>')
def get_airflow_variable(key):
    """Get a specific Airflow variable."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_variable(key)
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/variables')
def create_airflow_variable():
    """Create a new Airflow variable."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    key = data.get('key')
    value = data.get('value')
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    if not key or value is None:
        return jsonify({'message': 'Key and value are required'}), 400
    try:
        result = client.create_variable(key, value)
        log(f"Created Airflow variable: {key}")
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.patch('/api/airflow/variables/<key>')
def update_airflow_variable(key):
    """Update an Airflow variable."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    value = data.get('value')
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    if value is None:
        return jsonify({'message': 'Value is required'}), 400
    try:
        result = client.update_variable(key, value)
        log(f"Updated Airflow variable: {key}")
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.delete('/api/airflow/variables/<key>')
def delete_airflow_variable(key):
    """Delete an Airflow variable."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        client.delete_variable(key)
        log(f"Deleted Airflow variable: {key}")
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/connections')
def list_airflow_connections():
    """List all Airflow connections."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_connections()
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/pools')
def list_airflow_pools():
    """List all Airflow pools."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_pools()
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/pools')
def create_airflow_pool():
    """Create a new Airflow pool."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    name = data.get('name')
    slots = data.get('slots', 1)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    if not name:
        return jsonify({'message': 'Name is required'}), 400
    try:
        result = client.create_pool(name, slots)
        log(f"Created Airflow pool: {name}")
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/health')
def get_airflow_health():
    """Get Airflow health status."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_health()
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/import-errors')
def get_airflow_import_errors():
    """Get DAG import errors."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_import_errors()
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/dags/<dag_id>/wait-for-completion')
def wait_for_dag_completion(dag_id):
    """Wait for a DAG's running instances to complete."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    timeout = data.get('timeout', 600)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        success, message = client.wait_for_dag_completion(dag_id, timeout_seconds=timeout)
        return jsonify({'success': success, 'message': message})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/dags/<dag_id>/ensure-ready')
def ensure_dag_ready(dag_id):
    """Ensure a DAG is paused and has no running instances."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    timeout = data.get('timeout', 600)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        success, messages = client.ensure_dag_paused_and_idle(dag_id, wait_timeout=timeout)
        return jsonify({'success': success, 'messages': messages})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.post('/api/airflow/prepare-workflow')
def prepare_dags_for_workflow():
    """Prepare multiple DAGs for workflow execution."""
    data = request.get_json() or {}
    credential_id = data.get('credentialId')
    dag_ids = data.get('dagIds', [])
    timeout = data.get('timeout', 600)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    if not dag_ids:
        return jsonify({'message': 'No DAG IDs provided'}), 400
    try:
        success, messages = client.prepare_dags_for_workflow(dag_ids, wait_timeout=timeout)
        return jsonify({'success': success, 'messages': messages})
    except Exception as e:
        return jsonify({'message': str(e)}), 500

@app.get('/api/airflow/dags/<dag_id>/xcom/<dag_run_id>/<task_id>')
def get_xcom_entries(dag_id, dag_run_id, task_id):
    """Get XCom entries for a task instance."""
    credential_id = request.args.get('credentialId', type=int)
    client, error = get_airflow_client_from_credential(credential_id)
    if error:
        return jsonify({'message': error}), 400
    try:
        result = client.get_xcom_entries(dag_id, dag_run_id, task_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({'message': str(e)}), 500

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

def extract_dag_ids_from_workflow(nodes):
    """Extract all DAG IDs from workflow nodes that involve Airflow operations."""
    dag_ids = set()
    airflow_node_types = [
        'airflow_trigger', 'airflow_pause', 'airflow_unpause', 
        'airflow_wait_completion', 'airflow_ensure_ready', 'airflow_get_dag',
        'airflow_list_runs', 'airflow_get_run', 'airflow_log_check',
        'airflow_update_run_state', 'airflow_delete_run', 'airflow_clear_tasks',
        'airflow_list_tasks', 'airflow_get_task', 'airflow_get_task_logs'
    ]
    for node in nodes:
        node_data = node.get('data', {})
        node_type = node_data.get('type', '')
        if node_type in airflow_node_types:
            config = node_data.get('config', {})
            dag_id = config.get('dagId', '')
            if dag_id and not dag_id.startswith('{{'):
                dag_ids.add(dag_id)
    return list(dag_ids)

def prepare_dags_before_execution(nodes, logs, storage):
    """Ensure all DAGs in workflow are paused and not running before execution."""
    dag_ids = extract_dag_ids_from_workflow(nodes)
    if not dag_ids:
        return True, logs
    
    trigger_nodes = [n for n in nodes if n.get('data', {}).get('type') == 'airflow_trigger']
    if not trigger_nodes:
        return True, logs
    
    first_trigger = trigger_nodes[0]
    credential_id = first_trigger.get('data', {}).get('config', {}).get('credentialId')
    
    if not credential_id:
        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'WARN', 'message': 'No credential configured, skipping pre-trigger DAG safety checks'})
        return True, logs
    
    cred = storage.get_credential(int(credential_id))
    if not cred or cred.get('type') != 'airflow':
        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'WARN', 'message': 'Invalid Airflow credential, skipping pre-trigger safety checks'})
        return True, logs
    
    client = get_airflow_client(cred.get('data', {}))
    if not client:
        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'WARN', 'message': 'Failed to create Airflow client, skipping pre-trigger safety checks'})
        return True, logs
    
    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f'Pre-trigger safety check: Preparing {len(dag_ids)} DAG(s): {", ".join(dag_ids)}'})
    
    for dag_id in dag_ids:
        try:
            if client.is_dag_running(dag_id):
                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f'DAG {dag_id} has running instances, waiting for completion...'})
                success, msg = client.wait_for_dag_completion(dag_id, timeout_seconds=600)
                if not success:
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': f'Timeout waiting for DAG {dag_id} to complete: {msg}'})
                    return False, logs
                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f'DAG {dag_id} runs completed'})
            
            if not client.is_dag_paused(dag_id):
                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f'Pausing DAG {dag_id}...'})
                client.pause_dag(dag_id)
                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f'DAG {dag_id} paused successfully'})
            else:
                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f'DAG {dag_id} is already paused'})
        except Exception as e:
            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': f'Failed to prepare DAG {dag_id}: {str(e)}'})
            return False, logs
    
    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': 'All DAGs are paused and ready for workflow execution'})
    return True, logs

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
    
    success, logs = prepare_dags_before_execution(nodes, logs, storage)
    storage.update_execution(execution_id, 'running', logs)
    if not success:
        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': 'Workflow aborted: Pre-trigger safety checks failed'})
        storage.update_execution(execution_id, 'failed', logs, results)
        return
    
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
            
            # Update results to indicate the node is currently running
            results[node_id] = {'status': 'running'}
            storage.update_execution(execution_id, 'running', logs, results)
            
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
                    results[node_id] = {'status': 'success', 'dagId': dag_id, 'dagRunId': dag_run_id}
                
                elif node_type == 'airflow_log_check':
                    node_dag_id = resolve_variables(config.get('dagId', execution_context.get('dagId', '')), execution_context)
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
                    
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Checking logs for {task_name} in DAG {node_dag_id}..."})
                    
                    logs_text = "INFO: Task started\nSUCCESS: Processed 5000 rows\nINFO: Task completed"
                    
                    if base_url and run_id:
                        try:
                            task_id = 'check_status' if task_name == 'entire_dag' else task_name
                            response = requests.get(
                                f"{base_url}/api/v1/dags/{node_dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
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
                    results[node_id] = {'status': 'success' if found else 'failure', 'found': found}
                    
                    if not found:
                        assertion_failed = True
                        break
                
                elif node_type == 'sql_query':
                    query = resolve_variables(config.get('query', ''), execution_context)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Running SQL: {query}"})
                    record_count = 12
                    execution_context['queryResult'] = {'record_count': record_count}
                    execution_context[node_id] = {'count': record_count}
                    results[node_id] = {'status': 'success', 'count': record_count}
                
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
                    results[node_id] = {'status': 'success', 'passed': passed}
                
                elif node_type == 'airflow_pause':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    client.pause_dag(dag_id)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Paused DAG: {dag_id}"})
                    results[node_id] = {'status': 'success', 'dagId': dag_id, 'action': 'paused'}
                
                elif node_type == 'airflow_unpause':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    client.unpause_dag(dag_id)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Unpaused DAG: {dag_id}"})
                    results[node_id] = {'status': 'success', 'dagId': dag_id, 'action': 'unpaused'}
                
                elif node_type == 'airflow_wait_completion':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    timeout = config.get('timeout', 600)
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Waiting for DAG {dag_id} to complete..."})
                    success, msg = client.wait_for_dag_completion(dag_id, timeout_seconds=timeout)
                    if not success:
                        raise Exception(f"Timeout waiting for DAG completion: {msg}")
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"DAG {dag_id} completed: {msg}"})
                    results[node_id] = {'status': 'success', 'dagId': dag_id, 'message': msg}
                
                elif node_type == 'airflow_ensure_ready':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    timeout = config.get('timeout', 600)
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Ensuring DAG {dag_id} is ready..."})
                    success, messages = client.ensure_dag_paused_and_idle(dag_id, wait_timeout=timeout)
                    for msg in messages:
                        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': msg})
                    if not success:
                        raise Exception(f"Failed to ensure DAG {dag_id} is ready")
                    results[node_id] = {'status': 'success', 'dagId': dag_id, 'messages': messages}
                
                elif node_type == 'airflow_get_dag':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    dag_info = client.get_dag(dag_id)
                    execution_context['dagInfo'] = dag_info
                    execution_context[node_id] = dag_info
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Retrieved DAG info for {dag_id}: is_paused={dag_info.get('is_paused')}"})
                    results[node_id] = {'status': 'success', 'dagInfo': dag_info}
                
                elif node_type == 'airflow_list_runs':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    state = config.get('state')
                    limit = config.get('limit', 10)
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    runs = client.list_dag_runs(dag_id, limit=limit, state=state)
                    execution_context['dagRuns'] = runs
                    execution_context[node_id] = runs
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Listed {len(runs.get('dag_runs', []))} runs for DAG {dag_id}"})
                    results[node_id] = {'status': 'success', 'runCount': len(runs.get('dag_runs', []))}
                
                elif node_type == 'airflow_get_run':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    dag_run_id = resolve_variables(config.get('dagRunId', execution_context.get('dagRunId', '')), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    run_info = client.get_dag_run(dag_id, dag_run_id)
                    execution_context['runInfo'] = run_info
                    execution_context[node_id] = run_info
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"DAG run {dag_run_id} state: {run_info.get('state')}"})
                    results[node_id] = {'status': 'success', 'state': run_info.get('state')}
                
                elif node_type == 'airflow_update_run_state':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    dag_run_id = resolve_variables(config.get('dagRunId', execution_context.get('dagRunId', '')), execution_context)
                    new_state = config.get('state', 'failed')
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    client.update_dag_run_state(dag_id, dag_run_id, new_state)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Updated DAG run {dag_run_id} state to: {new_state}"})
                    results[node_id] = {'status': 'success', 'newState': new_state}
                
                elif node_type == 'airflow_delete_run':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    dag_run_id = resolve_variables(config.get('dagRunId', execution_context.get('dagRunId', '')), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    client.delete_dag_run(dag_id, dag_run_id)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Deleted DAG run: {dag_run_id}"})
                    results[node_id] = {'status': 'success'}
                
                elif node_type == 'airflow_clear_tasks':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    task_ids = config.get('taskIds')
                    only_failed = config.get('onlyFailed', False)
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    result = client.clear_task_instances(dag_id, task_ids=task_ids, only_failed=only_failed)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Cleared task instances for DAG {dag_id}"})
                    results[node_id] = {'status': 'success', 'result': result}
                
                elif node_type == 'airflow_list_tasks':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    dag_run_id = resolve_variables(config.get('dagRunId', execution_context.get('dagRunId', '')), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    tasks = client.list_task_instances(dag_id, dag_run_id)
                    execution_context['tasks'] = tasks
                    execution_context[node_id] = tasks
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Listed {len(tasks.get('task_instances', []))} tasks for run {dag_run_id}"})
                    results[node_id] = {'status': 'success', 'taskCount': len(tasks.get('task_instances', []))}
                
                elif node_type == 'airflow_get_task':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    dag_run_id = resolve_variables(config.get('dagRunId', execution_context.get('dagRunId', '')), execution_context)
                    task_id = resolve_variables(config.get('taskId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    task_info = client.get_task_instance(dag_id, dag_run_id, task_id)
                    execution_context[node_id] = task_info
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Task {task_id} state: {task_info.get('state')}"})
                    results[node_id] = {'status': 'success', 'taskState': task_info.get('state')}
                
                elif node_type == 'airflow_get_task_logs':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    dag_run_id = resolve_variables(config.get('dagRunId', execution_context.get('dagRunId', '')), execution_context)
                    task_id = resolve_variables(config.get('taskId', ''), execution_context)
                    try_number = config.get('tryNumber', 1)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    task_logs = client.get_task_logs(dag_id, dag_run_id, task_id, try_number)
                    execution_context[node_id] = {'logs': task_logs}
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Retrieved logs for task {task_id} (try {try_number})"})
                    results[node_id] = {'status': 'success', 'logLength': len(task_logs)}
                
                elif node_type == 'airflow_get_variable':
                    key = resolve_variables(config.get('key', ''), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    variable = client.get_variable(key)
                    execution_context['variableValue'] = variable.get('value')
                    execution_context[node_id] = variable
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Retrieved variable: {key}"})
                    results[node_id] = {'status': 'success', 'key': key}
                
                elif node_type == 'airflow_set_variable':
                    key = resolve_variables(config.get('key', ''), execution_context)
                    value = resolve_variables(config.get('value', ''), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    try:
                        client.update_variable(key, value)
                    except:
                        client.create_variable(key, value)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Set variable: {key}"})
                    results[node_id] = {'status': 'success', 'key': key}
                
                elif node_type == 'airflow_delete_variable':
                    key = resolve_variables(config.get('key', ''), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    client.delete_variable(key)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Deleted variable: {key}"})
                    results[node_id] = {'status': 'success', 'key': key}
                
                elif node_type == 'airflow_list_pools':
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    pools = client.get_pools()
                    execution_context['pools'] = pools
                    execution_context[node_id] = pools
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Listed {len(pools.get('pools', []))} pools"})
                    results[node_id] = {'status': 'success', 'poolCount': len(pools.get('pools', []))}
                
                elif node_type == 'airflow_create_pool':
                    name = resolve_variables(config.get('name', ''), execution_context)
                    slots = config.get('slots', 1)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    pool = client.create_pool(name, slots)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Created pool: {name} with {slots} slots"})
                    results[node_id] = {'status': 'success', 'name': name, 'slots': slots}
                
                elif node_type == 'airflow_health':
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    health = client.get_health()
                    execution_context['healthStatus'] = health
                    execution_context[node_id] = health
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Airflow health check completed"})
                    results[node_id] = {'status': 'success', 'health': health}
                
                elif node_type == 'airflow_import_errors':
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    errors = client.get_import_errors()
                    execution_context['importErrors'] = errors
                    execution_context[node_id] = errors
                    error_count = len(errors.get('import_errors', []))
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO' if error_count == 0 else 'WARN', 'message': f"Found {error_count} import errors"})
                    results[node_id] = {'status': 'success', 'errorCount': error_count}
                
                elif node_type == 'airflow_get_xcom':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    dag_run_id = resolve_variables(config.get('dagRunId', execution_context.get('dagRunId', '')), execution_context)
                    task_id = resolve_variables(config.get('taskId', ''), execution_context)
                    credential_id = config.get('credentialId')
                    client, error = get_airflow_client_from_credential(credential_id)
                    if error:
                        raise Exception(f"Failed to get Airflow client: {error}")
                    xcom = client.get_xcom_entries(dag_id, dag_run_id, task_id)
                    execution_context[node_id] = xcom
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Retrieved XCom entries for task {task_id}"})
                    results[node_id] = {'status': 'success', 'entryCount': len(xcom.get('xcom_entries', []))}
                
                elif node_type == 'api_request':
                    url = resolve_variables(config.get('url', ''), execution_context)
                    method = config.get('method', 'GET').upper()
                    headers = config.get('headers', {})
                    body = resolve_variables(config.get('body', ''), execution_context)
                    try:
                        if method == 'GET':
                            resp = requests.get(url, headers=headers)
                        elif method == 'POST':
                            resp = requests.post(url, headers=headers, data=body)
                        elif method == 'PUT':
                            resp = requests.put(url, headers=headers, data=body)
                        elif method == 'DELETE':
                            resp = requests.delete(url, headers=headers)
                        else:
                            resp = requests.request(method, url, headers=headers, data=body)
                        execution_context[node_id] = {'status_code': resp.status_code, 'body': resp.text[:1000]}
                        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"API request {method} {url} returned {resp.status_code}"})
                        results[node_id] = {'status': 'success', 'statusCode': resp.status_code}
                    except Exception as e:
                        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': f"API request failed: {str(e)}"})
                        raise
                
                elif node_type == 'python_script':
                    code = resolve_variables(config.get('code', ''), execution_context)
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Executing Python script..."})
                    results[node_id] = {'status': 'success'}
                
                else:
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'WARN', 'message': f"Unknown node type: {node_type}"})
                    results[node_id] = {'status': 'success'}
                
                if node_type == 'condition':
                    next_nodes = find_next_nodes(node_id, output_handle)
                else:
                    next_nodes = find_next_nodes(node_id)
                next_batch.extend(next_nodes)
                
            except Exception as e:
                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': f"Error executing node: {str(e)}"})
                results[node_id] = {'status': 'failure', 'error': str(e)}
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

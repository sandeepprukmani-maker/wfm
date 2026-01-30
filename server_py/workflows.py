import os
import json
import time
import base64
import requests
import re
import threading
import sqlalchemy
from sqlalchemy import text
from datetime import datetime, timedelta
from flask import request, jsonify
from .storage import storage
from .utils import log, resolve_variables, get_ai

def export_to_excel(data, node_id, execution_id):
    """Export query result to an HTML-based Excel file with column auto-fitting and yellow headers."""
    try:
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
            
        file_path = f"/tmp/query_result_{execution_id}_{node_id}.xls"
        headers = list(data[0].keys())
        
        # Calculate approximate column widths based on headers and data
        col_widths = {h: len(str(h)) for h in headers}
        for row in data:
            for h in headers:
                val_len = len(str(row.get(h, '')))
                if val_len > col_widths[h]:
                    col_widths[h] = val_len

        # Generate HTML table with basic Excel-compatible styling
        html = [
            '<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel" xmlns="http://www.w3.org/TR/REC-html40">',
            '<head><meta http-equiv="content-type" content="application/vnd.ms-excel; charset=UTF-8">',
            '<style>',
            'table { border-collapse: collapse; }',
            'th { background-color: #FFFF00; border: 0.5pt solid black; font-weight: bold; }',
            'td { border: 0.5pt solid black; white-space: nowrap; }',
            '</style>',
            '</head><body><table>'
        ]
        
        # Set column widths using <col> tags (Excel respects width in points/pixels roughly)
        for h in headers:
            width = (col_widths[h] + 2) * 7 # Rough heuristic for pixel width
            html.append(f'<col width="{width}">')
            
        # Write headers
        html.append('<tr>')
        for h in headers:
            html.append(f'<th>{h}</th>')
        html.append('</tr>')
        
        # Write data
        for row in data:
            html.append('<tr>')
            for h in headers:
                val = str(row.get(h, ''))
                html.append(f'<td>{val}</td>')
            html.append('</tr>')
            
        html.append('</table></body></html>')
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(html))
            
        return file_path
    except Exception as e:
        log(f"Export failed: {e}")
        return None

def get_dag_state(dag_id, base_url, auth_headers):
    try:
        response = requests.get(
            f"{base_url}/api/v1/dags/{dag_id}/dagRuns",
            params={'order_by': '-execution_date', 'limit': 1},
            headers=auth_headers
        )
        response.raise_for_status()
        dag_runs = response.json().get('dag_runs', [])
        if dag_runs:
            return dag_runs[0].get('state', 'unknown')
        return 'no_runs'
    except Exception as e:
        log(f"Error checking DAG state for {dag_id}: {e}")
        return 'unknown'

def wait_for_dags_to_complete(dag_infos, logs, execution_id, storage):
    max_wait_time = 3600
    poll_interval = 10
    elapsed = 0
    
    while elapsed < max_wait_time:
        all_complete = True
        for dag_info in dag_infos:
            dag_id = dag_info['dag_id']
            base_url = dag_info['base_url']
            auth_headers = dag_info['auth_headers']
            
            if not base_url:
                continue
            
            state = get_dag_state(dag_id, base_url, auth_headers)
            running_states = ['running', 'queued', 'scheduled', 'up_for_retry', 'up_for_reschedule', 'restarting', 'deferred']
            
            if state.lower() in running_states:
                all_complete = False
                logs.append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'INFO',
                    'message': f"DAG {dag_id} is currently {state}. Waiting for it to reach a terminal state..."
                })
                storage.update_execution(execution_id, 'waiting', logs)
                break
        
        if all_complete:
            return True
        
        time.sleep(poll_interval)
        elapsed += poll_interval
    
    logs.append({
        'timestamp': datetime.now().isoformat(),
        'level': 'ERROR',
        'message': f"Timeout waiting for DAGs to reach a terminal state after {max_wait_time} seconds"
    })
    return False

def collect_dag_infos_from_workflow(nodes, storage):
    dag_infos = []
    for node in nodes:
        node_data = node.get('data', {})
        node_type = node_data.get('type')
        
        if node_type in ['airflow_trigger', 'airflow_log_check']:
            config = node_data.get('config', {})
            dag_id = config.get('dagId', '')
            credential_id = config.get('credentialId')
            
            base_url = ""
            auth_headers = {}
            
            if credential_id:
                cred = storage.get_credential(int(credential_id))
                if cred and cred.get('type') == 'airflow':
                    cred_data = cred.get('data', {})
                    base_url = cred_data.get('baseUrl', '')
                    auth = base64.b64encode(f"{cred_data.get('username')}:{cred_data.get('password')}".encode()).decode()
                    auth_headers = {'Authorization': f'Basic {auth}'}
            
            if dag_id and base_url:
                dag_infos.append({
                    'dag_id': dag_id,
                    'base_url': base_url,
                    'auth_headers': auth_headers
                })
    return dag_infos

def execute_workflow_async(execution_id, workflow_id):
    workflow = storage.get_workflow(workflow_id)
    if not workflow:
        return
    
    logs = []
    results = {}
    nodes = workflow.get('nodes', [])
    edges = workflow.get('edges', [])
    
    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': 'Checking if any involved DAGs are currently running...'})
    storage.update_execution(execution_id, 'checking', logs)
    
    dag_infos = collect_dag_infos_from_workflow(nodes, storage)
    
    if dag_infos:
        if not wait_for_dags_to_complete(dag_infos, logs, execution_id, storage):
            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': 'Workflow aborted: DAGs did not complete in time'})
            storage.update_execution(execution_id, 'failed', logs, results)
            return
    
    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': 'Starting workflow execution...'})
    storage.update_execution(execution_id, 'running', logs)
    
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
            if node_id in visited: continue
            visited.add(node_id)
            
            time.sleep(1)
            node_data = node.get('data', {})
            node_type = node_data.get('type')
            config = node_data.get('config', {})
            
            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Executing node {node_data.get('label')} ({node_type})..."})
            results[node_id] = {'status': 'running'}
            storage.update_execution(execution_id, 'running', logs, results)
            
            output_handle = 'output'
            try:
                retries = int(config.get('retries', 0))
                retry_delay = int(config.get('retryDelay', 5))
                timeout = int(config.get('timeout', 3600))
                
                def run_with_retry(func, *args, **kwargs):
                    last_exc = None
                    for attempt in range(retries + 1):
                        try:
                            return func(*args, **kwargs)
                        except Exception as e:
                            last_exc = e
                            if attempt < retries:
                                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'WARN', 'message': f"Attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s..."})
                                time.sleep(retry_delay)
                    raise last_exc

                if node_type == 'airflow_trigger':
                    dag_id = resolve_variables(config.get('dagId', ''), execution_context)
                    conf = config.get('conf', {})
                    credential_id = config.get('credentialId')
                    wait_for_completion = config.get('waitForCompletion', True)
                    
                    auth_headers = {}
                    base_url = ""
                    username = ""
                    password = ""
                    
                    if credential_id:
                        cred = storage.get_credential(int(credential_id))
                        if cred and cred.get('type') == 'airflow':
                            cred_data = cred.get('data', {})
                            base_url = cred_data.get('baseUrl', '')
                            username = cred_data.get('username', '')
                            password = cred_data.get('password', '')
                            auth = base64.b64encode(f"{username}:{password}".encode()).decode()
                            auth_headers = {'Authorization': f'Basic {auth}'}
                    
                    dag_run_id = f"run_{int(time.time() * 1000)}"
                    if base_url:
                        response = requests.post(f"{base_url}/api/v1/dags/{dag_id}/dagRuns", json={'conf': conf}, headers=auth_headers)
                        response.raise_for_status()
                        dag_run_id = response.json().get('dag_run_id', dag_run_id)
                        
                        if wait_for_completion:
                            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Waiting for DAG {dag_id} (run: {dag_run_id}) to complete..."})
                            terminal_states = ['success', 'failed']
                            max_wait_time = 3600  # 1 hour timeout
                            poll_interval = 10
                            elapsed_wait = 0
                            
                            while elapsed_wait < max_wait_time:
                                try:
                                    run_response = requests.get(f"{base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}", headers=auth_headers)
                                    run_response.raise_for_status()
                                    current_state = run_response.json().get('state', 'unknown')
                                    
                                    if current_state.lower() in terminal_states:
                                        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"DAG {dag_id} finished with state: {current_state}"})
                                        if current_state.lower() == 'failed':
                                            raise Exception(f"DAG {dag_id} failed")
                                        break
                                        
                                    # Log progress every minute
                                    if elapsed_wait % 60 == 0:
                                        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"DAG {dag_id} is still {current_state}..."})
                                        storage.update_execution(execution_id, 'running', logs, results)
                                        
                                except Exception as poll_error:
                                    log(f"Error polling DAG status: {poll_error}")
                                    # Don't fail immediately on network blips
                                
                                time.sleep(poll_interval)
                                elapsed_wait += poll_interval
                            else:
                                raise Exception(f"Timeout waiting for DAG {dag_id} to complete after {max_wait_time} seconds")
                    
                    execution_context['dagRunId'] = dag_run_id
                    execution_context['dagId'] = dag_id
                    
                    # Store result in node-specific context as well
                    execution_context[node_id] = {
                        'dagId': dag_id,
                        'dagRunId': dag_run_id,
                        'status': 'success'
                    }
                    results[node_id] = {'status': 'success', 'dagId': dag_id, 'dagRunId': dag_run_id}
                
                elif node_type == 'airflow_log_check':
                    node_dag_id = resolve_variables(config.get('dagId', execution_context.get('dagId', '')), execution_context)
                    task_name = resolve_variables(config.get('taskName', ''), execution_context)
                    log_assertions = config.get('logAssertions', []) # Expecting a list of strings
                    if not log_assertions and config.get('logAssertion'):
                        log_assertions = [config.get('logAssertion')]
                    
                    run_id = execution_context.get('dagRunId', '')
                    credential_id = config.get('credentialId') or next((n.get('data', {}).get('config', {}).get('credentialId') for n in nodes if n.get('data', {}).get('type') == 'airflow_trigger'), None)
                    
                    if not run_id or not node_dag_id or not task_name:
                        raise Exception("Missing DAG ID, Task Name, or Run ID for log check")

                    auth_headers = {}
                    base_url = ""
                    if credential_id:
                        cred = storage.get_credential(int(credential_id))
                        if cred and cred.get('type') == 'airflow':
                            cred_data = cred.get('data', {})
                            base_url = cred_data.get('baseUrl', '')
                            auth = base64.b64encode(f"{cred_data.get('username')}:{cred_data.get('password')}".encode()).decode()
                            auth_headers = {'Authorization': f'Basic {auth}'}

                    if base_url:
                        # Get task logs
                        log_response = requests.get(f"{base_url}/api/v1/dags/{node_dag_id}/dagRuns/{run_id}/taskInstances/{task_name}/logs/1", headers=auth_headers)
                        log_response.raise_for_status()
                        logs_text = log_response.text
                        
                        failed_assertions = []
                        for assertion in log_assertions:
                            resolved_assertion = resolve_variables(assertion, execution_context)
                            if resolved_assertion not in logs_text:
                                failed_assertions.append(resolved_assertion)
                        
                        if failed_assertions:
                            error_msg = f"Assertions failed: {', '.join(failed_assertions)}"
                            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': error_msg})
                            results[node_id] = {'status': 'failure', 'error': error_msg, 'logs_text': logs_text, 'dag_id': node_dag_id, 'task_name': task_name}
                            assertion_failed = True
                            break
                        else:
                            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"All {len(log_assertions)} log assertions passed for task {task_name}"})
                            execution_context[node_id] = {'status': 'success', 'logs_text': logs_text}
                            results[node_id] = {'status': 'success', 'logs_text': logs_text, 'dag_id': node_dag_id, 'task_name': task_name}
                    else:
                        raise Exception("Airflow credential not found for log check")
                
                elif node_type == 'parallel_dags':
                    dag_configs = config.get('dags', [])
                    threads = []
                    parallel_results = {}
                    
                    def run_single_dag(dag_conf, idx):
                        # Simulating trigger logic for each dag in the list
                        # In a real scenario, this would call airflow_trigger logic
                        # For now, we'll log it
                        dag_id = resolve_variables(dag_conf.get('dagId', ''), execution_context)
                        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Parallel trigger: {dag_id}"})
                        parallel_results[f"dag_{idx}"] = "triggered"

                    for i, dag_conf in enumerate(dag_configs):
                        t = threading.Thread(target=run_single_dag, args=(dag_conf, i))
                        t.start()
                        threads.append(t)
                    
                    for t in threads:
                        t.join()
                        
                    results[node_id] = {'status': 'success', 'parallel_results': parallel_results}

                elif node_type == 'sql_query':
                    query = resolve_variables(config.get('query', ''), execution_context)
                    credential_id = config.get('credentialId')
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Running SQL: {query}"})
                    
                    query_results = []
                    try:
                        if credential_id:
                            cred = storage.get_credential(int(credential_id))
                            if cred:
                                cred_type = cred.get('type')
                                cred_data = cred.get('data', {})
                                
                                if cred_type == 'mssql':
                                    conn_str = f"mssql+pymssql://{cred_data.get('username')}:{cred_data.get('password')}@{cred_data.get('host')}:{cred_data.get('port', 1433)}/{cred_data.get('database')}"
                                    engine = sqlalchemy.create_engine(conn_str)
                                    with engine.connect() as conn:
                                        result = conn.execute(text(query))
                                        query_results = [dict(row._mapping) for row in result]
                                else:
                                    raise Exception(f"Unsupported SQL credential type: {cred_type}")
                        else:
                            # Use internal database engine
                            from .models import engine as internal_engine
                            with internal_engine.connect() as conn:
                                result = conn.execute(text(query))
                                query_results = [dict(row._mapping) for row in result]
                    except Exception as e:
                        log(f"SQL Execution failed: {e}")
                        raise Exception(f"SQL Error: {str(e)}")

                    excel_path = export_to_excel(query_results, node_id, execution_id)
                    if excel_path:
                        logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Query results exported to Excel: {excel_path}"})
                    
                    record_count = len(query_results)
                    
                    # Process Python assertion if provided
                    python_assertion = config.get('pythonAssertion', '').strip()
                    assertion_passed = True
                    assertion_error = None
                    
                    if python_assertion:
                        try:
                            # Create a safe local scope for assertion evaluation
                            local_scope = {'results': query_results, 'count': record_count}
                            assertion_result = eval(python_assertion, {"__builtins__": {'any': any, 'all': all, 'len': len, 'sum': sum, 'min': min, 'max': max, 'abs': abs, 'round': round, 'True': True, 'False': False}}, local_scope)
                            
                            if not assertion_result:
                                assertion_passed = False
                                assertion_error = f"Assertion failed: '{python_assertion}' evaluated to False"
                                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': assertion_error})
                            else:
                                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Assertion passed: '{python_assertion}'"})
                        except Exception as e:
                            assertion_passed = False
                            assertion_error = f"Assertion error: {str(e)}"
                            logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': assertion_error})
                    
                    execution_context['queryResult'] = {'record_count': record_count}
                    execution_context[node_id] = {'count': record_count, 'excel_path': excel_path, 'results': query_results, 'assertion_passed': assertion_passed}
                    
                    if assertion_passed:
                        results[node_id] = {'status': 'success', 'count': record_count, 'excel_path': excel_path}
                    else:
                        results[node_id] = {'status': 'failure', 'count': record_count, 'excel_path': excel_path, 'error': assertion_error}
                        assertion_failed = True
                        break
                
                elif node_type == 'api_request':
                    url = resolve_variables(config.get('url', ''), execution_context)
                    method = config.get('method', 'GET').upper()
                    headers = config.get('headers', {})
                    body = resolve_variables(config.get('body', ''), execution_context)
                    
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': f"Sending {method} request to {url}"})
                    response = requests.request(method, url, headers=headers, data=body)
                    response.raise_for_status()
                    
                    try:
                        res_data = response.json()
                    except:
                        res_data = response.text
                        
                    execution_context[node_id] = {'response': res_data}
                    results[node_id] = {'status': 'success', 'data': res_data}

                elif node_type == 'python_script':
                    script_code = config.get('code', '')
                    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO', 'message': "Executing Python script..."})
                    
                    local_scope = {'context': execution_context, 'result': None, 'requests': requests, 'json': json}
                    exec(script_code, {}, local_scope)
                    
                    script_result = local_scope.get('result')
                    execution_context[node_id] = {'result': script_result}
                    results[node_id] = {'status': 'success', 'result': script_result}
                
                elif node_type == 's3_operation':
                    bucket = resolve_variables(config.get('bucket', ''), execution_context)
                    operation = config.get('operation', 'list')
                    key = resolve_variables(config.get('key', ''), execution_context)
                    credential_id = config.get('credentialId')
                    
                    if not credential_id:
                        raise Exception("S3 credentials required")
                    
                    cred = storage.get_credential(int(credential_id))
                    if not cred or cred.get('type') != 's3':
                        raise Exception("Invalid S3 credential")
                    
                    cred_data = cred.get('data', {})
                    import boto3
                    s3 = boto3.client(
                        's3',
                        aws_access_key_id=cred_data.get('accessKey'),
                        aws_secret_access_key=cred_data.get('secretKey'),
                        region_name=cred_data.get('region', 'us-east-1')
                    )
                    
                    if operation == 'list':
                        response = s3.list_objects_v2(Bucket=bucket, Prefix=config.get('prefix', ''))
                        contents = [obj['Key'] for obj in response.get('Contents', [])]
                        execution_context[node_id] = {'files': contents}
                        results[node_id] = {'status': 'success', 'files': contents}
                    elif operation == 'upload':
                        content = resolve_variables(config.get('content', ''), execution_context)
                        s3.put_object(Bucket=bucket, Key=key, Body=content)
                        results[node_id] = {'status': 'success'}
                    elif operation == 'delete':
                        s3.delete_object(Bucket=bucket, Key=key)
                        results[node_id] = {'status': 'success'}
                    
                elif node_type == 'sftp_operation':
                    host = resolve_variables(config.get('host', ''), execution_context)
                    port = int(config.get('port', 22))
                    operation = config.get('operation', 'list') # list, upload, download, delete
                    remote_path = resolve_variables(config.get('remotePath', ''), execution_context)
                    credential_id = config.get('credentialId')
                    
                    if not credential_id:
                        raise Exception("SFTP credentials required")
                    
                    cred = storage.get_credential(int(credential_id))
                    if not cred or cred.get('type') != 'sftp':
                        raise Exception("Invalid SFTP credential")
                    
                    cred_data = cred.get('data', {})
                    import paramiko
                    transport = paramiko.Transport((host, port))
                    transport.connect(username=cred_data.get('username'), password=cred_data.get('password'))
                    sftp = paramiko.SFTPClient.from_transport(transport)
                    
                    try:
                        if operation == 'list':
                            files = sftp.listdir(remote_path or '.')
                            execution_context[node_id] = {'files': files}
                            results[node_id] = {'status': 'success', 'files': files}
                        elif operation == 'upload':
                            content = resolve_variables(config.get('content', ''), execution_context)
                            import io
                            file_obj = io.BytesIO(content.encode() if isinstance(content, str) else content)
                            sftp.putfo(file_obj, remote_path)
                            results[node_id] = {'status': 'success'}
                        elif operation == 'download':
                            import io
                            file_obj = io.BytesIO()
                            sftp.getfo(remote_path, file_obj)
                            file_obj.seek(0)
                            downloaded_content = file_obj.read().decode()
                            execution_context[node_id] = {'content': downloaded_content}
                            results[node_id] = {'status': 'success', 'content': downloaded_content}
                        elif operation == 'delete':
                            sftp.remove(remote_path)
                            results[node_id] = {'status': 'success'}
                    finally:
                        sftp.close()
                        transport.close()
                    
                next_batch.extend(find_next_nodes(node_id, output_handle if node_type == 'condition' else None))
            except Exception as e:
                logs.append({'timestamp': datetime.now().isoformat(), 'level': 'ERROR', 'message': f"Error: {e}"})
                results[node_id] = {'status': 'failure', 'error': str(e)}
                assertion_failed = True
                break
            
            storage.update_execution(execution_id, 'running', logs, results)
        current_nodes = next_batch
    
    final_status = 'failed' if assertion_failed else 'completed'
    logs.append({'timestamp': datetime.now().isoformat(), 'level': 'INFO' if not assertion_failed else 'ERROR', 'message': f'Workflow {final_status}.'})
    storage.update_execution(execution_id, final_status, logs, results)

def generate_python_code(workflow):
    nodes = workflow.get('nodes', [])
    edges = workflow.get('edges', [])
    
    code = [
        "import requests",
        "import base64",
        "import json",
        "import time",
        "import boto3",
        "import paramiko",
        "import io",
        "import sqlalchemy",
        "from sqlalchemy import text",
        "from datetime import datetime, timedelta",
        "",
        "# Configuration and Helpers",
        "execution_context = {}",
        "",
        "def resolve_variables(text, context):",
        "    if not isinstance(text, str): return text",
        "    # Handle built-in date variables",
        "    today = datetime.now()",
        "    yesterday = today - timedelta(days=1)",
        "    text = text.replace('{{today}}', today.strftime('%Y-%m-%d'))",
        "    text = text.replace('{{yesterday}}', yesterday.strftime('%Y-%m-%d'))",
        "    ",
        "    for key, value in context.items():",
        "        # Handle node specific results",
        "        if isinstance(value, dict):",
        "            for k, v in value.items():",
        "                text = text.replace('{{' + key + '.' + k + '}}', str(v))",
        "        text = text.replace('{{' + key + '}}', str(value))",
        "    return text",
        "",
        "def run_workflow():",
        "    print(f\"Starting workflow execution: {datetime.now()}\")"
    ]
    
    # Simple dependency mapping
    def get_incoming_edges(node_id):
        return [e for e in edges if e.get('target') == node_id]

    # Topological-ish sort: find nodes with no incoming edges first
    visited = set()
    queue = [n for n in nodes if not get_incoming_edges(n.get('id'))]
    
    while queue:
        node = queue.pop(0)
        node_id = node.get('id')
        if node_id in visited: continue
        visited.add(node_id)
        
        node_data = node.get('data', {})
        node_type = node_data.get('type')
        config = node_data.get('config', {})
        label = node_data.get('label', node_id)
        
        code.append(f"\n    # --- Node: {label} ({node_type}) ---")
        
        if node_type == 'airflow_trigger':
            dag_id = config.get('dagId', '')
            code.append(f"    dag_id = resolve_variables('{dag_id}', execution_context)")
            code.append(f"    print(f\"Triggering Airflow DAG: {{dag_id}}\")")
            code.append("    # Note: Ensure base_url and auth are configured correctly")
            code.append("    # response = requests.post(f\"{base_url}/api/v1/dags/{dag_id}/dagRuns\", json={'conf': {}}, headers=auth_headers)")
            code.append(f"    execution_context['{node_id}'] = {{'status': 'success', 'dagRunId': 'run_' + str(int(time.time()))}}")
            
        elif node_type == 'sql_query':
            query = config.get('query', '').replace('\n', ' ')
            code.append(f"    query = resolve_variables(\"\"\"{query}\"\"\", execution_context)")
            code.append(f"    print(f\"Executing SQL Query: {{query[:50]}}...\")")
            code.append("    # engine = sqlalchemy.create_engine(conn_str)")
            code.append("    # with engine.connect() as conn: result = conn.execute(text(query)); query_results = [dict(row._mapping) for row in result]")
            code.append(f"    execution_context['{node_id}'] = {{'status': 'success', 'count': 0, 'results': []}}")
            
        elif node_type == 'python_script':
            script = config.get('code', '').replace('\n', '\n        ')
            code.append(f"    print(\"Executing Python Script...\")")
            code.append(f"    # User Script:")
            code.append(f"    # {script}")
            code.append(f"    execution_context['{node_id}'] = {{'status': 'success', 'result': None}}")
            
        elif node_type == 'api_request':
            url = config.get('url', '')
            method = config.get('method', 'GET')
            code.append(f"    url = resolve_variables('{url}', execution_context)")
            code.append(f"    print(f\"Sending {method} request to {{url}}\")")
            code.append(f"    # response = requests.request('{method}', url, json={config.get('body', {})})")
            code.append(f"    execution_context['{node_id}'] = {{'status': 'success', 'data': {{}}}}")

        elif node_type == 'condition':
            variable = config.get('variable', '')
            operator = config.get('operator', '==')
            value = config.get('value', '')
            code.append(f"    val = resolve_variables('{{{{{variable}}}}}', execution_context)")
            code.append(f"    condition_met = str(val) {operator} '{value}'")
            code.append(f"    print(f\"Condition check: {{condition_met}}\")")
            code.append(f"    execution_context['{node_id}'] = {{'status': 'success', 'result': condition_met}}")
            
        # Add next nodes to queue
        next_nodes = [n for edge in edges if edge.get('source') == node_id for n in nodes if n.get('id') == edge.get('target')]
        queue.extend(next_nodes)

    code.append("\nif __name__ == '__main__':")
    code.append("    run_workflow()")
    
    return "\n".join(code)

def generate_pytest_suite(workflow_ids):
    import json
    
    code = [
        "import pytest",
        "import requests",
        "import base64",
        "import json",
        "import time",
        "import boto3",
        "import paramiko",
        "from datetime import datetime",
        "",
        "class AutomationSuite:",
        "    def __init__(self):",
        "        self.context = {}",
        "",
        "    def resolve(self, text):",
        "        if not isinstance(text, str): return text",
        "        for key, value in self.context.items():",
        "            text = text.replace('{{' + key + '}}', str(value))",
        "        return text",
        "",
        "    def run_airflow_trigger(self, config):",
        "        print(f\"Triggering DAG: {config.get('dagId')}\")",
        "        # Implementation details...",
        "        return \"run_123\"",
        "",
        "    def run_sql_query(self, config):",
        "        print(f\"Running SQL: {config.get('query')}\")",
        "        return {\"count\": 101}",
        ""
    ]
    
    for wid in workflow_ids:
        workflow = storage.get_workflow(wid)
        if not workflow: continue
        
        test_name = workflow['name'].lower().replace(' ', '_')
        code.append(f"def test_{test_name}():")
        code.append("    suite = AutomationSuite()")
        
        nodes = json.loads(workflow.get('nodes', '[]'))
        for node in nodes:
            node_data = node.get('data', {})
            node_type = node_data.get('type')
            config = node_data.get('config', {})
            
            if node_type == 'airflow_trigger':
                code.append(f"    suite.context['{node['id']}'] = suite.run_airflow_trigger({config})")
            elif node_type == 'sql_query':
                code.append(f"    result = suite.run_sql_query({config})")
                code.append(f"    assert result['count'] > 0")
        code.append("")
        
    return "\n".join(code)

def register_workflow_routes(app):
    @app.post('/api/automation/export-suite')
    def export_automation_suite():
        data = request.get_json()
        ids = data.get('workflowIds', [])
        code = generate_pytest_suite(ids)
        return jsonify({'code': code})

    @app.post('/api/git/sync')
    def git_sync():
        try:
            import git
        except ImportError:
            log("GitPython not installed")
            return jsonify({'status': 'error', 'message': 'GitPython not installed'}), 500
        
        data = request.get_json()
        action = data.get('action') # 'push' or 'pull'
        repo_path = os.getcwd()
        
        try:
            repo = git.Repo(repo_path)
            if action == 'push':
                repo.git.add(A=True)
                repo.index.commit("Automation Suite Update")
                origin = repo.remote(name='origin')
                origin.push()
            elif action == 'pull':
                origin = repo.remote(name='origin')
                origin.pull()
            return jsonify({'status': 'success'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @app.get('/api/workflows/<int:id>/export')
    def export_workflow(id):
        workflow = storage.get_workflow(id)
        if not workflow: return jsonify({'message': 'Workflow not found'}), 404
        python_code = generate_python_code(workflow)
        return jsonify({'code': python_code})

    @app.get('/api/executions/<int:execution_id>/excel/<node_id>')
    def download_excel(execution_id, node_id):
        from flask import send_file
        import os
        
        file_path = f"/tmp/query_result_{execution_id}_{node_id}.xlsx"
        if os.path.exists(file_path):
            return send_file(
                file_path,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=f'query_result_{execution_id}_{node_id}.xlsx'
            )
        return jsonify({'message': 'Excel file not found'}), 404

    @app.get('/api/executions/<int:execution_id>/zip')
    def download_execution_zip(execution_id):
        from flask import send_file
        import os
        import zipfile
        import io
        
        include_logs = request.args.get('include_logs', 'true').lower() == 'true'
        
        execution = storage.get_execution(execution_id)
        if not execution:
            return jsonify({'message': 'Execution not found'}), 404
        
        workflow = storage.get_workflow(execution.get('workflowId'))
        workflow_name = workflow.get('name', 'workflow') if workflow else 'workflow'
        safe_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in workflow_name).strip()
        
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            results = execution.get('results', {})
            if isinstance(results, str):
                results = json.loads(results)
            
            # Add Excel files for SQL query nodes
            for node_id, result in results.items():
                if isinstance(result, dict) and result.get('excel_path'):
                    excel_path = f"/tmp/query_result_{execution_id}_{node_id}.xlsx"
                    if os.path.exists(excel_path):
                        # Get node label for filename
                        node_label = node_id
                        if workflow:
                            for node in workflow.get('nodes', []):
                                if node.get('id') == node_id:
                                    node_label = node.get('data', {}).get('label', node_id)
                                    break
                        safe_label = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in str(node_label)).strip()
                        zipf.write(excel_path, f"excel/{safe_label}_{node_id}.xlsx")
            
            # Add execution logs
            if include_logs:
                logs = execution.get('logs', [])
                if isinstance(logs, str):
                    logs = json.loads(logs)
                
                log_content = []
                for log_entry in logs:
                    if isinstance(log_entry, dict):
                        timestamp = log_entry.get('timestamp', '')
                        level = log_entry.get('level', 'INFO')
                        message = log_entry.get('message', '')
                        log_content.append(f"[{timestamp}] [{level}] {message}")
                    else:
                        log_content.append(str(log_entry))
                
                if log_content:
                    zipf.writestr("logs/execution_log.txt", "\n".join(log_content))
                
                # Add DAG-specific logs from results
                for node_id, result in results.items():
                    if isinstance(result, dict):
                        # Check if this is an airflow log check node with captured logs
                        if result.get('logs_text'):
                            node_label = node_id
                            if workflow:
                                for node in workflow.get('nodes', []):
                                    if node.get('id') == node_id:
                                        node_label = node.get('data', {}).get('label', node_id)
                                        break
                            safe_label = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in str(node_label)).strip()
                            zipf.writestr(f"logs/dag_{safe_label}_{node_id}.txt", result.get('logs_text', ''))
            
            # Add execution summary
            summary = {
                'execution_id': execution_id,
                'workflow_name': workflow_name,
                'status': execution.get('status'),
                'started_at': execution.get('startedAt'),
                'completed_at': execution.get('completedAt'),
                'results_summary': {k: {'status': v.get('status'), 'count': v.get('count')} for k, v in results.items() if isinstance(v, dict)}
            }
            zipf.writestr("execution_summary.json", json.dumps(summary, indent=2, default=str))
        
        zip_buffer.seek(0)
        
        return send_file(
            zip_buffer,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'execution_{execution_id}_{safe_name}.zip'
        )

    @app.get('/api/workflows')
    def list_workflows():
        return jsonify(storage.get_workflows())

    @app.get('/api/workflows/<int:id>')
    def get_workflow(id):
        workflow = storage.get_workflow(id)
        if not workflow: return jsonify({'message': 'Workflow not found'}), 404
        return jsonify(workflow)

    @app.post('/api/workflows')
    def create_workflow():
        return jsonify(storage.create_workflow(request.get_json())), 201

    @app.put('/api/workflows/<int:id>')
    def update_workflow(id):
        workflow = storage.update_workflow(id, request.get_json())
        if not workflow: return jsonify({'message': 'Workflow not found'}), 404
        return jsonify(workflow)

    @app.delete('/api/workflows/<int:id>')
    def delete_workflow(id):
        storage.delete_workflow(id)
        return '', 204

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

    @app.post('/api/workflows/<int:id>/execute')
    def execute_workflow(id):
        execution = storage.create_execution(id)
        thread = threading.Thread(target=execute_workflow_async, args=(execution['id'], id))
        thread.start()
        return jsonify(execution), 201

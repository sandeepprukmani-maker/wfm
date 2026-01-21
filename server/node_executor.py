import json
import re
import time
import sqlite3
import requests
from datetime import datetime
from typing import Any
from server.llm_service import generate_code, analyze_output


def substitute_variables(text: str, context: dict, quote_strings: bool = False) -> str:
    """Replace {{ node_id.variable }} or {{ node_id.rows[0].column }} patterns with actual values from context.
    
    Args:
        text: The text containing variable placeholders
        context: Dictionary of node outputs
        quote_strings: If True, wrap string values in quotes (for use in eval conditions)
    """
    pattern = r'\{\{\s*([\w\.\[\]]+)\s*\}\}'
    
    def parse_path(path: str) -> list:
        """Parse a path like 'node_id.rows[0].column' into segments."""
        segments = []
        current = ""
        i = 0
        while i < len(path):
            char = path[i]
            if char == '.':
                if current:
                    segments.append(current)
                    current = ""
            elif char == '[':
                if current:
                    segments.append(current)
                    current = ""
                j = i + 1
                while j < len(path) and path[j] != ']':
                    j += 1
                index_str = path[i+1:j]
                if index_str.isdigit():
                    segments.append(int(index_str))
                else:
                    segments.append(index_str)
                i = j
            else:
                current += char
            i += 1
        if current:
            segments.append(current)
        return segments
    
    def replacer(match):
        path = match.group(1)
        segments = parse_path(path)
        value = context
        try:
            for segment in segments:
                if isinstance(value, dict):
                    if isinstance(segment, str):
                        value = value.get(segment, match.group(0))
                    else:
                        return match.group(0)
                elif isinstance(value, list) and isinstance(segment, int):
                    if 0 <= segment < len(value):
                        value = value[segment]
                    else:
                        return match.group(0)
                else:
                    return match.group(0)
            if isinstance(value, (dict, list)):
                return json.dumps(value)
            if value is None:
                return 'None'
            if isinstance(value, bool):
                return 'True' if value else 'False'
            if isinstance(value, (int, float)):
                return str(value)
            if quote_strings and isinstance(value, str):
                return repr(value)
            return str(value)
        except:
            return match.group(0)
    
    return re.sub(pattern, replacer, str(text))


def evaluate_condition(condition: str, context: dict) -> bool:
    """Evaluate a condition string with context variables."""
    try:
        substituted = substitute_variables(condition, context, quote_strings=True)
        safe_globals = {"__builtins__": {}, "None": None, "True": True, "False": False}
        safe_locals = {}
        result = eval(substituted, safe_globals, safe_locals)
        return bool(result)
    except Exception as e:
        return False


class NodeExecutor:
    """Executes workflow nodes with output passing."""
    
    def __init__(self, db_path: str = "workflow.db"):
        self.db_path = db_path
        self.context = {}
    
    def execute_node(self, node: dict, context: dict) -> dict:
        """Execute a single node and return its output."""
        self.context = context
        node_type = node.get('type', '')
        node_data = node.get('data', {})
        node_id = node.get('id', '')
        
        executors = {
            'api': self._execute_api_node,
            'sql': self._execute_sql_node,
            'python': self._execute_python_node,
            'decision': self._execute_decision_node,
            'wait': self._execute_wait_node,
            'llm': self._execute_llm_node,
            'airflow': self._execute_airflow_node,
        }
        
        executor = executors.get(node_type)
        if not executor:
            return {
                'status': 'failed',
                'error': f'Unknown node type: {node_type}',
                'outputs': {}
            }
        
        try:
            result = executor(node_data, node_id)
            return {
                'status': 'success',
                'outputs': result,
                'logs': result.get('_logs', '')
            }
        except Exception as e:
            return {
                'status': 'failed',
                'error': str(e),
                'outputs': {}
            }
    
    def _execute_api_node(self, data: dict, node_id: str) -> dict:
        """Execute an API call node."""
        url = substitute_variables(data.get('url', ''), self.context)
        method = data.get('method', 'GET').upper()
        headers = data.get('headers', {})
        body = data.get('body', '')
        
        if headers:
            headers = {k: substitute_variables(str(v), self.context) for k, v in headers.items()}
        
        if body:
            body = substitute_variables(body, self.context)
            try:
                body = json.loads(body)
            except:
                pass
        
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=body if isinstance(body, dict) else None,
            data=body if isinstance(body, str) else None,
            timeout=30
        )
        
        try:
            response_data = response.json()
        except:
            response_data = response.text
        
        return {
            'status_code': response.status_code,
            'response': response_data,
            'headers': dict(response.headers),
            '_logs': f'API {method} {url} -> {response.status_code}'
        }
    
    def _execute_sql_node(self, data: dict, node_id: str) -> dict:
        """Execute a SQL query node."""
        query = substitute_variables(data.get('query', ''), self.context)
        db_path = data.get('database', self.db_path)
        
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                rows = cursor.fetchall()
                columns = [description[0] for description in cursor.description] if cursor.description else []
                result = [dict(zip(columns, row)) for row in rows]
                return {
                    'rows': result,
                    'row_count': len(result),
                    'columns': columns,
                    '_logs': f'SQL query returned {len(result)} rows'
                }
            else:
                conn.commit()
                return {
                    'affected_rows': cursor.rowcount,
                    '_logs': f'SQL query affected {cursor.rowcount} rows'
                }
        finally:
            conn.close()
    
    def _execute_python_node(self, data: dict, node_id: str) -> dict:
        """Execute Python code node."""
        import io
        import sys
        
        code = substitute_variables(data.get('code', ''), self.context)
        
        local_vars = {'context': self.context.copy(), 'result': {}}
        
        for key, value in self.context.items():
            local_vars[key] = value
        
        old_stdout = sys.stdout
        sys.stdout = captured_output = io.StringIO()
        
        try:
            exec(code, {'__builtins__': __builtins__}, local_vars)
        finally:
            sys.stdout = old_stdout
        
        printed_output = captured_output.getvalue()
        
        result = local_vars.get('result', {})
        if not isinstance(result, dict):
            result = {'value': result}
        
        result['printed'] = printed_output.strip() if printed_output else None
        result['_logs'] = f'Python code executed successfully'
        if printed_output:
            result['_logs'] += f'\nOutput:\n{printed_output}'
        return result
    
    def _execute_decision_node(self, data: dict, node_id: str) -> dict:
        """Execute a decision/conditional node."""
        condition = data.get('condition', 'True')
        substituted = substitute_variables(condition, self.context, quote_strings=True)
        result = evaluate_condition(condition, self.context)
        
        context_keys = list(self.context.keys())
        
        return {
            'condition': condition,
            'substituted': substituted,
            'result': result,
            'branch': 'true' if result else 'false',
            'available_nodes': context_keys,
            '_logs': f'Decision: {condition} => {substituted} => {result}'
        }
    
    def _execute_wait_node(self, data: dict, node_id: str) -> dict:
        """Execute a wait/delay node."""
        seconds = int(data.get('seconds', 10))
        time.sleep(seconds)
        
        return {
            'waited': seconds,
            '_logs': f'Waited for {seconds} seconds'
        }
    
    def _execute_llm_node(self, data: dict, node_id: str) -> dict:
        """Execute an LLM code generation node."""
        prompt = substitute_variables(data.get('prompt', ''), self.context)
        code_type = data.get('code_type', 'python')
        execute_code = data.get('execute_code', False)
        
        llm_result = generate_code(prompt, code_type, self.context)
        
        result = {
            'generated_code': llm_result.get('code', ''),
            'explanation': llm_result.get('explanation', ''),
            'required_inputs': llm_result.get('required_inputs', []),
            '_logs': f'LLM generated {code_type} code'
        }
        
        if execute_code and code_type == 'python':
            try:
                exec_result = self._execute_python_node({'code': llm_result.get('code', '')}, node_id)
                result['execution_result'] = exec_result
                result['_logs'] += f'\nCode executed: {exec_result}'
            except Exception as e:
                result['execution_error'] = str(e)
                result['_logs'] += f'\nCode execution failed: {e}'
        
        elif execute_code and code_type == 'sql':
            try:
                exec_result = self._execute_sql_node({'query': llm_result.get('code', '')}, node_id)
                result['execution_result'] = exec_result
                result['_logs'] += f'\nSQL executed: {exec_result}'
            except Exception as e:
                result['execution_error'] = str(e)
                result['_logs'] += f'\nSQL execution failed: {e}'
        
        return result
    
    def _execute_airflow_node(self, data: dict, node_id: str) -> dict:
        """Execute Airflow DAG trigger/status check node."""
        action = data.get('action', 'trigger')
        base_url = substitute_variables(data.get('base_url', ''), self.context)
        dag_id = substitute_variables(data.get('dag_id', ''), self.context)
        run_id = substitute_variables(data.get('run_id', ''), self.context)
        auth = data.get('auth', {})
        
        headers = {'Content-Type': 'application/json'}
        if auth.get('username') and auth.get('password'):
            import base64
            credentials = base64.b64encode(f"{auth['username']}:{auth['password']}".encode()).decode()
            headers['Authorization'] = f'Basic {credentials}'
        
        if action == 'trigger':
            url = f"{base_url}/api/v1/dags/{dag_id}/dagRuns"
            conf = data.get('conf', {})
            if isinstance(conf, str):
                conf = json.loads(substitute_variables(conf, self.context))
            
            response = requests.post(url, headers=headers, json={'conf': conf}, timeout=30)
            response_data = response.json()
            
            return {
                'dag_run_id': response_data.get('dag_run_id'),
                'state': response_data.get('state'),
                'execution_date': response_data.get('execution_date'),
                '_logs': f'Triggered DAG {dag_id}'
            }
        
        elif action == 'status':
            url = f"{base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
            response = requests.get(url, headers=headers, timeout=30)
            response_data = response.json()
            
            return {
                'state': response_data.get('state'),
                'is_complete': response_data.get('state') in ['success', 'failed'],
                'is_success': response_data.get('state') == 'success',
                '_logs': f'DAG {dag_id} status: {response_data.get("state")}'
            }
        
        elif action == 'logs':
            url = f"{base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
            response = requests.get(url, headers=headers, timeout=30)
            task_instances = response.json().get('task_instances', [])
            
            logs = []
            for task in task_instances:
                task_id = task.get('task_id')
                log_url = f"{base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
                log_response = requests.get(log_url, headers=headers, timeout=30)
                logs.append({
                    'task_id': task_id,
                    'state': task.get('state'),
                    'log_content': log_response.text
                })
            
            return {
                'logs': logs,
                'log_content': '\n'.join([l.get('log_content', '') for l in logs]),
                '_logs': f'Fetched logs for {len(logs)} tasks'
            }
        
        return {'error': f'Unknown action: {action}'}

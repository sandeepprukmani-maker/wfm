import base64
import requests
from datetime import datetime
from typing import Optional, Dict, Any, List


class AirflowAPI:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.auth_headers = {
            'Authorization': f'Basic {base64.b64encode(f"{username}:{password}".encode()).decode()}',
            'Content-Type': 'application/json'
        }
    
    def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, json_data: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/api/v1{endpoint}"
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.auth_headers,
                params=params,
                json=json_data,
                timeout=30
            )
            response.raise_for_status()
            if response.text:
                return {'success': True, 'data': response.json()}
            return {'success': True, 'data': None}
        except requests.exceptions.HTTPError as e:
            error_msg = str(e)
            try:
                error_data = e.response.json()
                error_msg = error_data.get('detail', str(e))
            except:
                pass
            return {'success': False, 'error': error_msg, 'status_code': e.response.status_code if e.response else None}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def list_dags(self, limit: int = 100, offset: int = 0, only_active: bool = False, paused: Optional[bool] = None) -> Dict:
        params = {'limit': limit, 'offset': offset, 'only_active': only_active}
        if paused is not None:
            params['paused'] = paused
        return self._request('GET', '/dags', params=params)
    
    def get_dag(self, dag_id: str) -> Dict:
        return self._request('GET', f'/dags/{dag_id}')
    
    def get_dag_details(self, dag_id: str) -> Dict:
        return self._request('GET', f'/dags/{dag_id}/details')
    
    def pause_dag(self, dag_id: str) -> Dict:
        return self._request('PATCH', f'/dags/{dag_id}', json_data={'is_paused': True})
    
    def unpause_dag(self, dag_id: str) -> Dict:
        return self._request('PATCH', f'/dags/{dag_id}', json_data={'is_paused': False})
    
    def get_dag_tasks(self, dag_id: str) -> Dict:
        return self._request('GET', f'/dags/{dag_id}/tasks')
    
    def get_task(self, dag_id: str, task_id: str) -> Dict:
        return self._request('GET', f'/dags/{dag_id}/tasks/{task_id}')

    def list_dag_runs(self, dag_id: str, limit: int = 25, offset: int = 0, state: Optional[str] = None, order_by: str = '-execution_date') -> Dict:
        params = {'limit': limit, 'offset': offset, 'order_by': order_by}
        if state:
            params['state'] = state
        return self._request('GET', f'/dags/{dag_id}/dagRuns', params=params)
    
    def trigger_dag(self, dag_id: str, conf: Optional[Dict] = None, logical_date: Optional[str] = None, dag_run_id: Optional[str] = None) -> Dict:
        json_data = {'conf': conf or {}}
        if logical_date:
            json_data['logical_date'] = logical_date
        if dag_run_id:
            json_data['dag_run_id'] = dag_run_id
        return self._request('POST', f'/dags/{dag_id}/dagRuns', json_data=json_data)
    
    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict:
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}')
    
    def update_dag_run_state(self, dag_id: str, dag_run_id: str, state: str) -> Dict:
        return self._request('PATCH', f'/dags/{dag_id}/dagRuns/{dag_run_id}', json_data={'state': state})
    
    def delete_dag_run(self, dag_id: str, dag_run_id: str) -> Dict:
        return self._request('DELETE', f'/dags/{dag_id}/dagRuns/{dag_run_id}')
    
    def clear_dag_run(self, dag_id: str, dag_run_id: str, dry_run: bool = False) -> Dict:
        json_data = {
            'dag_run_id': dag_run_id,
            'dry_run': dry_run,
            'reset_dag_runs': True
        }
        return self._request('POST', f'/dags/{dag_id}/clearTaskInstances', json_data=json_data)

    def list_task_instances(self, dag_id: str, dag_run_id: str, limit: int = 100, offset: int = 0) -> Dict:
        params = {'limit': limit, 'offset': offset}
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances', params=params)
    
    def get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str) -> Dict:
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}')
    
    def update_task_instance_state(self, dag_id: str, dag_run_id: str, task_id: str, new_state: str) -> Dict:
        return self._request('PATCH', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}', json_data={'state': new_state})
    
    def get_task_logs(self, dag_id: str, dag_run_id: str, task_id: str, task_try_number: int = 1, full_content: bool = True) -> Dict:
        params = {'full_content': full_content}
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}', params=params)
    
    def clear_task_instances(self, dag_id: str, dag_run_id: Optional[str] = None, task_ids: Optional[List[str]] = None, 
                             start_date: Optional[str] = None, end_date: Optional[str] = None, 
                             include_downstream: bool = False, include_future: bool = False,
                             include_past: bool = False, only_failed: bool = False, only_running: bool = False,
                             dry_run: bool = False) -> Dict:
        json_data = {
            'dry_run': dry_run,
            'include_downstream': include_downstream,
            'include_future': include_future,
            'include_past': include_past,
            'only_failed': only_failed,
            'only_running': only_running,
            'reset_dag_runs': True
        }
        if dag_run_id:
            json_data['dag_run_id'] = dag_run_id
        if task_ids:
            json_data['task_ids'] = task_ids
        if start_date:
            json_data['start_date'] = start_date
        if end_date:
            json_data['end_date'] = end_date
        return self._request('POST', f'/dags/{dag_id}/clearTaskInstances', json_data=json_data)

    def list_connections(self, limit: int = 100, offset: int = 0) -> Dict:
        return self._request('GET', '/connections', params={'limit': limit, 'offset': offset})
    
    def get_connection(self, connection_id: str) -> Dict:
        return self._request('GET', f'/connections/{connection_id}')
    
    def create_connection(self, connection_id: str, conn_type: str, host: Optional[str] = None, 
                          login: Optional[str] = None, password: Optional[str] = None,
                          port: Optional[int] = None, schema: Optional[str] = None,
                          extra: Optional[Dict] = None, description: Optional[str] = None) -> Dict:
        json_data = {'connection_id': connection_id, 'conn_type': conn_type}
        if host: json_data['host'] = host
        if login: json_data['login'] = login
        if password: json_data['password'] = password
        if port: json_data['port'] = port
        if schema: json_data['schema'] = schema
        if extra: json_data['extra'] = str(extra)
        if description: json_data['description'] = description
        return self._request('POST', '/connections', json_data=json_data)
    
    def update_connection(self, connection_id: str, **kwargs) -> Dict:
        return self._request('PATCH', f'/connections/{connection_id}', json_data=kwargs)
    
    def delete_connection(self, connection_id: str) -> Dict:
        return self._request('DELETE', f'/connections/{connection_id}')

    def list_pools(self, limit: int = 100, offset: int = 0) -> Dict:
        return self._request('GET', '/pools', params={'limit': limit, 'offset': offset})
    
    def get_pool(self, pool_name: str) -> Dict:
        return self._request('GET', f'/pools/{pool_name}')
    
    def create_pool(self, name: str, slots: int, description: Optional[str] = None) -> Dict:
        json_data = {'name': name, 'slots': slots}
        if description: json_data['description'] = description
        return self._request('POST', '/pools', json_data=json_data)
    
    def update_pool(self, pool_name: str, slots: Optional[int] = None, description: Optional[str] = None) -> Dict:
        json_data = {}
        if slots is not None: json_data['slots'] = slots
        if description: json_data['description'] = description
        return self._request('PATCH', f'/pools/{pool_name}', json_data=json_data)
    
    def delete_pool(self, pool_name: str) -> Dict:
        return self._request('DELETE', f'/pools/{pool_name}')

    def list_variables(self, limit: int = 100, offset: int = 0) -> Dict:
        return self._request('GET', '/variables', params={'limit': limit, 'offset': offset})
    
    def get_variable(self, variable_key: str) -> Dict:
        return self._request('GET', f'/variables/{variable_key}')
    
    def create_variable(self, key: str, value: str, description: Optional[str] = None) -> Dict:
        json_data = {'key': key, 'value': value}
        if description: json_data['description'] = description
        return self._request('POST', '/variables', json_data=json_data)
    
    def update_variable(self, variable_key: str, value: str, description: Optional[str] = None) -> Dict:
        json_data = {'key': variable_key, 'value': value}
        if description: json_data['description'] = description
        return self._request('PATCH', f'/variables/{variable_key}', json_data=json_data)
    
    def delete_variable(self, variable_key: str) -> Dict:
        return self._request('DELETE', f'/variables/{variable_key}')

    def list_xcoms(self, dag_id: str, dag_run_id: str, task_id: str, limit: int = 100, offset: int = 0) -> Dict:
        params = {'limit': limit, 'offset': offset}
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries', params=params)
    
    def get_xcom(self, dag_id: str, dag_run_id: str, task_id: str, xcom_key: str) -> Dict:
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}')

    def list_event_logs(self, limit: int = 100, offset: int = 0, dag_id: Optional[str] = None, 
                        event: Optional[str] = None, owner: Optional[str] = None) -> Dict:
        params = {'limit': limit, 'offset': offset}
        if dag_id: params['dag_id'] = dag_id
        if event: params['event'] = event
        if owner: params['owner'] = owner
        return self._request('GET', '/eventLogs', params=params)
    
    def get_event_log(self, event_log_id: int) -> Dict:
        return self._request('GET', f'/eventLogs/{event_log_id}')

    def list_import_errors(self, limit: int = 100, offset: int = 0) -> Dict:
        return self._request('GET', '/importErrors', params={'limit': limit, 'offset': offset})
    
    def get_import_error(self, import_error_id: int) -> Dict:
        return self._request('GET', f'/importErrors/{import_error_id}')

    def get_health(self) -> Dict:
        return self._request('GET', '/health')
    
    def get_version(self) -> Dict:
        return self._request('GET', '/version')
    
    def get_config(self) -> Dict:
        return self._request('GET', '/config')

    def list_plugins(self) -> Dict:
        return self._request('GET', '/plugins')
    
    def list_providers(self) -> Dict:
        return self._request('GET', '/providers')

    def get_dag_source(self, file_token: str) -> Dict:
        return self._request('GET', f'/dagSources/{file_token}')

    def list_dag_warnings(self, dag_id: Optional[str] = None, limit: int = 100, offset: int = 0) -> Dict:
        params = {'limit': limit, 'offset': offset}
        if dag_id: params['dag_id'] = dag_id
        return self._request('GET', '/dagWarnings', params=params)

    def get_dag_stats(self, dag_ids: List[str]) -> Dict:
        params = {'dag_ids': ','.join(dag_ids)}
        return self._request('GET', '/dags/~/dagRuns/~/statistics', params=params)


AIRFLOW_OPERATIONS = {
    "list_dags": {
        "description": "List all DAGs in Airflow",
        "method": "list_dags",
        "params": ["limit", "offset", "only_active", "paused"],
        "keywords": ["list dags", "show dags", "get all dags", "display dags", "dags list", "what dags", "which dags"]
    },
    "get_dag": {
        "description": "Get details of a specific DAG",
        "method": "get_dag",
        "params": ["dag_id"],
        "required": ["dag_id"],
        "keywords": ["get dag", "dag details", "dag info", "show dag", "describe dag"]
    },
    "get_dag_details": {
        "description": "Get detailed information about a DAG including tasks",
        "method": "get_dag_details",
        "params": ["dag_id"],
        "required": ["dag_id"],
        "keywords": ["dag details with tasks", "full dag info", "complete dag details"]
    },
    "pause_dag": {
        "description": "Pause a DAG",
        "method": "pause_dag",
        "params": ["dag_id"],
        "required": ["dag_id"],
        "keywords": ["pause dag", "stop dag", "disable dag", "halt dag", "suspend dag"]
    },
    "unpause_dag": {
        "description": "Unpause/resume a DAG",
        "method": "unpause_dag",
        "params": ["dag_id"],
        "required": ["dag_id"],
        "keywords": ["unpause dag", "resume dag", "enable dag", "start dag", "activate dag"]
    },
    "get_dag_tasks": {
        "description": "Get all tasks for a DAG",
        "method": "get_dag_tasks",
        "params": ["dag_id"],
        "required": ["dag_id"],
        "keywords": ["dag tasks", "list tasks", "get tasks", "show tasks", "tasks in dag"]
    },
    "trigger_dag": {
        "description": "Trigger a new DAG run",
        "method": "trigger_dag",
        "params": ["dag_id", "conf", "logical_date", "dag_run_id"],
        "required": ["dag_id"],
        "keywords": ["trigger dag", "run dag", "execute dag", "start dag run", "kick off dag", "launch dag"]
    },
    "list_dag_runs": {
        "description": "List all runs for a DAG",
        "method": "list_dag_runs",
        "params": ["dag_id", "limit", "offset", "state", "order_by"],
        "required": ["dag_id"],
        "keywords": ["list dag runs", "dag runs", "show runs", "get runs", "execution history"]
    },
    "get_dag_run": {
        "description": "Get details of a specific DAG run",
        "method": "get_dag_run",
        "params": ["dag_id", "dag_run_id"],
        "required": ["dag_id", "dag_run_id"],
        "keywords": ["get dag run", "dag run details", "run status", "run info"]
    },
    "update_dag_run_state": {
        "description": "Update the state of a DAG run (mark as success/failed)",
        "method": "update_dag_run_state",
        "params": ["dag_id", "dag_run_id", "state"],
        "required": ["dag_id", "dag_run_id", "state"],
        "keywords": ["update dag run", "mark dag run", "set dag run state", "change run status"]
    },
    "delete_dag_run": {
        "description": "Delete a DAG run",
        "method": "delete_dag_run",
        "params": ["dag_id", "dag_run_id"],
        "required": ["dag_id", "dag_run_id"],
        "keywords": ["delete dag run", "remove dag run", "clear dag run"]
    },
    "clear_dag_run": {
        "description": "Clear all task instances in a DAG run for re-execution",
        "method": "clear_dag_run",
        "params": ["dag_id", "dag_run_id", "dry_run"],
        "required": ["dag_id", "dag_run_id"],
        "keywords": ["clear dag run", "reset dag run", "retry dag run", "rerun dag"]
    },
    "list_task_instances": {
        "description": "List all task instances for a DAG run",
        "method": "list_task_instances",
        "params": ["dag_id", "dag_run_id", "limit", "offset"],
        "required": ["dag_id", "dag_run_id"],
        "keywords": ["list task instances", "task instances", "tasks in run", "show task status"]
    },
    "get_task_instance": {
        "description": "Get details of a specific task instance",
        "method": "get_task_instance",
        "params": ["dag_id", "dag_run_id", "task_id"],
        "required": ["dag_id", "dag_run_id", "task_id"],
        "keywords": ["get task instance", "task status", "task details", "task info"]
    },
    "update_task_instance_state": {
        "description": "Update the state of a task instance",
        "method": "update_task_instance_state",
        "params": ["dag_id", "dag_run_id", "task_id", "new_state"],
        "required": ["dag_id", "dag_run_id", "task_id", "new_state"],
        "keywords": ["update task", "mark task", "set task state", "change task status"]
    },
    "get_task_logs": {
        "description": "Get logs for a task instance",
        "method": "get_task_logs",
        "params": ["dag_id", "dag_run_id", "task_id", "task_try_number", "full_content"],
        "required": ["dag_id", "dag_run_id", "task_id"],
        "keywords": ["get task logs", "task logs", "show logs", "view logs", "read logs"]
    },
    "clear_task_instances": {
        "description": "Clear task instances for re-execution",
        "method": "clear_task_instances",
        "params": ["dag_id", "dag_run_id", "task_ids", "start_date", "end_date", "include_downstream", "include_future", "include_past", "only_failed", "only_running", "dry_run"],
        "required": ["dag_id"],
        "keywords": ["clear tasks", "reset tasks", "retry tasks", "rerun tasks", "clear failed tasks"]
    },
    "list_connections": {
        "description": "List all Airflow connections",
        "method": "list_connections",
        "params": ["limit", "offset"],
        "keywords": ["list connections", "show connections", "get connections", "all connections"]
    },
    "get_connection": {
        "description": "Get details of a specific connection",
        "method": "get_connection",
        "params": ["connection_id"],
        "required": ["connection_id"],
        "keywords": ["get connection", "connection details", "connection info"]
    },
    "create_connection": {
        "description": "Create a new Airflow connection",
        "method": "create_connection",
        "params": ["connection_id", "conn_type", "host", "login", "password", "port", "schema", "extra", "description"],
        "required": ["connection_id", "conn_type"],
        "keywords": ["create connection", "add connection", "new connection"]
    },
    "update_connection": {
        "description": "Update an existing connection",
        "method": "update_connection",
        "params": ["connection_id"],
        "required": ["connection_id"],
        "keywords": ["update connection", "modify connection", "edit connection"]
    },
    "delete_connection": {
        "description": "Delete a connection",
        "method": "delete_connection",
        "params": ["connection_id"],
        "required": ["connection_id"],
        "keywords": ["delete connection", "remove connection"]
    },
    "list_pools": {
        "description": "List all Airflow pools",
        "method": "list_pools",
        "params": ["limit", "offset"],
        "keywords": ["list pools", "show pools", "get pools", "all pools"]
    },
    "get_pool": {
        "description": "Get details of a specific pool",
        "method": "get_pool",
        "params": ["pool_name"],
        "required": ["pool_name"],
        "keywords": ["get pool", "pool details", "pool info"]
    },
    "create_pool": {
        "description": "Create a new pool",
        "method": "create_pool",
        "params": ["name", "slots", "description"],
        "required": ["name", "slots"],
        "keywords": ["create pool", "add pool", "new pool"]
    },
    "update_pool": {
        "description": "Update an existing pool",
        "method": "update_pool",
        "params": ["pool_name", "slots", "description"],
        "required": ["pool_name"],
        "keywords": ["update pool", "modify pool", "change pool slots"]
    },
    "delete_pool": {
        "description": "Delete a pool",
        "method": "delete_pool",
        "params": ["pool_name"],
        "required": ["pool_name"],
        "keywords": ["delete pool", "remove pool"]
    },
    "list_variables": {
        "description": "List all Airflow variables",
        "method": "list_variables",
        "params": ["limit", "offset"],
        "keywords": ["list variables", "show variables", "get variables", "all variables"]
    },
    "get_variable": {
        "description": "Get a specific variable value",
        "method": "get_variable",
        "params": ["variable_key"],
        "required": ["variable_key"],
        "keywords": ["get variable", "variable value", "read variable"]
    },
    "create_variable": {
        "description": "Create a new variable",
        "method": "create_variable",
        "params": ["key", "value", "description"],
        "required": ["key", "value"],
        "keywords": ["create variable", "set variable", "add variable", "new variable"]
    },
    "update_variable": {
        "description": "Update an existing variable",
        "method": "update_variable",
        "params": ["variable_key", "value", "description"],
        "required": ["variable_key", "value"],
        "keywords": ["update variable", "modify variable", "change variable"]
    },
    "delete_variable": {
        "description": "Delete a variable",
        "method": "delete_variable",
        "params": ["variable_key"],
        "required": ["variable_key"],
        "keywords": ["delete variable", "remove variable"]
    },
    "list_xcoms": {
        "description": "List XCom entries for a task instance",
        "method": "list_xcoms",
        "params": ["dag_id", "dag_run_id", "task_id", "limit", "offset"],
        "required": ["dag_id", "dag_run_id", "task_id"],
        "keywords": ["list xcoms", "show xcoms", "get xcoms", "xcom entries"]
    },
    "get_xcom": {
        "description": "Get a specific XCom value",
        "method": "get_xcom",
        "params": ["dag_id", "dag_run_id", "task_id", "xcom_key"],
        "required": ["dag_id", "dag_run_id", "task_id", "xcom_key"],
        "keywords": ["get xcom", "xcom value", "read xcom"]
    },
    "list_event_logs": {
        "description": "List event logs from Airflow",
        "method": "list_event_logs",
        "params": ["limit", "offset", "dag_id", "event", "owner"],
        "keywords": ["list event logs", "event logs", "audit logs", "show logs"]
    },
    "list_import_errors": {
        "description": "List DAG import/parsing errors",
        "method": "list_import_errors",
        "params": ["limit", "offset"],
        "keywords": ["import errors", "parsing errors", "dag errors", "broken dags"]
    },
    "get_health": {
        "description": "Check Airflow health status",
        "method": "get_health",
        "params": [],
        "keywords": ["health check", "airflow status", "is airflow running", "airflow health"]
    },
    "get_version": {
        "description": "Get Airflow version",
        "method": "get_version",
        "params": [],
        "keywords": ["airflow version", "version", "what version"]
    },
    "get_config": {
        "description": "Get Airflow configuration",
        "method": "get_config",
        "params": [],
        "keywords": ["airflow config", "configuration", "settings"]
    },
    "list_plugins": {
        "description": "List installed Airflow plugins",
        "method": "list_plugins",
        "params": [],
        "keywords": ["list plugins", "plugins", "installed plugins"]
    },
    "list_providers": {
        "description": "List installed provider packages",
        "method": "list_providers",
        "params": [],
        "keywords": ["list providers", "providers", "provider packages"]
    },
    "list_dag_warnings": {
        "description": "List DAG warnings",
        "method": "list_dag_warnings",
        "params": ["dag_id", "limit", "offset"],
        "keywords": ["dag warnings", "warnings", "dag issues"]
    }
}

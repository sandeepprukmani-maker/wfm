import base64
import time
import requests
from datetime import datetime
from typing import Optional, Dict, List, Any, Tuple

class AirflowAPI:
    """Comprehensive Airflow 2.7.3 REST API wrapper for DAG operations."""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.headers = {
            'Authorization': f'Basic {self.auth}',
            'Content-Type': 'application/json'
        }
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make an authenticated request to the Airflow API."""
        url = f"{self.base_url}/api/v1{endpoint}"
        kwargs['headers'] = {**self.headers, **kwargs.get('headers', {})}
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        if response.content:
            return response.json()
        return {}
    
    def list_dags(self, limit: int = 100, offset: int = 0, tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """List all DAGs with optional filtering."""
        params = {'limit': limit, 'offset': offset}
        if tags:
            params['tags'] = ','.join(tags)
        return self._request('GET', '/dags', params=params)
    
    def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """Get details of a specific DAG."""
        return self._request('GET', f'/dags/{dag_id}')
    
    def pause_dag(self, dag_id: str) -> Dict[str, Any]:
        """Pause a DAG."""
        return self._request('PATCH', f'/dags/{dag_id}', 
                           params={'update_mask': 'is_paused'},
                           json={'is_paused': True})
    
    def unpause_dag(self, dag_id: str) -> Dict[str, Any]:
        """Unpause a DAG."""
        return self._request('PATCH', f'/dags/{dag_id}',
                           params={'update_mask': 'is_paused'},
                           json={'is_paused': False})
    
    def delete_dag(self, dag_id: str) -> Dict[str, Any]:
        """Delete a DAG and all associated metadata."""
        return self._request('DELETE', f'/dags/{dag_id}')
    
    def get_dag_source(self, file_token: str) -> str:
        """Get the source code of a DAG file."""
        response = self._request('GET', f'/dagSources/{file_token}')
        return response.get('content', '')
    
    def list_dag_runs(self, dag_id: str, limit: int = 100, offset: int = 0, 
                      state: Optional[str] = None, execution_date_gte: Optional[str] = None,
                      execution_date_lte: Optional[str] = None) -> Dict[str, Any]:
        """List DAG runs with optional filtering."""
        params = {'limit': limit, 'offset': offset}
        if state:
            params['state'] = state
        if execution_date_gte:
            params['execution_date_gte'] = execution_date_gte
        if execution_date_lte:
            params['execution_date_lte'] = execution_date_lte
        return self._request('GET', f'/dags/{dag_id}/dagRuns', params=params)
    
    def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Get details of a specific DAG run."""
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}')
    
    def trigger_dag(self, dag_id: str, conf: Optional[Dict] = None, 
                    dag_run_id: Optional[str] = None,
                    logical_date: Optional[str] = None) -> Dict[str, Any]:
        """Trigger a new DAG run."""
        payload = {}
        if conf:
            payload['conf'] = conf
        if dag_run_id:
            payload['dag_run_id'] = dag_run_id
        if logical_date:
            payload['logical_date'] = logical_date
        return self._request('POST', f'/dags/{dag_id}/dagRuns', json=payload)
    
    def update_dag_run_state(self, dag_id: str, dag_run_id: str, state: str) -> Dict[str, Any]:
        """Update the state of a DAG run (e.g., mark as failed)."""
        return self._request('PATCH', f'/dags/{dag_id}/dagRuns/{dag_run_id}',
                           json={'state': state})
    
    def delete_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Delete a specific DAG run."""
        return self._request('DELETE', f'/dags/{dag_id}/dagRuns/{dag_run_id}')
    
    def list_task_instances(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """List all task instances for a DAG run."""
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances')
    
    def get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str) -> Dict[str, Any]:
        """Get details of a specific task instance."""
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}')
    
    def get_task_logs(self, dag_id: str, dag_run_id: str, task_id: str, 
                      try_number: int = 1) -> str:
        """Get logs for a specific task instance."""
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.text
    
    def clear_task_instances(self, dag_id: str, task_ids: Optional[List[str]] = None,
                            start_date: Optional[str] = None, end_date: Optional[str] = None,
                            only_failed: bool = False, reset_dag_runs: bool = True,
                            dry_run: bool = False) -> Dict[str, Any]:
        """Clear task instances to allow re-running."""
        payload = {
            'dry_run': dry_run,
            'only_failed': only_failed,
            'reset_dag_runs': reset_dag_runs
        }
        if task_ids:
            payload['task_ids'] = task_ids
        if start_date:
            payload['start_date'] = start_date
        if end_date:
            payload['end_date'] = end_date
        return self._request('POST', f'/dags/{dag_id}/clearTaskInstances', json=payload)
    
    def set_task_instance_state(self, dag_id: str, dag_run_id: str, task_id: str, 
                                state: str) -> Dict[str, Any]:
        """Set the state of a task instance."""
        return self._request('PATCH', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}',
                           json={'state': state})
    
    def get_variables(self) -> Dict[str, Any]:
        """List all Airflow variables."""
        return self._request('GET', '/variables')
    
    def get_variable(self, key: str) -> Dict[str, Any]:
        """Get a specific Airflow variable."""
        return self._request('GET', f'/variables/{key}')
    
    def create_variable(self, key: str, value: str, description: Optional[str] = None) -> Dict[str, Any]:
        """Create a new Airflow variable."""
        payload = {'key': key, 'value': value}
        if description:
            payload['description'] = description
        return self._request('POST', '/variables', json=payload)
    
    def update_variable(self, key: str, value: str, description: Optional[str] = None) -> Dict[str, Any]:
        """Update an existing Airflow variable."""
        payload = {'key': key, 'value': value}
        if description:
            payload['description'] = description
        return self._request('PATCH', f'/variables/{key}', json=payload)
    
    def delete_variable(self, key: str) -> Dict[str, Any]:
        """Delete an Airflow variable."""
        return self._request('DELETE', f'/variables/{key}')
    
    def get_connections(self) -> Dict[str, Any]:
        """List all Airflow connections."""
        return self._request('GET', '/connections')
    
    def get_connection(self, connection_id: str) -> Dict[str, Any]:
        """Get a specific Airflow connection."""
        return self._request('GET', f'/connections/{connection_id}')
    
    def create_connection(self, connection_id: str, conn_type: str, 
                         host: Optional[str] = None, port: Optional[int] = None,
                         login: Optional[str] = None, password: Optional[str] = None,
                         schema: Optional[str] = None, extra: Optional[str] = None) -> Dict[str, Any]:
        """Create a new Airflow connection."""
        payload = {'connection_id': connection_id, 'conn_type': conn_type}
        if host: payload['host'] = host
        if port: payload['port'] = port
        if login: payload['login'] = login
        if password: payload['password'] = password
        if schema: payload['schema'] = schema
        if extra: payload['extra'] = extra
        return self._request('POST', '/connections', json=payload)
    
    def delete_connection(self, connection_id: str) -> Dict[str, Any]:
        """Delete an Airflow connection."""
        return self._request('DELETE', f'/connections/{connection_id}')
    
    def get_pools(self) -> Dict[str, Any]:
        """List all Airflow pools."""
        return self._request('GET', '/pools')
    
    def get_pool(self, pool_name: str) -> Dict[str, Any]:
        """Get a specific Airflow pool."""
        return self._request('GET', f'/pools/{pool_name}')
    
    def create_pool(self, name: str, slots: int, description: Optional[str] = None) -> Dict[str, Any]:
        """Create a new Airflow pool."""
        payload = {'name': name, 'slots': slots}
        if description:
            payload['description'] = description
        return self._request('POST', '/pools', json=payload)
    
    def update_pool(self, name: str, slots: int, description: Optional[str] = None) -> Dict[str, Any]:
        """Update an existing Airflow pool."""
        payload = {'name': name, 'slots': slots}
        if description:
            payload['description'] = description
        return self._request('PATCH', f'/pools/{name}', json=payload)
    
    def delete_pool(self, pool_name: str) -> Dict[str, Any]:
        """Delete an Airflow pool."""
        return self._request('DELETE', f'/pools/{pool_name}')
    
    def get_xcom_entries(self, dag_id: str, dag_run_id: str, task_id: str) -> Dict[str, Any]:
        """Get XCom entries for a task instance."""
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries')
    
    def get_xcom_entry(self, dag_id: str, dag_run_id: str, task_id: str, xcom_key: str) -> Dict[str, Any]:
        """Get a specific XCom entry."""
        return self._request('GET', f'/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}')
    
    def get_import_errors(self) -> Dict[str, Any]:
        """List all DAG import errors."""
        return self._request('GET', '/importErrors')
    
    def get_health(self) -> Dict[str, Any]:
        """Get Airflow health status."""
        return self._request('GET', '/health')
    
    def get_dag_warnings(self, dag_id: Optional[str] = None) -> Dict[str, Any]:
        """Get DAG warnings."""
        params = {}
        if dag_id:
            params['dag_id'] = dag_id
        return self._request('GET', '/dagWarnings', params=params)
    
    def is_dag_running(self, dag_id: str) -> bool:
        """Check if a DAG has any running DAG runs."""
        try:
            runs = self.list_dag_runs(dag_id, state='running')
            return len(runs.get('dag_runs', [])) > 0
        except:
            return False
    
    def is_dag_paused(self, dag_id: str) -> bool:
        """Check if a DAG is paused."""
        try:
            dag = self.get_dag(dag_id)
            return dag.get('is_paused', False)
        except:
            return False
    
    def wait_for_dag_completion(self, dag_id: str, timeout_seconds: int = 3600, 
                                poll_interval: int = 10) -> Tuple[bool, str]:
        """Wait for all running DAG runs to complete."""
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            if not self.is_dag_running(dag_id):
                return True, "All runs completed"
            time.sleep(poll_interval)
        return False, f"Timeout after {timeout_seconds} seconds"
    
    def ensure_dag_paused_and_idle(self, dag_id: str, wait_timeout: int = 600,
                                   poll_interval: int = 10) -> Tuple[bool, List[str]]:
        """Ensure a DAG is paused and has no running instances."""
        messages = []
        
        if self.is_dag_running(dag_id):
            messages.append(f"DAG {dag_id} has running instances, waiting for completion...")
            success, msg = self.wait_for_dag_completion(dag_id, wait_timeout, poll_interval)
            if not success:
                messages.append(f"Failed to wait for DAG completion: {msg}")
                return False, messages
            messages.append(f"DAG {dag_id} runs completed")
        
        if not self.is_dag_paused(dag_id):
            messages.append(f"Pausing DAG {dag_id}...")
            self.pause_dag(dag_id)
            messages.append(f"DAG {dag_id} paused successfully")
        else:
            messages.append(f"DAG {dag_id} is already paused")
        
        return True, messages
    
    def prepare_dags_for_workflow(self, dag_ids: List[str], wait_timeout: int = 600,
                                  poll_interval: int = 10) -> Tuple[bool, List[str]]:
        """Prepare multiple DAGs for workflow execution by ensuring they are paused and idle."""
        all_messages = []
        all_success = True
        
        for dag_id in dag_ids:
            success, messages = self.ensure_dag_paused_and_idle(dag_id, wait_timeout, poll_interval)
            all_messages.extend(messages)
            if not success:
                all_success = False
                break
        
        return all_success, all_messages


def get_airflow_client(credential_data: Dict[str, Any]) -> Optional[AirflowAPI]:
    """Create an Airflow API client from credential data."""
    base_url = credential_data.get('baseUrl', '')
    username = credential_data.get('username', '')
    password = credential_data.get('password', '')
    
    if not base_url or not username or not password:
        return None
    
    return AirflowAPI(base_url, username, password)

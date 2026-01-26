import json
import sys

def export_to_python(workflow_json):
    """
    Exports a workflow JSON to a Python script using Airflow 2.x patterns.
    Enhanced to handle log assertions, conditional branching, and result collection.
    """
    data = json.loads(workflow_json)
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])
    
    output = [
        "from airflow import DAG",
        "from airflow.operators.python import PythonOperator, BranchPythonOperator",
        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator",
        "from airflow.providers.common.sql.operators.sql import SQLExecutor",
        "from airflow.operators.empty import EmptyOperator",
        "from airflow.utils.state import State",
        "from datetime import datetime",
        "import re",
        "",
        "def check_logs_func(**context):",
        "    # Real-time log retrieval using Airflow TaskInstance",
        "    from airflow.models import TaskInstance",
        "    from airflow.utils.session import create_session",
        "    ",
        "    params = context.get('params', {})",
        "    pattern = params.get('pattern', '')",
        "    success_nodes = params.get('success_nodes', [])",
        "    target_task_id = params.get('task_id', 'unknown')",
        "    dag_run = context['dag_run']",
        "    ",
        "    print(f'Fetching real-time logs for task {target_task_id} in DAG {dag_run.dag_id}')",
        "    ",
        "    # Retrieve the task instance for the specific task",
        "    ti = TaskInstance(context['dag'].get_task(target_task_id), run_id=dag_run.run_id)",
        "    ",
        "    # Try to fetch logs from the logger",
        "    try:",
        "        # In Airflow 2.x, logs can be retrieved via the log attribute or TaskInstance.log",
        "        # Note: This requires the task to have finished or at least started emitting logs",
        "        logs = ti.log.get_records() if hasattr(ti.log, 'get_records') else ''",
        "        if not logs:",
        "            # Fallback to standard log retrieval if possible",
        "            from airflow.utils.log.log_reader import TaskLogReader",
        "            reader = TaskLogReader()",
        "            logs, metadata = reader.read_log_chunks(ti, try_number=ti.try_number, metadata={})",
        "        ",
        "        print(f'Logs retrieved: {len(logs)} characters')",
        "        if re.search(pattern, logs):",
        "            print(f'Pattern \"{pattern}\" found in logs!')",
        "            return success_nodes",
        "    except Exception as e:",
        "        print(f'Error retrieving logs: {str(e)}')",
        "    ",
        "    return [] # Stop execution if pattern not found",
        "",
        "def collect_results(**context):",
        "    # Logic to collect and return DAG run IDs from XCom",
        "    dag_run_ids = {}",
        "    # This is a conceptual implementation",
        "    print('Collecting DAG Run IDs...')",
        "    return dag_run_ids",
        "",
        "with DAG('generated_workflow', start_date=datetime(2023, 1, 1), schedule=None) as dag:",
    ]
    
    node_map = {}
    branch_map = {} # Maps node_id to branch_task_id
    
    # Pre-map all task IDs
    for node in nodes:
        node_id = node["id"]
        safe_id = node_id.replace("-", "_")
        node_map[node_id] = f"task_{safe_id}"

    for node in nodes:
        node_id = node["id"]
        safe_id = node_id.replace("-", "_")
        node_type = node["data"]["type"]
        label = node["data"]["label"]
        config = node["data"]["config"]
        
        if node_type == "airflow_trigger":
            log_assertion = config.get('logAssertion')
            task_name_to_check = config.get('taskName', 'unknown_task')
            if log_assertion:
                output.append(f"    task_{safe_id} = TriggerDagRunOperator(task_id='{label}', trigger_dag_id='{config.get('dagId')}', wait_for_completion=True)")
                
                # Success nodes are those directly connected to this trigger
                success_targets = [node_map[e["target"]] for e in edges if e["source"] == node_id]
                
                output.append(f"    branch_{safe_id} = BranchPythonOperator(")
                output.append(f"        task_id='check_logs_{safe_id}',")
                output.append(f"        python_callable=check_logs_func,")
                output.append(f"        params={{'pattern': '{log_assertion}', 'task_id': '{task_name_to_check}', 'success_nodes': {success_targets}}}")
                output.append(f"    )")
                branch_map[node_id] = f"branch_{safe_id}"
            else:
                output.append(f"    task_{safe_id} = TriggerDagRunOperator(task_id='{label}', trigger_dag_id='{config.get('dagId')}', wait_for_completion=True)")
        
        elif node_type == "sql_query":
            output.append(f"    def sql_exec_{safe_id}(**context):")
            output.append(f"        import pymssql")
            output.append(f"        # Credential lookup would happen here in a real implementation")
            output.append(f"        conn = pymssql.connect(server='your_server', user='your_user', password='your_password', database='your_db')")
            output.append(f"        cursor = conn.cursor()")
            output.append(f"        query = \"{config.get('query')}\"")
            output.append(f"        cursor.execute(query)")
            output.append(f"        results = cursor.fetchall()")
            output.append(f"        conn.close()")
            output.append(f"        return results")
            output.append(f"    ")
            output.append(f"    task_{safe_id} = PythonOperator(task_id='{label}', python_callable=sql_exec_{safe_id})")
        
        elif node_type == "python_script":
            output.append(f"    task_{safe_id} = PythonOperator(task_id='{label}', python_callable=collect_results)")
            
        elif node_type == "airflow_sla_check":
            output.append(f"    task_{safe_id} = EmptyOperator(task_id='{label}')  # SLA Check placeholder")

        elif node_type == "airflow_var_mgmt":
            output.append(f"    task_{safe_id} = PythonOperator(task_id='{label}', python_callable=collect_results)  # Var Mgmt placeholder")

        elif node_type == "api_request":
            output.append(f"    task_{safe_id} = PythonOperator(task_id='{label}', python_callable=collect_results)  # API Request placeholder")
            
        elif node_type == "condition":
            output.append(f"    task_{safe_id} = EmptyOperator(task_id='{label}')")
            
        else:
            output.append(f"    task_{safe_id} = EmptyOperator(task_id='{label}')")

    # Define dependencies
    for node_id, task_name in node_map.items():
        targets = [e["target"] for e in edges if e["source"] == node_id]
        if not targets:
            continue
            
        if node_id in branch_map:
            branch_task = branch_map[node_id]
            target_list = ", ".join([node_map[t] for t in targets])
            output.append(f"    {task_name} >> {branch_task} >> [{target_list}]")
        else:
            for t in targets:
                output.append(f"    {task_name} >> {node_map[t]}")
                
    return "\n".join(output)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r') as f:
            print(export_to_python(f.read()))

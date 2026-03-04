# This script replaces the Airflow section in tool_registry.py

import re

with open('/home/claude/flowforge_clean/backend/mcp_servers/tool_registry.py', 'r') as f:
    src = f.read()

NEW_AIRFLOW_DEFS = '''
    # ── Airflow 2.7.3 — Full REST API coverage ────────────────────────────────

    # Health & Version
    {"name":"airflow_health_check","description":"Check if Airflow is reachable and healthy. Returns scheduler/metadb status.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"}},"required":["credential"]}},
    {"name":"airflow_get_version","description":"Get the Airflow version and Git commit hash.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"}},"required":["credential"]}},

    # DAGs
    {"name":"airflow_list_dags","description":"List all DAGs. Supports filtering by tags, paused state, and dag_id pattern.",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},
         "limit":{"type":"integer","default":100},
         "offset":{"type":"integer","default":0},
         "only_active":{"type":"boolean","default":False},
         "paused":{"type":"boolean","description":"Filter by paused state"},
         "dag_id_pattern":{"type":"string","description":"Substring filter on dag_id"},
         "order_by":{"type":"string","default":"dag_id"}},"required":["credential"]}},
    {"name":"airflow_get_dag","description":"Get metadata for a specific DAG: schedule, tags, owners, paused state, file location.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"}},"required":["credential","dag_id"]}},
    {"name":"airflow_get_dag_details","description":"Get extended DAG details including concurrency, catchup, and doc_md.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"}},"required":["credential","dag_id"]}},
    {"name":"airflow_get_dag_source","description":"Retrieve raw Python source code of a DAG file using its file_token.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"file_token":{"type":"string","description":"The file_token field from get_dag response"}},"required":["credential","file_token"]}},
    {"name":"airflow_get_dag_tags","description":"List all DAG tags defined across the Airflow instance.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"}},"required":["credential"]}},
    {"name":"airflow_pause_dag","description":"Pause a DAG so it stops being scheduled.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"}},"required":["credential","dag_id"]}},
    {"name":"airflow_unpause_dag","description":"Unpause a DAG to resume scheduled runs.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"}},"required":["credential","dag_id"]}},
    {"name":"airflow_delete_dag","description":"Delete a DAG and all its runs and metadata. Irreversible.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"}},"required":["credential","dag_id"]}},

    # DAG Runs
    {"name":"airflow_trigger_dag","description":"Trigger a new DAG run. Optionally pass a conf dict and logical_date.",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},"dag_id":{"type":"string"},
         "conf":{"type":"object","description":"Config dict passed to DAG run context"},
         "logical_date":{"type":"string","description":"ISO8601 execution date"},
         "run_id":{"type":"string","description":"Custom dag_run_id"},
         "note":{"type":"string"}},"required":["credential","dag_id"]}},
    {"name":"airflow_trigger_and_wait","description":"Trigger a DAG and wait (poll) until it reaches success/failed/skipped. Returns final state.",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},"dag_id":{"type":"string"},
         "conf":{"type":"object"},"timeout":{"type":"integer","default":3600},
         "poll_interval":{"type":"integer","default":10}},"required":["credential","dag_id"]}},
    {"name":"airflow_list_dag_runs","description":"List runs for a DAG with optional state filter and date range.",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},"dag_id":{"type":"string"},
         "limit":{"type":"integer","default":25},"offset":{"type":"integer","default":0},
         "state":{"type":"string","description":"queued|running|success|failed"},
         "order_by":{"type":"string","default":"-execution_date"},
         "start_date_gte":{"type":"string"},"start_date_lte":{"type":"string"}},"required":["credential","dag_id"]}},
    {"name":"airflow_list_dag_runs_batch","description":"Query runs across multiple DAGs in one call.",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},
         "dag_ids":{"type":"array","items":{"type":"string"}},
         "states":{"type":"array","items":{"type":"string"}},
         "execution_date_gte":{"type":"string"},"execution_date_lte":{"type":"string"},
         "page_limit":{"type":"integer","default":100},"order_by":{"type":"string","default":"-execution_date"}},"required":["credential"]}},
    {"name":"airflow_get_run_status","description":"Get the current state of a specific DAG run.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"}},"required":["credential","dag_id","dag_run_id"]}},
    {"name":"airflow_update_dag_run_state","description":"Force set a DAG run state to queued, success, or failed.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"},"state":{"type":"string","description":"queued|success|failed"},"note":{"type":"string"}},"required":["credential","dag_id","dag_run_id","state"]}},
    {"name":"airflow_delete_dag_run","description":"Delete a DAG run record permanently.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"}},"required":["credential","dag_id","dag_run_id"]}},
    {"name":"airflow_clear_dag_run","description":"Clear (re-queue) a DAG run, optionally only failed/running tasks.",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"},
         "dry_run":{"type":"boolean","default":True},"only_failed":{"type":"boolean","default":False},
         "only_running":{"type":"boolean","default":False},"reset_dag_runs":{"type":"boolean","default":False},
         "include_upstream":{"type":"boolean","default":False},"include_downstream":{"type":"boolean","default":False}},"required":["credential","dag_id","dag_run_id"]}},
    {"name":"airflow_list_recent_runs","description":"Shortcut: list the N most recent runs for a DAG.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"limit":{"type":"integer","default":10}},"required":["credential","dag_id"]}},

    # Tasks
    {"name":"airflow_list_tasks","description":"List all tasks defined in a DAG with their operator types and dependencies.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"}},"required":["credential","dag_id"]}},
    {"name":"airflow_get_task","description":"Get the definition of a specific task in a DAG.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"task_id":{"type":"string"}},"required":["credential","dag_id","task_id"]}},
    {"name":"airflow_get_task_instances","description":"Get all task instances for a DAG run with their states and durations.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"}},"required":["credential","dag_id","dag_run_id"]}},
    {"name":"airflow_get_task_instance","description":"Get a single task instance with full detail.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"},"task_id":{"type":"string"}},"required":["credential","dag_id","dag_run_id","task_id"]}},
    {"name":"airflow_get_task_log","description":"Fetch execution logs for a specific Airflow task attempt.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"},"task_id":{"type":"string"},"try_number":{"type":"integer","default":1},"full_content":{"type":"boolean","default":False}},"required":["credential","dag_id","dag_run_id","task_id"]}},
    {"name":"airflow_clear_task_instances","description":"Clear (re-queue) task instances in a run, optionally only failed ones.",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"},
         "task_ids":{"type":"array","items":{"type":"string"}},
         "dry_run":{"type":"boolean","default":True},"only_failed":{"type":"boolean","default":False},
         "include_downstream":{"type":"boolean","default":False}},"required":["credential","dag_id"]}},
    {"name":"airflow_set_task_instance_state","description":"Override the state of task instances (e.g. mark as success/failed/skipped).",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},"dag_id":{"type":"string"},"task_id":{"type":"string"},
         "execution_date":{"type":"string"},"state":{"type":"string","description":"success|failed|skipped|up_for_retry"},
         "dry_run":{"type":"boolean","default":True},"include_upstream":{"type":"boolean","default":False},
         "include_downstream":{"type":"boolean","default":False},"include_future":{"type":"boolean","default":False},
         "include_past":{"type":"boolean","default":False}},"required":["credential","dag_id","task_id","execution_date","state"]}},
    {"name":"airflow_list_task_instances_batch","description":"Query task instances across multiple DAGs/runs in one call.",
     "input_schema":{"type":"object","properties":{
         "credential":{"type":"string"},
         "dag_ids":{"type":"array","items":{"type":"string"}},
         "dag_run_ids":{"type":"array","items":{"type":"string"}},
         "task_ids":{"type":"array","items":{"type":"string"}},
         "state":{"type":"array","items":{"type":"string"}}},"required":["credential"]}},
    {"name":"airflow_get_mapped_task_instances","description":"List all mapped/dynamic task instances for a mapped operator.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"},"task_id":{"type":"string"},"limit":{"type":"integer","default":100}},"required":["credential","dag_id","dag_run_id","task_id"]}},

    # XCom
    {"name":"airflow_list_xcom_entries","description":"List XCom entries pushed by a task instance.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"},"task_id":{"type":"string"},"limit":{"type":"integer","default":25}},"required":["credential","dag_id","dag_run_id","task_id"]}},
    {"name":"airflow_get_xcom_entry","description":"Get a specific XCom value by key from a task instance.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string"},"dag_run_id":{"type":"string"},"task_id":{"type":"string"},"xcom_key":{"type":"string"}},"required":["credential","dag_id","dag_run_id","task_id","xcom_key"]}},

    # Variables
    {"name":"airflow_list_variables","description":"List all Airflow variables (keys only, values masked for sensitive ones).",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"limit":{"type":"integer","default":100}},"required":["credential"]}},
    {"name":"airflow_get_variable","description":"Get the value of a specific Airflow variable.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"key":{"type":"string"}},"required":["credential","key"]}},
    {"name":"airflow_create_variable","description":"Create a new Airflow variable.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"key":{"type":"string"},"value":{"type":"string"},"description":{"type":"string"}},"required":["credential","key","value"]}},
    {"name":"airflow_update_variable","description":"Update an existing Airflow variable value.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"key":{"type":"string"},"value":{"type":"string"}},"required":["credential","key","value"]}},
    {"name":"airflow_delete_variable","description":"Delete an Airflow variable.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"key":{"type":"string"}},"required":["credential","key"]}},

    # Pools
    {"name":"airflow_list_pools","description":"List all Airflow worker pools with slot counts.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"}},"required":["credential"]}},
    {"name":"airflow_get_pool","description":"Get a specific pool and its available/occupied slot counts.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"pool_name":{"type":"string"}},"required":["credential","pool_name"]}},
    {"name":"airflow_create_pool","description":"Create a new Airflow pool.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"name":{"type":"string"},"slots":{"type":"integer"},"description":{"type":"string"}},"required":["credential","name","slots"]}},
    {"name":"airflow_update_pool","description":"Update slot count or description of an existing pool.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"name":{"type":"string"},"slots":{"type":"integer"},"description":{"type":"string"}},"required":["credential","name"]}},
    {"name":"airflow_delete_pool","description":"Delete an Airflow pool.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"pool_name":{"type":"string"}},"required":["credential","pool_name"]}},

    # Connections
    {"name":"airflow_list_connections","description":"List all Airflow connections (passwords redacted).",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"limit":{"type":"integer","default":100}},"required":["credential"]}},
    {"name":"airflow_get_connection","description":"Get a specific Airflow connection definition.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"conn_id":{"type":"string"}},"required":["credential","conn_id"]}},
    {"name":"airflow_create_connection","description":"Create a new Airflow connection.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"conn_id":{"type":"string"},"conn_type":{"type":"string"},"host":{"type":"string"},"schema":{"type":"string"},"login":{"type":"string"},"password":{"type":"string"},"port":{"type":"integer"},"extra":{"type":"object"}},"required":["credential","conn_id","conn_type"]}},
    {"name":"airflow_update_connection","description":"Update an existing Airflow connection.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"conn_id":{"type":"string"},"conn_type":{"type":"string"},"host":{"type":"string"},"login":{"type":"string"},"password":{"type":"string"},"port":{"type":"integer"}},"required":["credential","conn_id"]}},
    {"name":"airflow_delete_connection","description":"Delete an Airflow connection.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"conn_id":{"type":"string"}},"required":["credential","conn_id"]}},
    {"name":"airflow_test_connection","description":"Test whether an Airflow connection is reachable.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"conn_id":{"type":"string"}},"required":["credential","conn_id"]}},

    # Datasets
    {"name":"airflow_list_datasets","description":"List all Airflow datasets (data-aware scheduling).",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"uri_pattern":{"type":"string"},"limit":{"type":"integer","default":100}},"required":["credential"]}},
    {"name":"airflow_get_dataset","description":"Get a specific dataset by URI.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"uri":{"type":"string"}},"required":["credential","uri"]}},
    {"name":"airflow_list_dataset_events","description":"List dataset update events, optionally filtered by source DAG/task.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"source_dag_id":{"type":"string"},"source_task_id":{"type":"string"},"limit":{"type":"integer","default":25}},"required":["credential"]}},

    # Import errors & warnings
    {"name":"airflow_list_import_errors","description":"List DAG import errors (syntax/import failures) in Airflow.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"limit":{"type":"integer","default":50}},"required":["credential"]}},
    {"name":"airflow_list_dag_warnings","description":"List validation warnings for DAGs.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"},"dag_id":{"type":"string","description":"Optional: filter by dag_id"}},"required":["credential"]}},

    # Providers & Plugins
    {"name":"airflow_list_providers","description":"List all installed Airflow provider packages with versions.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"}},"required":["credential"]}},
    {"name":"airflow_list_plugins","description":"List all Airflow plugins loaded from the plugins folder.",
     "input_schema":{"type":"object","properties":{"credential":{"type":"string"}},"required":["credential"]}},
'''

# Replace old Airflow section
old_start = '    # ── Airflow ───────'
old_end   = '    # ── SQL ───────'

i1 = src.find(old_start)
i2 = src.find(old_end)
if i1 == -1 or i2 == -1:
    print("ERROR: could not find airflow section markers")
    print(f"i1={i1} i2={i2}")
else:
    src = src[:i1] + NEW_AIRFLOW_DEFS + "\n    " + src[i2:]
    print(f"Replaced airflow section ({i2-i1} chars old → {len(NEW_AIRFLOW_DEFS)} chars new)")

with open('/home/claude/flowforge_clean/backend/mcp_servers/tool_registry.py', 'w') as f:
    f.write(src)
print("Saved tool_registry.py")

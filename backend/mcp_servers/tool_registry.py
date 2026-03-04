"""
FlowForge — MCP Tool Registry

Single source of truth for:
  1. TOOL_DEFINITIONS — Anthropic tool_use format, passed to Claude API
  2. execute_tool()   — inline executor called by the chat router when Claude
                        uses a tool mid-conversation

The chat router does NOT spawn subprocesses. It instantiates connector classes
directly (same pattern as the workflow engine) so tools execute in-process.

Credentials are looked up from the database for the current user via
CredentialManager, falling back to environment variables for server-mode use.
"""

import os
import sys
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── All tool definitions in Anthropic tool_use format ─────────────────────────

TOOL_DEFINITIONS = [


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

        # ── SQL ───────────────────────────────────────────────────────────────────
    {
        "name": "sql_execute_query",
        "description": (
            "Execute a SQL SELECT query against the database and return real results. "
            "Use this whenever the user asks a data question — NEVER guess at data values. "
            "Always run the query to get the actual answer."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string", "description": "Credential name (e.g. PROD_MSSQL)"},
                "query":      {"type": "string", "description": "Valid SQL SELECT statement"},
                "max_rows":   {"type": "integer", "default": 100},
            },
            "required": ["credential", "query"],
        },
    },
    {
        "name": "sql_get_row_count",
        "description": "Get the row count from a table or COUNT(*) query. Use when user asks how many rows are in a table.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "query":      {"type": "string", "description": "Table name or SELECT COUNT(*) query"},
            },
            "required": ["credential", "query"],
        },
    },
    {
        "name": "sql_list_tables",
        "description": "List all tables in a database schema. Use when user asks what tables exist.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "schema":     {"type": "string", "default": "dbo"},
            },
            "required": ["credential"],
        },
    },
    {
        "name": "sql_describe_table",
        "description": "Get the column definitions for a table. Use when user asks about table structure or schema.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "table_name": {"type": "string"},
            },
            "required": ["credential", "table_name"],
        },
    },

    # ── HTTP ──────────────────────────────────────────────────────────────────
    {
        "name": "http_get",
        "description": "Make an HTTP GET request to a URL and return the response. Use when user asks about API health or wants to fetch live data from an endpoint.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url":        {"type": "string"},
                "credential": {"type": "string", "description": "Optional credential for auth headers"},
                "params":     {"type": "object", "description": "Optional query parameters"},
            },
            "required": ["url"],
        },
    },
    {
        "name": "http_health_check",
        "description": "Check if an HTTP endpoint is reachable and returns a healthy response.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url":        {"type": "string"},
                "credential": {"type": "string"},
            },
            "required": ["url"],
        },
    },

    # ── S3 ────────────────────────────────────────────────────────────────────
    {
        "name": "s3_list_objects",
        "description": "List objects in an S3 bucket. Use when user asks what files are in S3 or wants to check recent uploads.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential":  {"type": "string"},
                "bucket":      {"type": "string"},
                "prefix":      {"type": "string", "default": ""},
                "max_results": {"type": "integer", "default": 50},
            },
            "required": ["credential", "bucket"],
        },
    },
    {
        "name": "s3_object_exists",
        "description": "Check whether a specific file exists in S3.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "bucket":     {"type": "string"},
                "key":        {"type": "string"},
            },
            "required": ["credential", "bucket", "key"],
        },
    },

    # ── Azure ─────────────────────────────────────────────────────────────────
    {
        "name": "azure_list_blobs",
        "description": "List blobs in an Azure Blob Storage container.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "container":  {"type": "string"},
                "prefix":     {"type": "string", "default": ""},
            },
            "required": ["credential", "container"],
        },
    },
    {
        "name": "azure_blob_exists",
        "description": "Check if a specific Azure Blob exists.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "container":  {"type": "string"},
                "blob_name":  {"type": "string"},
            },
            "required": ["credential", "container", "blob_name"],
        },
    },

    # ── SFTP ──────────────────────────────────────────────────────────────────
    {
        "name": "sftp_list_directory",
        "description": "List files in a remote SFTP directory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential":  {"type": "string"},
                "remote_path": {"type": "string", "default": "/"},
            },
            "required": ["credential"],
        },
    },
    {
        "name": "sftp_file_exists",
        "description": "Check if a file exists on the SFTP server.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential":  {"type": "string"},
                "remote_path": {"type": "string"},
            },
            "required": ["credential", "remote_path"],
        },
    },
]

# Index by name for fast lookup
_TOOL_INDEX = {t["name"]: t for t in TOOL_DEFINITIONS}


def get_tool_definitions() -> list:
    """Return tool definitions in Anthropic API format."""
    return TOOL_DEFINITIONS


# ── Inline executor ───────────────────────────────────────────────────────────

def execute_tool(
    name: str,
    args: dict,
    credential_manager=None,
    owner_id: str = None,
) -> dict:
    """
    Execute a tool call inline (no subprocess).

    credential_manager and owner_id are optional — if provided, credentials are
    looked up from the database. Otherwise falls back to env vars (server mode).
    """
    logger.info(f"[MCP] execute_tool: {name}  args={list(args.keys())}")

    def _build(service_type: str, cred_name: str):
        """Build connector: DB credentials preferred, env vars fallback."""
        if credential_manager and owner_id and cred_name:
            try:
                return credential_manager.build_connector(cred_name, owner_id)
            except Exception as e:
                logger.warning(f"DB credential lookup failed ({cred_name}): {e}. Trying env vars.")
        # Env var fallback — used when running as standalone MCP server
        import importlib.util
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        if service_type == "airflow":
            from connectors.airflow_mcp import AirflowMCP
            return AirflowMCP(
                base_url=os.environ["AIRFLOW_BASE_URL"],
                username=os.environ.get("AIRFLOW_USERNAME", ""),
                password=os.environ.get("AIRFLOW_PASSWORD", ""),
            )
        elif service_type == "mssql":
            from connectors.mssql_mcp import MSSQLMCP
            cs = os.environ.get("MSSQL_CONNECTION_STRING")
            return MSSQLMCP(connection_string=cs) if cs else MSSQLMCP(
                server=os.environ["MSSQL_SERVER"],
                database=os.environ["MSSQL_DATABASE"],
                username=os.environ.get("MSSQL_USERNAME"),
                password=os.environ.get("MSSQL_PASSWORD"),
            )
        elif service_type == "http":
            from connectors.http_mcp import HTTPConnector
            return HTTPConnector(
                base_url=os.environ.get("HTTP_BASE_URL", ""),
                bearer_token=os.environ.get("HTTP_BEARER_TOKEN"),
                api_key=os.environ.get("HTTP_API_KEY"),
            )
        elif service_type == "s3":
            from connectors.s3_mcp import S3Connector
            return S3Connector(
                access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
                region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            )
        elif service_type == "azure":
            from connectors.azure_mcp import AzureConnector
            cs = os.environ.get("AZURE_CONNECTION_STRING")
            return AzureConnector(connection_string=cs) if cs else AzureConnector(
                account_name=os.environ["AZURE_ACCOUNT_NAME"],
                account_key=os.environ["AZURE_ACCOUNT_KEY"],
            )
        elif service_type == "sftp":
            from connectors.sftp_mcp import SFTPConnector
            return SFTPConnector(
                host=os.environ["SFTP_HOST"],
                username=os.environ["SFTP_USERNAME"],
                password=os.environ.get("SFTP_PASSWORD"),
                port=int(os.environ.get("SFTP_PORT", 22)),
            )
        raise ValueError(f"Unknown service type: {service_type}")

    cred = args.get("credential", "")

    try:
        # ── Airflow tools — full Airflow 2.7.3 REST API coverage ─────────────
        if name == "airflow_health_check":
            c = _build("airflow", cred)
            return c.health_check()

        elif name == "airflow_get_version":
            c = _build("airflow", cred)
            return c.get_version()

        # ── DAG Operations ────────────────────────────────────────────────────
        elif name == "airflow_list_dags":
            c = _build("airflow", cred)
            dags = c.list_dags(
                limit=int(args.get("limit", 50)),
                only_active=args.get("only_active", True),
                tags=args.get("tags"),
            )
            return {
                "dags": [
                    {"dag_id": d.get("dag_id"), "is_paused": d.get("is_paused"),
                     "schedule": d.get("schedule_interval"),
                     "owners": d.get("owners", []),
                     "tags": [t.get("name") for t in d.get("tags", [])]}
                    for d in dags
                ],
                "total": len(dags),
            }

        elif name == "airflow_get_dag":
            c = _build("airflow", cred)
            return c.get_dag(args["dag_id"])

        elif name == "airflow_get_dag_details":
            c = _build("airflow", cred)
            return c.get_dag_details(args["dag_id"])

        elif name == "airflow_get_dag_source":
            c = _build("airflow", cred)
            src = c.get_dag_source(args["file_token"])
            return {"source": src[:20000], "truncated": len(src) > 20000}

        elif name == "airflow_get_dag_tags":
            c = _build("airflow", cred)
            return c._get("/dagTags")

        elif name == "airflow_pause_dag":
            c = _build("airflow", cred)
            r = c.pause_dag(args["dag_id"])
            return {"dag_id": args["dag_id"], "is_paused": r.get("is_paused")}

        elif name == "airflow_unpause_dag":
            c = _build("airflow", cred)
            r = c.unpause_dag(args["dag_id"])
            return {"dag_id": args["dag_id"], "is_paused": r.get("is_paused")}

        elif name == "airflow_delete_dag":
            c = _build("airflow", cred)
            status = c.delete_dag(args["dag_id"])
            return {"deleted": True, "dag_id": args["dag_id"], "status": status}

        elif name == "airflow_list_dag_warnings":
            c = _build("airflow", cred)
            return {"warnings": c.list_dag_warnings(args.get("dag_id"))}

        elif name == "airflow_list_import_errors":
            c = _build("airflow", cred)
            return c._get("/importErrors")

        # ── DAG Run Operations ────────────────────────────────────────────────
        elif name == "airflow_trigger_dag":
            c      = _build("airflow", cred)
            dag_id = args["dag_id"]
            timeout = int(args.get("timeout", 3600))

            # Pre-flight: if DAG is already running, wait for it to finish first
            waited_on = []
            if args.get("wait_if_running", False):
                waited_on = c.wait_if_running(dag_id, timeout=timeout)

            # Trigger the new run
            run        = c.trigger_dag(dag_id=dag_id, conf=args.get("conf"),
                                        logical_date=args.get("logical_date"),
                                        run_id=args.get("run_id"))
            dag_run_id  = run["dag_run_id"]
            final_state = run.get("state")

            # Block until completion if requested
            if args.get("wait_for_completion", False):
                final_state = c.wait_for_completion(dag_id, dag_run_id, timeout=timeout)
                if final_state != "success":
                    raise RuntimeError(
                        f"DAG {dag_id} run {dag_run_id} ended in state '{final_state}'"
                    )

            return {
                "dag_id":       dag_id,
                "dag_run_id":   dag_run_id,
                "state":        final_state,
                "waited_on":    waited_on,
                "logical_date": run.get("logical_date"),
            }

            c = _build("airflow", cred)
            run = c.trigger_dag(
                dag_id=args["dag_id"],
                conf=args.get("conf"),
            )
            dag_run_id = run["dag_run_id"]
            final_state = c.wait_for_completion(
                args["dag_id"], dag_run_id,
                timeout=int(args.get("timeout", 3600)),
                poll_interval=int(args.get("poll_interval", 10)),
            )
            return {
                "dag_id":     args["dag_id"],
                "dag_run_id": dag_run_id,
                "final_state": final_state,
                "success": final_state == "success",
            }

        elif name == "airflow_get_run_status":
            c = _build("airflow", cred)
            run = c.get_dag_run(args["dag_id"], args["dag_run_id"])
            return {
                "dag_id":     run.get("dag_id"),
                "dag_run_id": run.get("dag_run_id"),
                "state":      run.get("state"),
                "start_date": run.get("start_date"),
                "end_date":   run.get("end_date"),
            }

        elif name == "airflow_list_dag_runs":
            c = _build("airflow", cred)
            runs = c.list_dag_runs(
                dag_id=args["dag_id"],
                limit=int(args.get("limit", 10)),
                states=args.get("states"),
            )
            return {
                "dag_id": args["dag_id"],
                "runs": [
                    {"dag_run_id": r.get("dag_run_id"), "state": r.get("state"),
                     "start_date": r.get("start_date"), "end_date": r.get("end_date"),
                     "logical_date": r.get("logical_date")}
                    for r in runs
                ],
            }

        elif name == "airflow_list_recent_runs":
            c = _build("airflow", cred)
            runs = c.list_dag_runs(dag_id=args["dag_id"],
                                   limit=int(args.get("limit", 10)))
            return {
                "dag_id": args["dag_id"],
                "runs": [{"dag_run_id": r.get("dag_run_id"), "state": r.get("state"),
                           "start_date": r.get("start_date"), "end_date": r.get("end_date")}
                          for r in runs],
            }

        elif name == "airflow_list_dag_runs_batch":
            c = _build("airflow", cred)
            return c._post("/dags/~/dagRuns/list", args.get("body", {}))

        elif name == "airflow_clear_dag_run":
            c = _build("airflow", cred)
            return c.clear_dag_run(
                args["dag_id"], args["dag_run_id"],
                dry_run=bool(args.get("dry_run", False)),
            )

        elif name == "airflow_delete_dag_run":
            c = _build("airflow", cred)
            status = c.delete_dag_run(args["dag_id"], args["dag_run_id"])
            return {"deleted": True, "dag_run_id": args["dag_run_id"], "status": status}

        elif name == "airflow_update_dag_run_state":
            c = _build("airflow", cred)
            return c.update_dag_run_state(
                args["dag_id"], args["dag_run_id"], args["state"]
            )

        # ── Task Instance Operations ──────────────────────────────────────────
        elif name in ("airflow_get_task_instances", "airflow_list_task_instances"):
            c = _build("airflow", cred)
            tasks = c.list_task_instances(args["dag_id"], args["dag_run_id"])
            return {
                "tasks": [
                    {"task_id": t.get("task_id"), "state": t.get("state"),
                     "duration": t.get("duration"), "try_number": t.get("try_number"),
                     "start_date": t.get("start_date"), "end_date": t.get("end_date")}
                    for t in tasks
                ]
            }

        elif name == "airflow_get_task_instance":
            c = _build("airflow", cred)
            return c.get_task_instance(
                args["dag_id"], args["dag_run_id"], args["task_id"]
            )

        elif name == "airflow_get_task_log":
            c = _build("airflow", cred)
            log = c.get_task_log(
                dag_id=args["dag_id"], dag_run_id=args["dag_run_id"],
                task_id=args["task_id"],
                try_number=int(args.get("try_number", 1)),
            )
            return {"log": log[:8000], "truncated": len(log) > 8000}

        elif name == "airflow_list_task_logs":
            c = _build("airflow", cred)
            return {"logs": c.list_task_logs(
                args["dag_id"], args["dag_run_id"], args["task_id"]
            )}

        elif name in ("airflow_clear_task_instances", "airflow_clear_task_instance"):
            c = _build("airflow", cred)
            task_ids = args.get("task_ids") or ([args["task_id"]] if "task_id" in args else None)
            return c._post(
                f"/dags/{args['dag_id']}/dagRuns/{args['dag_run_id']}/taskInstances/clear",
                {"dry_run": bool(args.get("dry_run", False)),
                 "task_ids": task_ids,
                 "only_failed": bool(args.get("only_failed", False))}
            )

        elif name in ("airflow_set_task_instance_state", "airflow_update_task_instance"):
            c = _build("airflow", cred)
            return c.update_task_instance(
                args["dag_id"], args["dag_run_id"], args["task_id"],
                state=args["state"],
                dry_run=bool(args.get("dry_run", False)),
            )

        elif name == "airflow_list_task_instances_batch":
            c = _build("airflow", cred)
            return c._post("/dags/~/dagRuns/~/taskInstances/list", args.get("body", {}))

        elif name == "airflow_list_tasks":
            c = _build("airflow", cred)
            return c._get(f"/dags/{args['dag_id']}/tasks")

        elif name == "airflow_get_task":
            c = _build("airflow", cred)
            return c._get(f"/dags/{args['dag_id']}/tasks/{args['task_id']}")

        elif name == "airflow_get_mapped_task_instances":
            c = _build("airflow", cred)
            return c._get(
                f"/dags/{args['dag_id']}/dagRuns/{args['dag_run_id']}"
                f"/taskInstances/{args['task_id']}/listMapped"
            )

        # ── XCom Operations ───────────────────────────────────────────────────
        elif name == "airflow_get_xcom_entry":
            c = _build("airflow", cred)
            return c.get_xcom_entry(
                args["dag_id"], args["dag_run_id"], args["task_id"],
                xcom_key=args.get("xcom_key", "return_value"),
            )

        elif name == "airflow_list_xcom_entries":
            c = _build("airflow", cred)
            return {"xcom_entries": c.list_xcom_entries(
                args["dag_id"], args["dag_run_id"], args["task_id"],
                limit=int(args.get("limit", 20)),
            )}

        # ── Variable Operations ───────────────────────────────────────────────
        elif name == "airflow_list_variables":
            c = _build("airflow", cred)
            return {"variables": c.list_variables(limit=int(args.get("limit", 100)))}

        elif name == "airflow_get_variable":
            c = _build("airflow", cred)
            return c.get_variable(args["key"])

        elif name in ("airflow_create_variable", "airflow_set_variable",
                       "airflow_update_variable"):
            c = _build("airflow", cred)
            return c.set_variable(
                args["key"], args["value"],
                description=args.get("description", ""),
            )

        elif name == "airflow_delete_variable":
            c = _build("airflow", cred)
            status = c.delete_variable(args["key"])
            return {"deleted": True, "key": args["key"], "status": status}

        # ── Connection Operations ─────────────────────────────────────────────
        elif name == "airflow_list_connections":
            c = _build("airflow", cred)
            return {"connections": c.list_connections(limit=int(args.get("limit", 100)))}

        elif name == "airflow_get_connection":
            c = _build("airflow", cred)
            return c.get_connection(args["conn_id"])

        elif name == "airflow_create_connection":
            c = _build("airflow", cred)
            return c.create_connection(
                conn_id=args["conn_id"], conn_type=args["conn_type"],
                host=args.get("host", ""), port=args.get("port"),
                login=args.get("login", ""), password=args.get("password", ""),
                schema=args.get("schema", ""), extra=args.get("extra", ""),
            )

        elif name == "airflow_update_connection":
            c = _build("airflow", cred)
            fields = {k: v for k, v in args.items() if k not in ("credential", "conn_id")}
            return c.update_connection(args["conn_id"], **fields)

        elif name == "airflow_delete_connection":
            c = _build("airflow", cred)
            status = c.delete_connection(args["conn_id"])
            return {"deleted": True, "conn_id": args["conn_id"], "status": status}

        elif name == "airflow_test_connection":
            c = _build("airflow", cred)
            return c._post("/connections/test", {"connection_id": args["conn_id"],
                                                  "conn_type": args.get("conn_type", "")})

        # ── Pool Operations ───────────────────────────────────────────────────
        elif name == "airflow_list_pools":
            c = _build("airflow", cred)
            return {"pools": c.list_pools()}

        elif name == "airflow_get_pool":
            c = _build("airflow", cred)
            return c.get_pool(args["pool_name"])

        elif name == "airflow_create_pool":
            c = _build("airflow", cred)
            return c.create_pool(args["name"], int(args["slots"]),
                                 args.get("description", ""))

        elif name == "airflow_update_pool":
            c = _build("airflow", cred)
            return c.update_pool(args["name"], int(args["slots"]),
                                 args.get("description"))

        elif name == "airflow_delete_pool":
            c = _build("airflow", cred)
            status = c.delete_pool(args["pool_name"])
            return {"deleted": True, "pool_name": args["pool_name"], "status": status}

        # ── Dataset Operations ────────────────────────────────────────────────
        elif name == "airflow_list_datasets":
            c = _build("airflow", cred)
            return c._get("/datasets", {"limit": int(args.get("limit", 50))})

        elif name == "airflow_get_dataset":
            import urllib.parse
            c = _build("airflow", cred)
            uri_enc = urllib.parse.quote(args["uri"], safe="")
            return c._get(f"/datasets/{uri_enc}")

        elif name == "airflow_list_dataset_events":
            c = _build("airflow", cred)
            params = {"limit": int(args.get("limit", 50))}
            if "dag_id" in args:
                params["source_dag_id"] = args["dag_id"]
            if "task_id" in args:
                params["source_task_id"] = args["task_id"]
            return c._get("/datasets/events", params)

        # ── Provider / Plugin Info ────────────────────────────────────────────
        elif name == "airflow_list_providers":
            c = _build("airflow", cred)
            return c._get("/providers")

        elif name == "airflow_list_plugins":
            c = _build("airflow", cred)
            return c._get("/plugins")

        # ── SQL tools ─────────────────────────────────────────────────────────
        elif name == "sql_execute_query":
            c     = _build("mssql", cred)
            max_r = int(args.get("max_rows", 100))
            c.connect()
            try:
                rows = c.execute_query(args["query"])
                cols = c.last_column_names
                row_dicts = [
                    dict(zip([str(col) for col in cols],
                             [str(v) if v is not None else None for v in row]))
                    for row in rows[:max_r]
                ]
                return {
                    "columns":       [str(col) for col in cols],
                    "rows":          row_dicts,
                    "rows_returned": len(rows),
                    "truncated":     len(rows) > max_r,
                }
            finally:
                c.disconnect()

        elif name == "sql_get_row_count":
            c     = _build("mssql", cred)
            query = args["query"].strip()
            if not query.upper().startswith("SELECT"):
                query = f"SELECT COUNT(*) FROM {query}"
            c.connect()
            try:
                count = c.execute_scalar(query)
                return {"count": count, "query": query}
            finally:
                c.disconnect()

        elif name == "sql_list_tables":
            c      = _build("mssql", cred)
            schema = args.get("schema", "dbo")
            c.connect()
            try:
                tables = c.list_tables(schema)
                return {"schema": schema, "tables": tables, "count": len(tables)}
            finally:
                c.disconnect()

        elif name == "sql_describe_table":
            c = _build("mssql", cred)
            c.connect()
            try:
                cols = c.describe_table(args["table_name"])
                return {"table": args["table_name"], "columns": cols}
            finally:
                c.disconnect()

        # ── HTTP tools ────────────────────────────────────────────────────────
        elif name == "http_get":
            c    = _build("http", cred)
            resp = c.get(args["url"], params=args.get("params"))
            try:
                body = resp.json()
            except Exception:
                body = resp.text[:4000]
            return {"status": resp.status_code, "url": str(resp.url), "body": body}

        elif name == "http_health_check":
            c = _build("http", cred)
            return c.health_check(args.get("url", "/health"))

        # ── S3 tools ──────────────────────────────────────────────────────────
        elif name == "s3_list_objects":
            c     = _build("s3", cred)
            items = c.list_objects(
                bucket=args["bucket"],
                prefix=args.get("prefix", ""),
                max_results=int(args.get("max_results", 50)),
            )
            return {"bucket": args["bucket"], "objects": items, "count": len(items)}

        elif name == "s3_object_exists":
            c      = _build("s3", cred)
            exists = c.object_exists(args["bucket"], args["key"])
            return {"bucket": args["bucket"], "key": args["key"], "exists": exists}

        # ── Azure tools ───────────────────────────────────────────────────────
        elif name == "azure_list_blobs":
            c     = _build("azure", cred)
            blobs = c.list_blobs(args["container"], args.get("prefix", ""))
            return {"container": args["container"], "blobs": blobs, "count": len(blobs)}

        elif name == "azure_blob_exists":
            c      = _build("azure", cred)
            exists = c.blob_exists(args["container"], args["blob_name"])
            return {"container": args["container"], "blob_name": args["blob_name"], "exists": exists}

        # ── SFTP tools ────────────────────────────────────────────────────────
        elif name == "sftp_list_directory":
            c     = _build("sftp", cred)
            path  = args.get("remote_path", "/")
            items = c.list_directory(path)
            c.disconnect()
            return {"path": path, "items": items, "count": len(items)}

        elif name == "sftp_file_exists":
            c      = _build("sftp", cred)
            exists = c.file_exists(args["remote_path"])
            c.disconnect()
            return {"remote_path": args["remote_path"], "exists": exists}

        else:
            return {"error": f"Unknown tool: {name}"}

    except Exception as e:
        logger.error(f"[MCP] Tool {name} failed: {e}", exc_info=True)
        return {"error": str(e), "tool": name}

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

    # ── Airflow ───────────────────────────────────────────────────────────────
    {
        "name": "airflow_health_check",
        "description": "Check if an Airflow instance is reachable and healthy. Use when the user asks about Airflow status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string", "description": "Credential name stored in FlowForge (e.g. PROD_AIRFLOW)"},
            },
            "required": ["credential"],
        },
    },
    {
        "name": "airflow_list_dags",
        "description": "List all DAGs in an Airflow instance. Use when the user asks what DAGs exist or wants to browse available pipelines.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "limit":      {"type": "integer", "default": 30},
            },
            "required": ["credential"],
        },
    },
    {
        "name": "airflow_get_dag",
        "description": "Get metadata for a specific Airflow DAG — schedule, tags, owners, paused status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "dag_id":     {"type": "string", "description": "The DAG ID to look up"},
            },
            "required": ["credential", "dag_id"],
        },
    },
    {
        "name": "airflow_get_run_status",
        "description": "Get the current status of a specific DAG run. Use when user asks whether a DAG is running, succeeded or failed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential":  {"type": "string"},
                "dag_id":      {"type": "string"},
                "dag_run_id":  {"type": "string"},
            },
            "required": ["credential", "dag_id", "dag_run_id"],
        },
    },
    {
        "name": "airflow_list_recent_runs",
        "description": "List the most recent runs for a DAG. Use when user asks about run history or wants to see recent executions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential": {"type": "string"},
                "dag_id":     {"type": "string"},
                "limit":      {"type": "integer", "default": 10},
            },
            "required": ["credential", "dag_id"],
        },
    },
    {
        "name": "airflow_get_task_instances",
        "description": "Get all task states for a DAG run. Use when user wants to know which tasks passed or failed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential":  {"type": "string"},
                "dag_id":      {"type": "string"},
                "dag_run_id":  {"type": "string"},
            },
            "required": ["credential", "dag_id", "dag_run_id"],
        },
    },
    {
        "name": "airflow_get_task_log",
        "description": "Fetch execution logs for a specific Airflow task. Use when user asks why a task failed or wants to see output.",
        "input_schema": {
            "type": "object",
            "properties": {
                "credential":  {"type": "string"},
                "dag_id":      {"type": "string"},
                "dag_run_id":  {"type": "string"},
                "task_id":     {"type": "string"},
                "try_number":  {"type": "integer", "default": 1},
            },
            "required": ["credential", "dag_id", "dag_run_id", "task_id"],
        },
    },

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
        # ── Airflow tools ─────────────────────────────────────────────────────
        if name == "airflow_health_check":
            c = _build("airflow", cred)
            return c.health_check()

        elif name == "airflow_list_dags":
            c     = _build("airflow", cred)
            dags  = c.list_dags()
            limit = int(args.get("limit", 30))
            return {
                "dags": [
                    {"dag_id": d.get("dag_id"), "is_paused": d.get("is_paused"),
                     "schedule": d.get("schedule_interval"),
                     "tags": [t.get("name") for t in d.get("tags", [])]}
                    for d in dags[:limit]
                ],
                "total": len(dags),
            }

        elif name == "airflow_get_dag":
            c   = _build("airflow", cred)
            dag = c.get_dag(args["dag_id"])
            return {
                "dag_id":    dag.get("dag_id"),
                "is_paused": dag.get("is_paused"),
                "schedule":  dag.get("schedule_interval"),
                "tags":      [t.get("name") for t in dag.get("tags", [])],
                "owners":    dag.get("owners", []),
            }

        elif name == "airflow_get_run_status":
            c   = _build("airflow", cred)
            run = c.get_dag_run(args["dag_id"], args["dag_run_id"])
            return {
                "dag_id":     run.get("dag_id"),
                "dag_run_id": run.get("dag_run_id"),
                "state":      run.get("state"),
                "start_date": run.get("start_date"),
                "end_date":   run.get("end_date"),
            }

        elif name == "airflow_list_recent_runs":
            c    = _build("airflow", cred)
            data = c._get(
                f"/dags/{args['dag_id']}/dagRuns",
                params={"limit": args.get("limit", 10), "order_by": "-execution_date"},
            )
            return {
                "dag_id": args["dag_id"],
                "runs": [
                    {"dag_run_id": r.get("dag_run_id"), "state": r.get("state"),
                     "start_date": r.get("start_date"), "end_date": r.get("end_date")}
                    for r in data.get("dag_runs", [])
                ],
            }

        elif name == "airflow_get_task_instances":
            c     = _build("airflow", cred)
            tasks = c.get_task_instances(args["dag_id"], args["dag_run_id"])
            return {
                "tasks": [
                    {"task_id": t.get("task_id"), "state": t.get("state"),
                     "duration": t.get("duration"), "try_number": t.get("try_number")}
                    for t in tasks
                ]
            }

        elif name == "airflow_get_task_log":
            c   = _build("airflow", cred)
            log = c.get_task_log(
                dag_id=args["dag_id"],
                dag_run_id=args["dag_run_id"],
                task_id=args["task_id"],
                try_number=int(args.get("try_number", 1)),
            )
            return {"log": log[:8000], "truncated": len(log) > 8000}

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

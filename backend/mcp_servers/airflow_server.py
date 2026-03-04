"""
FlowForge — Airflow MCP Server (Full Coverage)

Exposes all Apache Airflow 2.7.3 REST API operations as MCP tools.

Credentials injected via environment variables:
    AIRFLOW_BASE_URL   — e.g. http://airflow.internal:8080
    AIRFLOW_USERNAME
    AIRFLOW_PASSWORD

Run:
    python -m mcp_servers.airflow_server
"""

import os
import sys
import json
import asyncio
import logging

logger = logging.getLogger(__name__)

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    import mcp.types as types
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False


# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [

    # ── System ────────────────────────────────────────────────────────────────
    {
        "name": "airflow_health_check",
        "description": "Check if the Airflow instance is reachable and healthy.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "airflow_get_version",
        "description": "Get the Airflow version and Git version.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "airflow_get_config",
        "description": "Get the Airflow configuration (requires expose_config=True in airflow.cfg).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "section": {"type": "string", "description": "Config section to retrieve (omit for all)"},
            },
            "required": [],
        },
    },

    # ── DAG Operations ────────────────────────────────────────────────────────
    {
        "name": "airflow_list_dags",
        "description": "List all DAGs with their schedule, tags, and paused status.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit":       {"type": "integer", "description": "Max DAGs to return (default 100)"},
                "only_active": {"type": "boolean", "description": "Only return active (non-paused) DAGs"},
                "tags":        {"type": "array", "items": {"type": "string"}, "description": "Filter by tags"},
            },
            "required": [],
        },
    },
    {
        "name": "airflow_get_dag",
        "description": "Get metadata for a specific DAG (schedule, tags, owners, is_paused).",
        "inputSchema": {
            "type": "object",
            "properties": {"dag_id": {"type": "string"}},
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_get_dag_details",
        "description": "Get extended DAG info including doc_md, default_args, and params schema.",
        "inputSchema": {
            "type": "object",
            "properties": {"dag_id": {"type": "string"}},
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_get_dag_source",
        "description": "Get the Python source code of a DAG file.",
        "inputSchema": {
            "type": "object",
            "properties": {"dag_id": {"type": "string"}},
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_pause_dag",
        "description": "Pause a DAG (stops it from being scheduled).",
        "inputSchema": {
            "type": "object",
            "properties": {"dag_id": {"type": "string"}},
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_unpause_dag",
        "description": "Unpause a DAG (re-enables scheduling).",
        "inputSchema": {
            "type": "object",
            "properties": {"dag_id": {"type": "string"}},
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_delete_dag",
        "description": "Delete a DAG and all its metadata (runs, task instances). Irreversible.",
        "inputSchema": {
            "type": "object",
            "properties": {"dag_id": {"type": "string"}},
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_list_dag_warnings",
        "description": "List import warnings for DAGs (syntax errors, missing dependencies, etc.).",
        "inputSchema": {
            "type": "object",
            "properties": {"dag_id": {"type": "string", "description": "Filter by DAG ID (omit for all)"}},
            "required": [],
        },
    },

    # ── DAG Run Operations ────────────────────────────────────────────────────
    {
        "name": "airflow_trigger_dag",
        "description": "Trigger a new DAG run and return the dag_run_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":       {"type": "string"},
                "conf":         {"type": "object", "description": "Config dict passed to the DAG run"},
                "logical_date": {"type": "string", "description": "ISO8601 logical date override"},
                "run_id":       {"type": "string", "description": "Custom run ID"},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_get_run_status",
        "description": "Get current status of a DAG run (queued/running/success/failed).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
            },
            "required": ["dag_id", "dag_run_id"],
        },
    },
    {
        "name": "airflow_list_dag_runs",
        "description": "List recent runs for a DAG with state and duration.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "limit":  {"type": "integer", "description": "Number of runs to return (default 10)"},
                "states": {"type": "array", "items": {"type": "string"},
                           "description": "Filter by states e.g. ['running','failed']"},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_clear_dag_run",
        "description": "Clear (re-queue) all tasks in a DAG run so they re-execute.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "dry_run":    {"type": "boolean", "description": "Preview what would be cleared without doing it"},
            },
            "required": ["dag_id", "dag_run_id"],
        },
    },
    {
        "name": "airflow_delete_dag_run",
        "description": "Delete a specific DAG run and its task instance records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
            },
            "required": ["dag_id", "dag_run_id"],
        },
    },
    {
        "name": "airflow_update_dag_run_state",
        "description": "Force-set a DAG run's state to queued, success, or failed.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "state":      {"type": "string", "enum": ["queued", "success", "failed"]},
            },
            "required": ["dag_id", "dag_run_id", "state"],
        },
    },

    # ── Task Instance Operations ──────────────────────────────────────────────
    {
        "name": "airflow_get_task_instances",
        "description": "Get all task instances for a DAG run with states and durations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
            },
            "required": ["dag_id", "dag_run_id"],
        },
    },
    {
        "name": "airflow_get_task_instance",
        "description": "Get a specific task instance's state, duration, and try number.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id":    {"type": "string"},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
    {
        "name": "airflow_update_task_state",
        "description": "Force-set a task instance's state (e.g. mark as success or failed).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id":    {"type": "string"},
                "state":      {"type": "string",
                               "enum": ["success", "failed", "skipped", "up_for_retry", "queued"]},
                "dry_run":    {"type": "boolean"},
            },
            "required": ["dag_id", "dag_run_id", "task_id", "state"],
        },
    },
    {
        "name": "airflow_clear_task_instance",
        "description": "Clear (re-queue) a specific task instance so it re-runs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id":    {"type": "string"},
                "dry_run":    {"type": "boolean"},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
    {
        "name": "airflow_get_task_log",
        "description": "Fetch the execution log for a specific task instance.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id":    {"type": "string"},
                "try_number": {"type": "integer", "description": "Try number (default 1)"},
                "all_tries":  {"type": "boolean", "description": "Return logs for all try attempts"},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },

    # ── Variable Operations ───────────────────────────────────────────────────
    {
        "name": "airflow_list_variables",
        "description": "List all Airflow Variables (key-value pairs stored in Airflow).",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer", "description": "Max variables (default 100)"}},
            "required": [],
        },
    },
    {
        "name": "airflow_get_variable",
        "description": "Get the value of a specific Airflow Variable by key.",
        "inputSchema": {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
        },
    },
    {
        "name": "airflow_set_variable",
        "description": "Create or update an Airflow Variable (upsert).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key":         {"type": "string"},
                "value":       {"type": "string"},
                "description": {"type": "string"},
            },
            "required": ["key", "value"],
        },
    },
    {
        "name": "airflow_delete_variable",
        "description": "Delete an Airflow Variable by key.",
        "inputSchema": {
            "type": "object",
            "properties": {"key": {"type": "string"}},
            "required": ["key"],
        },
    },

    # ── Connection Operations ─────────────────────────────────────────────────
    {
        "name": "airflow_list_connections",
        "description": "List all Airflow Connections (database/service credentials).",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer"}},
            "required": [],
        },
    },
    {
        "name": "airflow_get_connection",
        "description": "Get a specific Airflow Connection by ID.",
        "inputSchema": {
            "type": "object",
            "properties": {"conn_id": {"type": "string"}},
            "required": ["conn_id"],
        },
    },
    {
        "name": "airflow_create_connection",
        "description": "Create a new Airflow Connection.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "conn_id":   {"type": "string"},
                "conn_type": {"type": "string", "description": "e.g. http, postgres, s3, generic"},
                "host":      {"type": "string"},
                "port":      {"type": "integer"},
                "login":     {"type": "string"},
                "password":  {"type": "string"},
                "schema":    {"type": "string"},
                "extra":     {"type": "string", "description": "JSON extra config"},
            },
            "required": ["conn_id", "conn_type"],
        },
    },
    {
        "name": "airflow_update_connection",
        "description": "Update fields on an existing Airflow Connection.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "conn_id":  {"type": "string"},
                "host":     {"type": "string"},
                "port":     {"type": "integer"},
                "login":    {"type": "string"},
                "password": {"type": "string"},
                "schema":   {"type": "string"},
                "extra":    {"type": "string"},
            },
            "required": ["conn_id"],
        },
    },
    {
        "name": "airflow_delete_connection",
        "description": "Delete an Airflow Connection by ID.",
        "inputSchema": {
            "type": "object",
            "properties": {"conn_id": {"type": "string"}},
            "required": ["conn_id"],
        },
    },

    # ── Pool Operations ───────────────────────────────────────────────────────
    {
        "name": "airflow_list_pools",
        "description": "List all Airflow Pools with slot counts and occupancy.",
        "inputSchema": {
            "type": "object",
            "properties": {"limit": {"type": "integer"}},
            "required": [],
        },
    },
    {
        "name": "airflow_get_pool",
        "description": "Get a specific Airflow Pool's slot count and current occupancy.",
        "inputSchema": {
            "type": "object",
            "properties": {"pool_name": {"type": "string"}},
            "required": ["pool_name"],
        },
    },
    {
        "name": "airflow_create_pool",
        "description": "Create a new Airflow Pool with a slot limit.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pool_name":   {"type": "string"},
                "slots":       {"type": "integer"},
                "description": {"type": "string"},
            },
            "required": ["pool_name", "slots"],
        },
    },
    {
        "name": "airflow_update_pool",
        "description": "Update an Airflow Pool's slot count.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pool_name":   {"type": "string"},
                "slots":       {"type": "integer"},
                "description": {"type": "string"},
            },
            "required": ["pool_name", "slots"],
        },
    },
    {
        "name": "airflow_delete_pool",
        "description": "Delete an Airflow Pool.",
        "inputSchema": {
            "type": "object",
            "properties": {"pool_name": {"type": "string"}},
            "required": ["pool_name"],
        },
    },

    # ── XCom Operations ───────────────────────────────────────────────────────
    {
        "name": "airflow_get_xcom_entry",
        "description": "Get an XCom value pushed by a task (default key: return_value).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id":    {"type": "string"},
                "xcom_key":   {"type": "string", "description": "XCom key (default: return_value)"},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
    {
        "name": "airflow_list_xcom_entries",
        "description": "List all XCom entries pushed by a task instance.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id":    {"type": "string"},
                "limit":      {"type": "integer", "description": "Max entries (default 20)"},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
]


def _build_connector():
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from connectors.airflow_mcp import AirflowMCP
    return AirflowMCP(
        base_url=os.environ["AIRFLOW_BASE_URL"],
        username=os.environ.get("AIRFLOW_USERNAME", ""),
        password=os.environ.get("AIRFLOW_PASSWORD", ""),
    )


def execute_tool(name: str, args: dict) -> dict:
    c = _build_connector()
    try:
        # ── System ────────────────────────────────────────────────────────────
        if   name == "airflow_health_check": return c.health_check()
        elif name == "airflow_get_version":  return c.get_version()
        elif name == "airflow_get_config":
            return c.get_config(section=args.get("section"))

        # ── DAG ops ───────────────────────────────────────────────────────────
        elif name == "airflow_list_dags":
            dags = c.list_dags(
                limit=int(args.get("limit", 100)),
                tags=args.get("tags"),
                only_active=args.get("only_active", True),
            )
            return {"dags": [{"dag_id": d.get("dag_id"), "is_paused": d.get("is_paused"),
                               "schedule": d.get("schedule_interval"),
                               "tags": [t.get("name") for t in d.get("tags", [])],
                               "description": d.get("description", "")}
                              for d in dags], "total": len(dags)}

        elif name == "airflow_get_dag":         return c.get_dag(args["dag_id"])
        elif name == "airflow_get_dag_details": return c.get_dag_details(args["dag_id"])
        elif name == "airflow_get_dag_source":
            dag = c.get_dag(args["dag_id"])
            src = c.get_dag_source(dag.get("file_token", ""))
            return {"dag_id": args["dag_id"], "source": src}
        elif name == "airflow_pause_dag":   return c.pause_dag(args["dag_id"])
        elif name == "airflow_unpause_dag": return c.unpause_dag(args["dag_id"])
        elif name == "airflow_delete_dag":
            status = c.delete_dag(args["dag_id"])
            return {"dag_id": args["dag_id"], "deleted": True, "status_code": status}
        elif name == "airflow_list_dag_warnings":
            warnings = c.list_dag_warnings(args.get("dag_id"))
            return {"warnings": warnings, "count": len(warnings)}

        # ── DAG Run ops ───────────────────────────────────────────────────────
        elif name == "airflow_trigger_dag":
            run = c.trigger_dag(
                dag_id=args["dag_id"],
                conf=args.get("conf", {}),
                logical_date=args.get("logical_date"),
                run_id=args.get("run_id"),
            )
            return {"dag_id": args["dag_id"], "dag_run_id": run.get("dag_run_id"),
                    "state": run.get("state"), "logical_date": run.get("logical_date")}

        elif name == "airflow_get_run_status":
            run = c.get_dag_run(args["dag_id"], args["dag_run_id"])
            return {"dag_id": run.get("dag_id"), "dag_run_id": run.get("dag_run_id"),
                    "state": run.get("state"), "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"), "logical_date": run.get("logical_date")}

        elif name == "airflow_list_dag_runs":
            runs = c.list_dag_runs(
                dag_id=args["dag_id"],
                limit=int(args.get("limit", 10)),
                states=args.get("states"),
            )
            return {"dag_id": args["dag_id"], "runs": [
                {"dag_run_id": r.get("dag_run_id"), "state": r.get("state"),
                 "start_date": r.get("start_date"), "end_date": r.get("end_date"),
                 "logical_date": r.get("logical_date")} for r in runs
            ], "count": len(runs)}

        elif name == "airflow_clear_dag_run":
            result = c.clear_dag_run(args["dag_id"], args["dag_run_id"],
                                     dry_run=args.get("dry_run", False))
            return {"dag_id": args["dag_id"], "dag_run_id": args["dag_run_id"],
                    "dry_run": args.get("dry_run", False), **result}

        elif name == "airflow_delete_dag_run":
            status = c.delete_dag_run(args["dag_id"], args["dag_run_id"])
            return {"dag_id": args["dag_id"], "dag_run_id": args["dag_run_id"],
                    "deleted": True, "status_code": status}

        elif name == "airflow_update_dag_run_state":
            result = c.update_dag_run_state(args["dag_id"], args["dag_run_id"], args["state"])
            return {"dag_id": args["dag_id"], "dag_run_id": args["dag_run_id"],
                    "state": args["state"], **result}

        # ── Task Instance ops ─────────────────────────────────────────────────
        elif name == "airflow_get_task_instances":
            tasks = c.get_task_instances(args["dag_id"], args["dag_run_id"])
            return {"tasks": [{"task_id": t.get("task_id"), "state": t.get("state"),
                                "start_date": t.get("start_date"), "end_date": t.get("end_date"),
                                "duration": t.get("duration"), "try_number": t.get("try_number")}
                               for t in tasks]}

        elif name == "airflow_get_task_instance":
            return c.get_task_instance(args["dag_id"], args["dag_run_id"], args["task_id"])

        elif name == "airflow_update_task_state":
            result = c.update_task_instance(args["dag_id"], args["dag_run_id"], args["task_id"],
                                            args["state"], dry_run=args.get("dry_run", False))
            return {"dag_id": args["dag_id"], "dag_run_id": args["dag_run_id"],
                    "task_id": args["task_id"], "state": args["state"], **result}

        elif name == "airflow_clear_task_instance":
            result = c.clear_task_instance(args["dag_id"], args["dag_run_id"], args["task_id"],
                                           dry_run=args.get("dry_run", False))
            return {"dag_id": args["dag_id"], "dag_run_id": args["dag_run_id"],
                    "task_id": args["task_id"], "cleared": True, **result}

        elif name == "airflow_get_task_log":
            if args.get("all_tries"):
                logs = c.list_task_logs(args["dag_id"], args["dag_run_id"], args["task_id"])
                return {"task_id": args["task_id"], "logs": logs}
            else:
                log = c.get_task_log(args["dag_id"], args["dag_run_id"], args["task_id"],
                                     try_number=int(args.get("try_number", 1)))
                return {"task_id": args["task_id"], "try_number": args.get("try_number", 1),
                        "log": log[:8000], "truncated": len(log) > 8000}

        # ── Variable ops ──────────────────────────────────────────────────────
        elif name == "airflow_list_variables":
            variables = c.list_variables(limit=int(args.get("limit", 100)))
            return {"variables": variables, "count": len(variables)}

        elif name == "airflow_get_variable":
            var = c.get_variable(args["key"])
            return {"key": args["key"], "value": var.get("value"), "description": var.get("description", "")}

        elif name == "airflow_set_variable":
            result = c.set_variable(args["key"], args["value"], description=args.get("description", ""))
            return {"key": args["key"], "value": args["value"], **result}

        elif name == "airflow_delete_variable":
            c.delete_variable(args["key"])
            return {"key": args["key"], "deleted": True}

        # ── Connection ops ────────────────────────────────────────────────────
        elif name == "airflow_list_connections":
            conns = c.list_connections(limit=int(args.get("limit", 100)))
            return {"connections": conns, "count": len(conns)}

        elif name == "airflow_get_connection":    return c.get_connection(args["conn_id"])
        elif name == "airflow_create_connection":
            return c.create_connection(
                conn_id=args["conn_id"], conn_type=args["conn_type"],
                host=args.get("host", ""), port=args.get("port"),
                login=args.get("login", ""), password=args.get("password", ""),
                schema=args.get("schema", ""), extra=args.get("extra", ""),
            )
        elif name == "airflow_update_connection":
            fields = {k: v for k, v in args.items() if k != "conn_id"}
            return c.update_connection(args["conn_id"], **fields)

        elif name == "airflow_delete_connection":
            c.delete_connection(args["conn_id"])
            return {"conn_id": args["conn_id"], "deleted": True}

        # ── Pool ops ──────────────────────────────────────────────────────────
        elif name == "airflow_list_pools":
            pools = c.list_pools(limit=int(args.get("limit", 100)))
            return {"pools": pools, "count": len(pools)}

        elif name == "airflow_get_pool": return c.get_pool(args["pool_name"])
        elif name == "airflow_create_pool":
            return c.create_pool(name=args["pool_name"], slots=int(args["slots"]),
                                 description=args.get("description", ""))
        elif name == "airflow_update_pool":
            return c.update_pool(name=args["pool_name"], slots=int(args["slots"]),
                                 description=args.get("description"))
        elif name == "airflow_delete_pool":
            c.delete_pool(args["pool_name"])
            return {"pool_name": args["pool_name"], "deleted": True}

        # ── XCom ops ──────────────────────────────────────────────────────────
        elif name == "airflow_get_xcom_entry":
            entry = c.get_xcom_entry(args["dag_id"], args["dag_run_id"], args["task_id"],
                                     xcom_key=args.get("xcom_key", "return_value"))
            return {"key": args.get("xcom_key", "return_value"),
                    "value": entry.get("value"), "timestamp": entry.get("timestamp")}

        elif name == "airflow_list_xcom_entries":
            entries = c.list_xcom_entries(args["dag_id"], args["dag_run_id"], args["task_id"],
                                          limit=int(args.get("limit", 20)))
            return {"entries": entries, "count": len(entries)}

        else:
            return {"error": f"Unknown tool: {name}"}

    except Exception as e:
        return {"error": str(e), "tool": name}


# ── MCP Server entrypoint ─────────────────────────────────────────────────────

async def main():
    if not MCP_AVAILABLE:
        print("ERROR: mcp package not installed. Run: pip install mcp", file=sys.stderr)
        sys.exit(1)

    server = Server("flowforge-airflow")

    @server.list_tools()
    async def list_tools():
        return [
            types.Tool(name=t["name"], description=t["description"], inputSchema=t["inputSchema"])
            for t in TOOLS
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict):
        result = await asyncio.get_event_loop().run_in_executor(None, execute_tool, name, arguments)
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())

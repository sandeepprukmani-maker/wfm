"""
FlowForge — Airflow MCP Server

Standalone MCP server wrapping AirflowMCP connector.
Credentials injected via environment variables:
    AIRFLOW_BASE_URL   — e.g. http://airflow.internal:8080
    AIRFLOW_USERNAME
    AIRFLOW_PASSWORD

Run:
    python -m mcp_servers.airflow_server

Claude Desktop config (claude_desktop_config.json):
    {
      "mcpServers": {
        "flowforge-airflow": {
          "command": "python",
          "args": ["-m", "mcp_servers.airflow_server"],
          "cwd": "/path/to/flowforge/backend",
          "env": {
            "AIRFLOW_BASE_URL": "http://airflow.internal:8080",
            "AIRFLOW_USERNAME": "admin",
            "AIRFLOW_PASSWORD": "secret"
          }
        }
      }
    }
"""

import os
import sys
import json
import asyncio
import logging

logger = logging.getLogger(__name__)

# ── MCP imports ───────────────────────────────────────────────────────────────
try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    import mcp.types as types
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False


# ── Tool definitions ──────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "airflow_health_check",
        "description": "Check if the Airflow instance is reachable and healthy.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "airflow_list_dags",
        "description": "List all DAGs in the Airflow instance with their status (paused/active).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max number of DAGs to return (default 50)",
                    "default": 50,
                }
            },
            "required": [],
        },
    },
    {
        "name": "airflow_get_dag",
        "description": "Get metadata for a specific DAG including its schedule and tags.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "The DAG ID to look up"},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_trigger_dag",
        "description": "Trigger a DAG run and return the dag_run_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "DAG to trigger"},
                "conf":   {"type": "object", "description": "Optional config dict passed to DAG run"},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "airflow_get_run_status",
        "description": "Get the current status of a DAG run (queued/running/success/failed).",
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
        "name": "airflow_get_task_instances",
        "description": "Get all task instances for a DAG run with their states and durations.",
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
        "name": "airflow_get_task_log",
        "description": "Fetch the execution log for a specific task in a DAG run.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id":    {"type": "string"},
                "try_number": {"type": "integer", "default": 1},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
    {
        "name": "airflow_list_recent_runs",
        "description": "List the most recent runs for a DAG with their states and durations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "limit":  {"type": "integer", "default": 10},
            },
            "required": ["dag_id"],
        },
    },
]


def _build_connector():
    """Build AirflowMCP from environment variables."""
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from connectors.airflow_mcp import AirflowMCP
    return AirflowMCP(
        base_url=os.environ["AIRFLOW_BASE_URL"],
        username=os.environ.get("AIRFLOW_USERNAME", ""),
        password=os.environ.get("AIRFLOW_PASSWORD", ""),
    )


def execute_tool(name: str, args: dict) -> dict:
    """Execute a tool call. Used both by the MCP server and the inline chat executor."""
    connector = _build_connector()
    try:
        if name == "airflow_health_check":
            return connector.health_check()

        elif name == "airflow_list_dags":
            dags = connector.list_dags()
            limit = int(args.get("limit", 50))
            return {
                "dags": [
                    {
                        "dag_id":      d.get("dag_id"),
                        "is_paused":   d.get("is_paused"),
                        "schedule":    d.get("schedule_interval"),
                        "tags":        [t.get("name") for t in d.get("tags", [])],
                        "description": d.get("description", ""),
                    }
                    for d in dags[:limit]
                ],
                "total": len(dags),
            }

        elif name == "airflow_get_dag":
            dag = connector.get_dag(args["dag_id"])
            return {
                "dag_id":   dag.get("dag_id"),
                "is_paused":dag.get("is_paused"),
                "schedule": dag.get("schedule_interval"),
                "tags":     [t.get("name") for t in dag.get("tags", [])],
                "owners":   dag.get("owners", []),
            }

        elif name == "airflow_trigger_dag":
            dag_run_id = connector.trigger_dag(
                dag_id=args["dag_id"],
                conf=args.get("conf", {}),
            )
            return {"dag_id": args["dag_id"], "dag_run_id": dag_run_id, "status": "triggered"}

        elif name == "airflow_get_run_status":
            run = connector.get_dag_run(args["dag_id"], args["dag_run_id"])
            return {
                "dag_id":        run.get("dag_id"),
                "dag_run_id":    run.get("dag_run_id"),
                "state":         run.get("state"),
                "start_date":    run.get("start_date"),
                "end_date":      run.get("end_date"),
                "logical_date":  run.get("logical_date"),
            }

        elif name == "airflow_get_task_instances":
            tasks = connector.get_task_instances(args["dag_id"], args["dag_run_id"])
            return {
                "tasks": [
                    {
                        "task_id":       t.get("task_id"),
                        "state":         t.get("state"),
                        "start_date":    t.get("start_date"),
                        "end_date":      t.get("end_date"),
                        "duration":      t.get("duration"),
                        "try_number":    t.get("try_number"),
                    }
                    for t in tasks
                ]
            }

        elif name == "airflow_get_task_log":
            log = connector.get_task_log(
                dag_id=args["dag_id"],
                dag_run_id=args["dag_run_id"],
                task_id=args["task_id"],
                try_number=int(args.get("try_number", 1)),
            )
            return {"log": log[:8000], "truncated": len(log) > 8000}

        elif name == "airflow_list_recent_runs":
            data = connector._get(
                f"/dags/{args['dag_id']}/dagRuns",
                params={"limit": args.get("limit", 10), "order_by": "-execution_date"},
            )
            runs = data.get("dag_runs", [])
            return {
                "dag_id": args["dag_id"],
                "runs": [
                    {
                        "dag_run_id":   r.get("dag_run_id"),
                        "state":        r.get("state"),
                        "start_date":   r.get("start_date"),
                        "end_date":     r.get("end_date"),
                        "logical_date": r.get("logical_date"),
                    }
                    for r in runs
                ],
            }

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
            types.Tool(
                name=t["name"],
                description=t["description"],
                inputSchema=t["inputSchema"],
            )
            for t in TOOLS
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict):
        result = await asyncio.get_event_loop().run_in_executor(
            None, execute_tool, name, arguments
        )
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())

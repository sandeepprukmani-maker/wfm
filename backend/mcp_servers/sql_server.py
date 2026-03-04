"""
FlowForge — SQL Server MCP Server (MSSQL)

Env vars:
    MSSQL_CONNECTION_STRING  — full ODBC connection string (preferred)
    MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD  — alternative

Claude Desktop config:
    {
      "mcpServers": {
        "flowforge-sql": {
          "command": "python",
          "args": ["-m", "mcp_servers.sql_server"],
          "cwd": "/path/to/flowforge/backend",
          "env": { "MSSQL_CONNECTION_STRING": "Driver={ODBC Driver 17...};..." }
        }
      }
    }
"""

import os
import sys
import json
import asyncio

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    import mcp.types as types
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False


TOOLS = [
    {
        "name": "sql_health_check",
        "description": "Check SQL Server connectivity and return server version.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "sql_execute_query",
        "description": (
            "Execute a SELECT query and return rows as a list of dicts. "
            "Always use this for data retrieval questions — never guess at data."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query":    {"type": "string", "description": "SQL SELECT statement to execute"},
                "max_rows": {"type": "integer", "description": "Max rows to return (default 100)", "default": 100},
            },
            "required": ["query"],
        },
    },
    {
        "name": "sql_get_row_count",
        "description": "Get the row count for a table or a COUNT(*) query result.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SELECT COUNT(*) query or just a table name"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "sql_list_tables",
        "description": "List all tables in a schema.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "schema": {"type": "string", "description": "Schema name (default: dbo)", "default": "dbo"},
            },
            "required": [],
        },
    },
    {
        "name": "sql_describe_table",
        "description": "Get column names, types, and nullability for a table.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Table name to describe"},
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "sql_execute_scalar",
        "description": "Execute a query and return a single scalar value (e.g. COUNT, MAX, SUM).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
            },
            "required": ["query"],
        },
    },
]


def _build_connector():
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from connectors.mssql_mcp import MSSQLMCP
    conn_str = os.environ.get("MSSQL_CONNECTION_STRING")
    if conn_str:
        return MSSQLMCP(connection_string=conn_str)
    return MSSQLMCP(
        server=os.environ["MSSQL_SERVER"],
        database=os.environ["MSSQL_DATABASE"],
        username=os.environ.get("MSSQL_USERNAME"),
        password=os.environ.get("MSSQL_PASSWORD"),
    )


def execute_tool(name: str, args: dict) -> dict:
    connector = _build_connector()
    try:
        connector.connect()

        if name == "sql_health_check":
            return connector.health_check()

        elif name == "sql_execute_query":
            query    = args["query"]
            max_rows = int(args.get("max_rows", 100))
            rows     = connector.execute_query(query)
            cols     = connector.last_column_names
            row_dicts = [
                dict(zip([str(c) for c in cols], [str(v) if v is not None else None for v in row]))
                for row in rows[:max_rows]
            ]
            return {
                "columns":       [str(c) for c in cols],
                "rows":          row_dicts,
                "rows_returned": len(rows),
                "truncated":     len(rows) > max_rows,
            }

        elif name == "sql_get_row_count":
            query = args["query"].strip()
            # If it's just a table name, wrap it
            if not query.upper().startswith("SELECT"):
                query = f"SELECT COUNT(*) FROM {query}"
            count = connector.execute_scalar(query)
            return {"count": count, "query": query}

        elif name == "sql_list_tables":
            schema = args.get("schema", "dbo")
            tables = connector.list_tables(schema)
            return {"schema": schema, "tables": tables, "count": len(tables)}

        elif name == "sql_describe_table":
            cols = connector.describe_table(args["table_name"])
            return {"table": args["table_name"], "columns": cols}

        elif name == "sql_execute_scalar":
            value = connector.execute_scalar(args["query"])
            return {"value": value, "query": args["query"]}

        else:
            return {"error": f"Unknown tool: {name}"}

    except Exception as e:
        return {"error": str(e), "tool": name}
    finally:
        try:
            connector.disconnect()
        except Exception:
            pass


async def main():
    if not MCP_AVAILABLE:
        print("ERROR: mcp package not installed. Run: pip install mcp", file=sys.stderr)
        sys.exit(1)

    server = Server("flowforge-sql")

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

"""
FlowForge — Azure Blob Storage MCP Server

Env vars:
    AZURE_CONNECTION_STRING   — preferred
    AZURE_ACCOUNT_NAME + AZURE_ACCOUNT_KEY  — alternative
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
        "name": "azure_health_check",
        "description": "Verify Azure Blob Storage connectivity.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "azure_list_containers",
        "description": "List all Blob Storage containers in the storage account.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "azure_list_blobs",
        "description": "List blobs in a container, optionally filtered by prefix.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "container": {"type": "string"},
                "prefix":    {"type": "string", "default": ""},
            },
            "required": ["container"],
        },
    },
    {
        "name": "azure_blob_exists",
        "description": "Check whether a specific blob exists in a container.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "container": {"type": "string"},
                "blob_name": {"type": "string"},
            },
            "required": ["container", "blob_name"],
        },
    },
]


def _build_connector():
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from connectors.azure_mcp import AzureConnector
    conn_str = os.environ.get("AZURE_CONNECTION_STRING")
    if conn_str:
        return AzureConnector(connection_string=conn_str)
    return AzureConnector(
        account_name=os.environ["AZURE_ACCOUNT_NAME"],
        account_key=os.environ["AZURE_ACCOUNT_KEY"],
    )


def execute_tool(name: str, args: dict) -> dict:
    connector = _build_connector()
    try:
        if name == "azure_health_check":
            return connector.health_check()

        elif name == "azure_list_containers":
            containers = connector.list_containers()
            return {"containers": containers, "count": len(containers)}

        elif name == "azure_list_blobs":
            blobs = connector.list_blobs(args["container"], args.get("prefix", ""))
            return {"container": args["container"], "blobs": blobs, "count": len(blobs)}

        elif name == "azure_blob_exists":
            exists = connector.blob_exists(args["container"], args["blob_name"])
            return {"container": args["container"], "blob_name": args["blob_name"], "exists": exists}

        else:
            return {"error": f"Unknown tool: {name}"}

    except Exception as e:
        return {"error": str(e), "tool": name}


async def main():
    if not MCP_AVAILABLE:
        print("ERROR: mcp package not installed. Run: pip install mcp", file=sys.stderr)
        sys.exit(1)

    server = Server("flowforge-azure")

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

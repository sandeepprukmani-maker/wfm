"""
FlowForge — S3 MCP Server

Env vars:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION  (default: us-east-1)
    S3_ENDPOINT_URL     (optional, for MinIO/LocalStack)
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
        "name": "s3_health_check",
        "description": "Verify S3 credentials by listing buckets.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "s3_list_objects",
        "description": "List objects in an S3 bucket, optionally filtered by prefix.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "bucket":      {"type": "string"},
                "prefix":      {"type": "string", "default": ""},
                "max_results": {"type": "integer", "default": 100},
            },
            "required": ["bucket"],
        },
    },
    {
        "name": "s3_object_exists",
        "description": "Check whether a specific S3 object exists.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "bucket": {"type": "string"},
                "key":    {"type": "string"},
            },
            "required": ["bucket", "key"],
        },
    },
    {
        "name": "s3_get_object_metadata",
        "description": "Get size, last modified date, and ETag for an S3 object.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "bucket": {"type": "string"},
                "key":    {"type": "string"},
            },
            "required": ["bucket", "key"],
        },
    },
    {
        "name": "s3_list_buckets",
        "description": "List all S3 buckets accessible with the current credentials.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
]


def _build_connector():
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from connectors.s3_mcp import S3Connector
    return S3Connector(
        access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
    )


def execute_tool(name: str, args: dict) -> dict:
    connector = _build_connector()
    try:
        if name == "s3_health_check":
            return connector.health_check()

        elif name == "s3_list_objects":
            items = connector.list_objects(
                bucket=args["bucket"],
                prefix=args.get("prefix", ""),
                max_results=int(args.get("max_results", 100)),
            )
            return {"bucket": args["bucket"], "objects": items, "count": len(items)}

        elif name == "s3_object_exists":
            exists = connector.object_exists(args["bucket"], args["key"])
            return {"bucket": args["bucket"], "key": args["key"], "exists": exists}

        elif name == "s3_get_object_metadata":
            meta = connector.get_object_metadata(args["bucket"], args["key"])
            return {"bucket": args["bucket"], "key": args["key"], **meta}

        elif name == "s3_list_buckets":
            buckets = connector._s3.list_buckets().get("Buckets", [])
            return {"buckets": [{"name": b["Name"], "created": str(b["CreationDate"])} for b in buckets]}

        else:
            return {"error": f"Unknown tool: {name}"}

    except Exception as e:
        return {"error": str(e), "tool": name}


async def main():
    if not MCP_AVAILABLE:
        print("ERROR: mcp package not installed. Run: pip install mcp", file=sys.stderr)
        sys.exit(1)

    server = Server("flowforge-s3")

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

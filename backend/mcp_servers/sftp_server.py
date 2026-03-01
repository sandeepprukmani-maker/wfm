"""
FlowForge — SFTP MCP Server

Env vars:
    SFTP_HOST
    SFTP_USERNAME
    SFTP_PASSWORD
    SFTP_PORT     (default: 22)
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
        "name": "sftp_health_check",
        "description": "Verify SFTP connectivity by connecting and listing root directory.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "sftp_list_directory",
        "description": "List files and directories at a remote SFTP path.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "remote_path": {"type": "string", "description": "Remote path to list (default: /)", "default": "/"},
            },
            "required": [],
        },
    },
    {
        "name": "sftp_file_exists",
        "description": "Check whether a file exists at a remote SFTP path.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "remote_path": {"type": "string"},
            },
            "required": ["remote_path"],
        },
    },
    {
        "name": "sftp_find_files",
        "description": "List files in a remote directory matching a name pattern.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "remote_path": {"type": "string", "description": "Directory to search"},
                "pattern":     {"type": "string", "description": "Filename pattern to match (e.g. '*.csv')"},
            },
            "required": ["remote_path"],
        },
    },
]


def _build_connector():
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from connectors.sftp_mcp import SFTPConnector
    return SFTPConnector(
        host=os.environ["SFTP_HOST"],
        username=os.environ["SFTP_USERNAME"],
        password=os.environ.get("SFTP_PASSWORD"),
        port=int(os.environ.get("SFTP_PORT", 22)),
    )


def execute_tool(name: str, args: dict) -> dict:
    connector = _build_connector()
    try:
        if name == "sftp_health_check":
            return connector.health_check()

        elif name == "sftp_list_directory":
            path  = args.get("remote_path", "/")
            items = connector.list_directory(path)
            return {"path": path, "items": items, "count": len(items)}

        elif name == "sftp_file_exists":
            exists = connector.file_exists(args["remote_path"])
            return {"remote_path": args["remote_path"], "exists": exists}

        elif name == "sftp_find_files":
            import fnmatch
            path    = args["remote_path"]
            pattern = args.get("pattern", "*")
            items   = connector.list_directory(path)
            matched = [i for i in items if fnmatch.fnmatch(i["name"], pattern)]
            return {"path": path, "pattern": pattern, "matches": matched, "count": len(matched)}

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

    server = Server("flowforge-sftp")

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

"""
FlowForge — HTTP MCP Server

Env vars:
    HTTP_BASE_URL      — optional base URL prefix
    HTTP_BEARER_TOKEN  — optional Authorization: Bearer header
    HTTP_API_KEY       — optional X-API-Key header

Claude Desktop config:
    {
      "mcpServers": {
        "flowforge-http": {
          "command": "python",
          "args": ["-m", "mcp_servers.http_server"],
          "cwd": "/path/to/flowforge/backend",
          "env": { "HTTP_BASE_URL": "https://api.example.com", "HTTP_BEARER_TOKEN": "..." }
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
        "name": "http_get",
        "description": "Make an HTTP GET request and return status code + response body.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url":     {"type": "string", "description": "Full URL or path relative to HTTP_BASE_URL"},
                "headers": {"type": "object", "description": "Optional additional headers"},
                "params":  {"type": "object", "description": "Optional query parameters"},
            },
            "required": ["url"],
        },
    },
    {
        "name": "http_post",
        "description": "Make an HTTP POST request with a JSON body.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url":     {"type": "string"},
                "body":    {"type": "object", "description": "JSON body to send"},
                "headers": {"type": "object"},
            },
            "required": ["url"],
        },
    },
    {
        "name": "http_health_check",
        "description": "Check if a URL is reachable (GET request, expect 2xx).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "URL to check (default: /health)"},
            },
            "required": [],
        },
    },
    {
        "name": "http_check_json_field",
        "description": "GET a URL and assert a specific JSON field has an expected value.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url":            {"type": "string"},
                "field":          {"type": "string", "description": "Top-level JSON field name"},
                "expected_value": {"type": "string", "description": "Expected value (as string)"},
            },
            "required": ["url", "field"],
        },
    },
]


def _build_connector():
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from connectors.http_mcp import HTTPConnector
    return HTTPConnector(
        base_url=os.environ.get("HTTP_BASE_URL", ""),
        bearer_token=os.environ.get("HTTP_BEARER_TOKEN"),
        api_key=os.environ.get("HTTP_API_KEY"),
    )


def _safe_body(resp) -> dict:
    """Try to parse JSON body, fall back to text."""
    try:
        return {"json": resp.json()}
    except Exception:
        text = resp.text
        return {"text": text[:4000], "truncated": len(text) > 4000}


def execute_tool(name: str, args: dict) -> dict:
    connector = _build_connector()
    try:
        if name == "http_get":
            resp = connector.get(
                args["url"],
                headers=args.get("headers"),
                params=args.get("params"),
            )
            return {"status": resp.status_code, "url": resp.url, **_safe_body(resp)}

        elif name == "http_post":
            resp = connector.post(
                args["url"],
                json=args.get("body"),
                headers=args.get("headers"),
            )
            return {"status": resp.status_code, "url": resp.url, **_safe_body(resp)}

        elif name == "http_health_check":
            url = args.get("url", "/health")
            return connector.health_check(url)

        elif name == "http_check_json_field":
            resp  = connector.get(args["url"])
            body  = resp.json()
            field = args["field"]
            value = body.get(field)
            expected = args.get("expected_value")
            result = {
                "status":       resp.status_code,
                "field":        field,
                "actual_value": value,
            }
            if expected is not None:
                result["expected_value"] = expected
                result["match"]          = str(value) == str(expected)
            return result

        else:
            return {"error": f"Unknown tool: {name}"}

    except Exception as e:
        return {"error": str(e), "tool": name}


async def main():
    if not MCP_AVAILABLE:
        print("ERROR: mcp package not installed. Run: pip install mcp", file=sys.stderr)
        sys.exit(1)

    server = Server("flowforge-http")

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

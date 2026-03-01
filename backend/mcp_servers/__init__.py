"""
FlowForge MCP Servers

Each file in this directory is a standalone MCP server that can be run as:
    python -m mcp_servers.airflow_server
    python -m mcp_servers.sql_server
    ...

They communicate over stdio (MCP protocol) and can be used with:
  - Claude Desktop (add to claude_desktop_config.json)
  - Cursor / VS Code MCP extensions
  - Any MCP-compatible client

Credentials are injected via environment variables before the server starts.
The tool_registry module provides the same tools for inline execution inside
the FlowForge chat router (no subprocess needed).
"""

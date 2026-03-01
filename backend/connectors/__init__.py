"""
FlowForge MCP Connectors (v2)

FIX from v1: Connectors are imported lazily to avoid crashing if optional
dependencies (boto3, paramiko, azure-storage-blob) are not installed.
Each connector raises a clear RuntimeError at instantiation time if its
dependency is missing — not at import time.
"""

from .airflow_mcp import AirflowMCP
from .mssql_mcp   import MSSQLMCP
from .http_mcp    import HTTPConnector
from .s3_mcp      import S3Connector
from .azure_mcp   import AzureConnector
from .sftp_mcp    import SFTPConnector

__all__ = [
    "AirflowMCP",
    "MSSQLMCP",
    "HTTPConnector",
    "S3Connector",
    "AzureConnector",
    "SFTPConnector",
]

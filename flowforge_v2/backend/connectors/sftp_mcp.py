"""
FlowForge — SFTP MCP Connector
Wraps paramiko for SFTP file operations.
"""

import logging
import os
from typing import List

logger = logging.getLogger(__name__)

try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False
    logger.warning("paramiko not installed. Run: pip install paramiko")


class SFTPConnector:
    """MCP connector for SFTP file transfers."""

    def __init__(
        self,
        host: str,
        username: str,
        password: str = None,
        private_key_path: str = None,
        port: int = 22,
        timeout: int = 30,
        credential_name: str = None,
    ):
        if not PARAMIKO_AVAILABLE:
            raise RuntimeError("paramiko not installed.")

        self.host            = host
        self.username        = username
        self.password        = password
        self.private_key_path= private_key_path
        self.port            = port
        self.timeout         = timeout
        self.credential_name = credential_name
        self._transport      = None
        self._sftp           = None
        logger.info(f"SFTPConnector initialized: {username}@{host}:{port} (credential={credential_name})")

    def connect(self):
        self._transport = paramiko.Transport((self.host, self.port))
        self._transport.connect(
            username=self.username,
            password=self.password,
            pkey=paramiko.RSAKey.from_private_key_file(self.private_key_path) if self.private_key_path else None
        )
        self._sftp = paramiko.SFTPClient.from_transport(self._transport)
        logger.info(f"SFTP connected to {self.host}")

    def disconnect(self):
        if self._sftp:  self._sftp.close()
        if self._transport: self._transport.close()

    def upload(self, local_path: str, remote_path: str) -> str:
        if not self._sftp: self.connect()
        self._sftp.put(local_path, remote_path)
        logger.info(f"SFTP upload: {local_path} → {remote_path}")
        return remote_path

    def download(self, remote_path: str, local_path: str) -> str:
        if not self._sftp: self.connect()
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        self._sftp.get(remote_path, local_path)
        logger.info(f"SFTP download: {remote_path} → {local_path}")
        return local_path

    def list_directory(self, remote_path: str = "/") -> List[dict]:
        if not self._sftp: self.connect()
        items = []
        for attr in self._sftp.listdir_attr(remote_path):
            items.append({
                "name": attr.filename,
                "size": attr.st_size,
                "is_dir": bool(attr.st_mode & 0o40000),
            })
        return items

    def file_exists(self, remote_path: str) -> bool:
        if not self._sftp: self.connect()
        try:
            self._sftp.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def delete(self, remote_path: str):
        if not self._sftp: self.connect()
        self._sftp.remove(remote_path)

    def assert_file_exists(self, remote_path: str):
        if not self.file_exists(remote_path):
            raise AssertionError(f"SFTP file not found: {remote_path}")

    def health_check(self) -> dict:
        try:
            self.connect()
            self.list_directory("/")
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *a):
        self.disconnect()

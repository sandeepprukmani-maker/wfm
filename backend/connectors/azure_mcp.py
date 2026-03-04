"""
FlowForge — Azure Blob Storage MCP Connector
"""

import logging
import os
from typing import List

logger = logging.getLogger(__name__)

try:
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    logger.warning("azure-storage-blob not installed. Run: pip install azure-storage-blob")


class AzureConnector:
    """MCP connector for Azure Blob Storage."""

    def __init__(
        self,
        account_name: str = None,
        account_key: str = None,
        connection_string: str = None,
        credential_name: str = None,
    ):
        if not AZURE_AVAILABLE:
            raise RuntimeError("azure-storage-blob not installed.")

        self.credential_name = credential_name

        if connection_string:
            self._client = BlobServiceClient.from_connection_string(connection_string)
        elif account_name and account_key:
            self._client = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=account_key
            )
        else:
            raise ValueError("Provide connection_string or (account_name + account_key)")

        logger.info(f"AzureConnector initialized (credential={credential_name})")

    def list_containers(self) -> List[str]:
        return [c["name"] for c in self._client.list_containers()]

    def list_blobs(self, container: str, prefix: str = "") -> List[dict]:
        cc = self._client.get_container_client(container)
        blobs = []
        for b in cc.list_blobs(name_starts_with=prefix):
            blobs.append({"name": b.name, "size": b.size, "last_modified": str(b.last_modified)})
        return blobs

    def upload_file(self, container: str, blob_name: str, local_path: str) -> str:
        blob = self._client.get_blob_client(container=container, blob=blob_name)
        with open(local_path, "rb") as f:
            blob.upload_blob(f, overwrite=True)
        uri = f"https://{self._client.account_name}.blob.core.windows.net/{container}/{blob_name}"
        logger.info(f"Uploaded {local_path} → {uri}")
        return uri

    def upload_bytes(self, container: str, blob_name: str, data: bytes) -> str:
        blob = self._client.get_blob_client(container=container, blob=blob_name)
        blob.upload_blob(data, overwrite=True)
        return blob_name

    def download_file(self, container: str, blob_name: str, local_path: str) -> str:
        blob = self._client.get_blob_client(container=container, blob=blob_name)
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(blob.download_blob().readall())
        return local_path

    def blob_exists(self, container: str, blob_name: str) -> bool:
        return self._client.get_blob_client(container=container, blob=blob_name).exists()

    def delete_blob(self, container: str, blob_name: str):
        self._client.get_blob_client(container=container, blob=blob_name).delete_blob()

    def assert_blob_exists(self, container: str, blob_name: str):
        if not self.blob_exists(container, blob_name):
            raise AssertionError(f"Azure blob not found: {container}/{blob_name}")

    def health_check(self) -> dict:
        try:
            list(self._client.list_containers(max_results=1))
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

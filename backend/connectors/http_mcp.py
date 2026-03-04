"""
FlowForge — HTTP MCP Connector
Wraps requests library with retry, assertion, and logging.
"""

import logging
import time
from typing import Any, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class HTTPConnector:
    """Generic HTTP/REST connector with retry logic and assertion helpers."""

    def __init__(
        self,
        base_url: str = "",
        headers: Dict[str, str] = None,
        bearer_token: str = None,
        api_key: str = None,
        timeout: int = 30,
        retries: int = 3,
        verify_ssl: bool = True,
        credential_name: str = None,
    ):
        self.base_url        = base_url.rstrip("/")
        self.timeout         = timeout
        self.credential_name = credential_name

        self.session = requests.Session()

        # Retry strategy
        retry = Retry(
            total=retries,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

        # Default headers
        default_headers = headers or {}
        if bearer_token:
            default_headers["Authorization"] = f"Bearer {bearer_token}"
        if api_key:
            default_headers["X-API-Key"] = api_key
        self.session.headers.update(default_headers)
        self.session.verify = verify_ssl

    def request(
        self,
        method: str,
        url: str,
        headers: Dict = None,
        params: Dict = None,
        json: Any = None,
        data: Any = None,
    ) -> requests.Response:
        full_url = url if url.startswith("http") else f"{self.base_url}/{url.lstrip('/')}"
        logger.info(f"HTTP {method} {full_url}")
        resp = self.session.request(
            method.upper(), full_url,
            headers=headers, params=params,
            json=json, data=data,
            timeout=self.timeout,
        )
        logger.info(f"HTTP {resp.status_code} ← {full_url}")
        return resp

    def get(self, url: str, **kwargs)    -> requests.Response: return self.request("GET", url, **kwargs)
    def post(self, url: str, **kwargs)   -> requests.Response: return self.request("POST", url, **kwargs)
    def put(self, url: str, **kwargs)    -> requests.Response: return self.request("PUT", url, **kwargs)
    def delete(self, url: str, **kwargs) -> requests.Response: return self.request("DELETE", url, **kwargs)
    def patch(self, url: str, **kwargs)  -> requests.Response: return self.request("PATCH", url, **kwargs)

    def assert_status(self, response: requests.Response, expected: int):
        if response.status_code != expected:
            raise AssertionError(
                f"HTTP status mismatch: expected {expected}, got {response.status_code}\n"
                f"Body: {response.text[:500]}"
            )

    def assert_json_field(self, response: requests.Response, field: str, expected: Any):
        data = response.json()
        actual = data.get(field)
        if actual != expected:
            raise AssertionError(f"JSON field '{field}': expected {expected!r}, got {actual!r}")

    def health_check(self, url: str = "/health") -> dict:
        try:
            resp = self.get(url)
            return {"status": "ok", "http_status": resp.status_code}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def __enter__(self):  return self
    def __exit__(self, *a): self.session.close()

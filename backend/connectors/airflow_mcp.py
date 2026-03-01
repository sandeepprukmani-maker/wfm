"""
FlowForge — Airflow MCP Connector
Wraps Apache Airflow 2.7.x REST API

Supports:
- Trigger DAG runs
- Wait for completion (polling)
- Get run status & task logs
- Validate task execution
"""

import time
import logging
from typing import Optional
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class AirflowMCP:
    """
    MCP connector for Apache Airflow 2.7.x.
    All operations go through the Airflow stable REST API.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        credential_name: str = None,
        timeout: int = 30,
        verify_ssl: bool = True,
    ):
        self.base_url   = base_url.rstrip("/")
        self.auth       = HTTPBasicAuth(username, password)
        self.timeout    = timeout
        self.verify_ssl = verify_ssl
        self.session    = requests.Session()
        self.session.auth = self.auth
        self.session.verify = verify_ssl
        self.credential_name = credential_name
        logger.info(f"AirflowMCP initialized: {base_url} (credential={credential_name})")

    # ── Core API helpers ─────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    def _get(self, path: str, **kwargs):
        resp = self.session.get(self._url(path), timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: dict = None, **kwargs):
        resp = self.session.post(self._url(path), json=json, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, json: dict = None, **kwargs):
        resp = self.session.patch(self._url(path), json=json, timeout=self.timeout, **kwargs)
        resp.raise_for_status()
        return resp.json()

    # ── DAG Operations ───────────────────────────────────────────────────────

    def list_dags(self) -> list:
        """Return list of all DAGs."""
        data = self._get("/dags")
        return data.get("dags", [])

    def get_dag(self, dag_id: str) -> dict:
        """Get DAG metadata."""
        return self._get(f"/dags/{dag_id}")

    def trigger_dag(
        self,
        dag_id: str,
        conf: dict = None,
        logical_date: str = None,
        run_id: str = None,
    ) -> str:
        """
        Trigger a DAG run.
        Returns dag_run_id string.
        """
        body = {"conf": conf or {}}
        if logical_date:
            body["logical_date"] = logical_date
        if run_id:
            body["dag_run_id"] = run_id

        data = self._post(f"/dags/{dag_id}/dagRuns", json=body)
        dag_run_id = data["dag_run_id"]
        logger.info(f"Triggered DAG {dag_id} → run_id={dag_run_id}")
        return dag_run_id

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        """Get current state of a dag run."""
        return self._get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")

    def get_run_status(self, dag_id: str, dag_run_id: str) -> str:
        """Returns: queued | running | success | failed | skipped"""
        run = self.get_dag_run(dag_id, dag_run_id)
        return run.get("state", "unknown")

    def wait_for_completion(
        self,
        dag_id: str,
        dag_run_id: str,
        timeout: int = 3600,
        poll_interval: int = 5,
        expected_state: str = "success",
    ) -> str:
        """
        Poll until DAG reaches terminal state or timeout.
        Returns final state string.
        Raises TimeoutError on timeout.
        """
        terminal = {"success", "failed", "skipped"}
        deadline = time.time() + timeout
        last_state = None

        while time.time() < deadline:
            state = self.get_run_status(dag_id, dag_run_id)
            if state != last_state:
                logger.info(f"DAG {dag_id}/{dag_run_id} → {state}")
                last_state = state
            if state in terminal:
                return state
            time.sleep(poll_interval)

        raise TimeoutError(f"DAG {dag_id}/{dag_run_id} did not complete in {timeout}s")

    # ── Task Operations ──────────────────────────────────────────────────────

    def get_task_instances(self, dag_id: str, dag_run_id: str) -> list:
        """Get all task instances for a run."""
        data = self._get(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
        return data.get("task_instances", [])

    def get_task_log(self, dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> str:
        """Get log content for a specific task attempt."""
        resp = self.session.get(
            self._url(f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"),
            timeout=60,
        )
        resp.raise_for_status()
        return resp.text

    def validate_task(self, dag_id: str, dag_run_id: str, task_id: str) -> bool:
        """Returns True if task completed successfully."""
        tasks = self.get_task_instances(dag_id, dag_run_id)
        for t in tasks:
            if t["task_id"] == task_id:
                return t["state"] == "success"
        raise ValueError(f"Task {task_id} not found in run {dag_run_id}")

    def validate_logs_contain(self, dag_id: str, dag_run_id: str, task_id: str, keyword: str) -> bool:
        """Assert that a task's log contains a specific keyword."""
        log = self.get_task_log(dag_id, dag_run_id, task_id)
        return keyword in log

    # ── Health Check ─────────────────────────────────────────────────────────

    def health_check(self) -> dict:
        """Returns Airflow health status."""
        try:
            data = self._get("/health")
            return {"status": "ok", "details": data}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

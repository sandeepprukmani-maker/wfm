"""
FlowForge — Airflow MCP Connector
Full coverage of Apache Airflow 2.7.3 Stable REST API

DAG Operations:
  list_dags, get_dag, get_dag_details, get_dag_source,
  update_dag (pause/unpause), delete_dag,
  trigger_dag, get_dag_run, list_dag_runs, delete_dag_run,
  clear_dag_run, update_dag_run_state, wait_for_completion

Task Instance Operations:
  list_task_instances, get_task_instance, update_task_instance,
  clear_task_instance, get_task_log, list_task_logs

Variable Operations:
  list_variables, get_variable, set_variable, delete_variable,
  import_variables, export_variables

Connection Operations:
  list_connections, get_connection, create_connection,
  update_connection, delete_connection

Pool Operations:
  list_pools, get_pool, create_pool, update_pool, delete_pool

XCom Operations:
  get_xcom_entry, list_xcom_entries

DAG Warnings:
  list_dag_warnings

System:
  health_check, get_version, get_config
"""

import time
import logging
from typing import Any, Dict, List, Optional
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class AirflowMCP:
    """Full-coverage connector for Apache Airflow 2.7.3 REST API."""

    def __init__(self, base_url: str, username: str, password: str,
                 credential_name: str = None, timeout: int = 30,
                 verify_ssl: bool = True):
        self.base_url        = base_url.rstrip("/")
        self.auth            = HTTPBasicAuth(username, password)
        self.timeout         = timeout
        self.verify_ssl      = verify_ssl
        self.session         = requests.Session()
        self.session.auth    = self.auth
        self.session.verify  = verify_ssl
        self.credential_name = credential_name
        logger.info(f"AirflowMCP init: {base_url} (cred={credential_name})")

    # ── Core HTTP helpers ────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    def _get(self, path: str, params: dict = None) -> Any:
        r = self.session.get(self._url(path), params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, json: dict = None) -> Any:
        r = self.session.post(self._url(path), json=json or {}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _patch(self, path: str, json: dict = None) -> Any:
        r = self.session.patch(self._url(path), json=json or {}, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _delete(self, path: str) -> int:
        r = self.session.delete(self._url(path), timeout=self.timeout)
        r.raise_for_status()
        return r.status_code

    def _get_text(self, path: str, params: dict = None) -> str:
        r = self.session.get(self._url(path), params=params, timeout=60)
        r.raise_for_status()
        return r.text

    # ════════════════════════════════════════════════════════════════
    # DAG Operations
    # ════════════════════════════════════════════════════════════════

    def list_dags(self, limit: int = 100, offset: int = 0,
                  tags: List[str] = None, only_active: bool = True) -> List[dict]:
        params = {"limit": limit, "offset": offset, "only_active": only_active}
        if tags:
            params["tags"] = tags
        return self._get("/dags", params=params).get("dags", [])

    def get_dag(self, dag_id: str) -> dict:
        return self._get(f"/dags/{dag_id}")

    def get_dag_details(self, dag_id: str) -> dict:
        """Extended info including doc_md, default_args, params schema."""
        return self._get(f"/dags/{dag_id}/details")

    def get_dag_source(self, file_token: str) -> str:
        """Get Python source code of a DAG (file_token from get_dag response)."""
        return self._get_text(f"/dagSources/{file_token}")

    def update_dag(self, dag_id: str, is_paused: bool) -> dict:
        return self._patch(f"/dags/{dag_id}", {"is_paused": is_paused})

    def pause_dag(self, dag_id: str) -> dict:
        return self.update_dag(dag_id, is_paused=True)

    def unpause_dag(self, dag_id: str) -> dict:
        return self.update_dag(dag_id, is_paused=False)

    def delete_dag(self, dag_id: str) -> int:
        return self._delete(f"/dags/{dag_id}")

    def list_dag_warnings(self, dag_id: str = None) -> List[dict]:
        params = {}
        if dag_id:
            params["dag_id"] = dag_id
        return self._get("/dagWarnings", params=params).get("dag_warnings", [])

    # ── DAG Run Operations ───────────────────────────────────────────

    def trigger_dag(self, dag_id: str, conf: dict = None,
                    logical_date: str = None, run_id: str = None) -> dict:
        """Trigger a DAG run. Returns full dagRun object with dag_run_id."""
        body: dict = {"conf": conf or {}}
        if logical_date:
            body["logical_date"] = logical_date
        if run_id:
            body["dag_run_id"] = run_id
        data = self._post(f"/dags/{dag_id}/dagRuns", json=body)
        logger.info(f"Triggered {dag_id} -> {data.get('dag_run_id')}")
        return data

    def wait_if_running(self, dag_id: str, timeout: int = 3600,
                        poll_interval: int = 10) -> List[str]:
        """
        Pre-flight: if any runs of dag_id are currently running or queued,
        wait for ALL of them to reach a terminal state before returning.
        Returns list of dag_run_ids that were waited on.
        """
        active_states = {"running", "queued"}
        terminal      = {"success", "failed", "skipped"}
        deadline      = time.time() + timeout
        waited_on     = []

        while time.time() < deadline:
            runs = self.list_dag_runs(dag_id, limit=5, states=list(active_states))
            active = [r for r in runs if r.get("state") in active_states]
            if not active:
                break
            if not waited_on:
                waited_on = [r["dag_run_id"] for r in active]
                logger.info(f"DAG {dag_id}: waiting for {len(active)} active run(s) to finish")
            time.sleep(poll_interval)
        else:
            raise TimeoutError(f"DAG {dag_id} active runs did not finish within {timeout}s")

        return waited_on

    def get_dag_run(self, dag_id: str, dag_run_id: str) -> dict:
        return self._get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")

    def list_dag_runs(self, dag_id: str, limit: int = 10, offset: int = 0,
                      order_by: str = "-execution_date",
                      states: List[str] = None) -> List[dict]:
        params: dict = {"limit": limit, "offset": offset, "order_by": order_by}
        if states:
            params["state"] = states
        return self._get(f"/dags/{dag_id}/dagRuns", params=params).get("dag_runs", [])

    def delete_dag_run(self, dag_id: str, dag_run_id: str) -> int:
        return self._delete(f"/dags/{dag_id}/dagRuns/{dag_run_id}")

    def clear_dag_run(self, dag_id: str, dag_run_id: str,
                      dry_run: bool = False) -> dict:
        """Re-queue failed/success tasks in a run. dry_run previews what will be cleared."""
        return self._post(f"/dags/{dag_id}/dagRuns/{dag_run_id}/clear",
                          {"dry_run": dry_run})

    def update_dag_run_state(self, dag_id: str, dag_run_id: str,
                             state: str) -> dict:
        """Force set run state: queued | success | failed"""
        return self._patch(f"/dags/{dag_id}/dagRuns/{dag_run_id}", {"state": state})

    def get_run_status(self, dag_id: str, dag_run_id: str) -> str:
        return self.get_dag_run(dag_id, dag_run_id).get("state", "unknown")

    def wait_for_completion(self, dag_id: str, dag_run_id: str,
                            timeout: int = 3600, poll_interval: int = 10) -> str:
        """Poll until terminal state or timeout. Returns final state string."""
        terminal  = {"success", "failed", "skipped"}
        deadline  = time.time() + timeout
        last_state = None
        while time.time() < deadline:
            state = self.get_run_status(dag_id, dag_run_id)
            if state != last_state:
                logger.info(f"DAG {dag_id}/{dag_run_id} -> {state}")
                last_state = state
            if state in terminal:
                return state
            time.sleep(poll_interval)
        raise TimeoutError(
            f"DAG {dag_id}/{dag_run_id} did not complete in {timeout}s "
            f"(last state: {last_state})"
        )

    # ── Task Instance Operations ─────────────────────────────────────

    def list_task_instances(self, dag_id: str, dag_run_id: str) -> List[dict]:
        return self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        ).get("task_instances", [])

    # backward compat alias
    def get_task_instances(self, dag_id: str, dag_run_id: str) -> List[dict]:
        return self.list_task_instances(dag_id, dag_run_id)

    def get_task_instance(self, dag_id: str, dag_run_id: str, task_id: str) -> dict:
        return self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        )

    def update_task_instance(self, dag_id: str, dag_run_id: str,
                             task_id: str, state: str,
                             dry_run: bool = False) -> dict:
        """Force set task state: success | failed | skipped | up_for_retry | queued"""
        return self._patch(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}",
            {"state": state, "dry_run": dry_run}
        )

    def clear_task_instance(self, dag_id: str, dag_run_id: str,
                            task_id: str, dry_run: bool = False) -> dict:
        """Clear (re-queue) a specific task instance."""
        return self._post(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/clear",
            {"dry_run": dry_run, "task_ids": [task_id]}
        )

    def get_task_log(self, dag_id: str, dag_run_id: str,
                     task_id: str, try_number: int = 1,
                     full_content: bool = True) -> str:
        return self._get_text(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}",
            params={"full_content": full_content}
        )

    def list_task_logs(self, dag_id: str, dag_run_id: str,
                       task_id: str) -> List[dict]:
        """Get logs for all try_numbers of a task."""
        ti    = self.get_task_instance(dag_id, dag_run_id, task_id)
        tries = ti.get("try_number", 1)
        out   = []
        for n in range(1, tries + 1):
            try:
                log = self.get_task_log(dag_id, dag_run_id, task_id, n)
                out.append({"try_number": n, "log": log[:8000],
                             "truncated": len(log) > 8000})
            except Exception as e:
                out.append({"try_number": n, "error": str(e)})
        return out

    def validate_task(self, dag_id: str, dag_run_id: str, task_id: str) -> bool:
        return self.get_task_instance(dag_id, dag_run_id, task_id).get("state") == "success"

    def validate_logs_contain(self, dag_id: str, dag_run_id: str,
                               task_id: str, keyword: str) -> bool:
        return keyword in self.get_task_log(dag_id, dag_run_id, task_id)

    # ════════════════════════════════════════════════════════════════
    # Variable Operations
    # ════════════════════════════════════════════════════════════════

    def list_variables(self, limit: int = 100) -> List[dict]:
        return self._get("/variables", {"limit": limit}).get("variables", [])

    def get_variable(self, key: str) -> dict:
        return self._get(f"/variables/{key}")

    def set_variable(self, key: str, value: str, description: str = "") -> dict:
        """Create or update an Airflow Variable (upsert)."""
        try:
            return self._patch(f"/variables/{key}",
                               {"key": key, "value": str(value), "description": description})
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                return self._post("/variables",
                                  {"key": key, "value": str(value), "description": description})
            raise

    def delete_variable(self, key: str) -> int:
        return self._delete(f"/variables/{key}")

    def import_variables(self, variables: Dict[str, str]) -> List[dict]:
        return [self.set_variable(k, str(v)) for k, v in variables.items()]

    def export_variables(self) -> Dict[str, str]:
        return {v["key"]: v["value"] for v in self.list_variables()}

    # ════════════════════════════════════════════════════════════════
    # Connection Operations (Airflow Connections)
    # ════════════════════════════════════════════════════════════════

    def list_connections(self, limit: int = 100) -> List[dict]:
        return self._get("/connections", {"limit": limit}).get("connections", [])

    def get_connection(self, conn_id: str) -> dict:
        return self._get(f"/connections/{conn_id}")

    def create_connection(self, conn_id: str, conn_type: str,
                          host: str = "", port: int = None,
                          login: str = "", password: str = "",
                          schema: str = "", extra: str = "") -> dict:
        body: dict = {"connection_id": conn_id, "conn_type": conn_type,
                      "host": host, "login": login, "password": password,
                      "schema": schema, "extra": extra}
        if port is not None:
            body["port"] = port
        return self._post("/connections", body)

    def update_connection(self, conn_id: str, **fields) -> dict:
        return self._patch(f"/connections/{conn_id}", fields)

    def delete_connection(self, conn_id: str) -> int:
        return self._delete(f"/connections/{conn_id}")

    # ════════════════════════════════════════════════════════════════
    # Pool Operations
    # ════════════════════════════════════════════════════════════════

    def list_pools(self, limit: int = 100) -> List[dict]:
        return self._get("/pools", {"limit": limit}).get("pools", [])

    def get_pool(self, pool_name: str) -> dict:
        return self._get(f"/pools/{pool_name}")

    def create_pool(self, name: str, slots: int, description: str = "") -> dict:
        return self._post("/pools", {"name": name, "slots": slots, "description": description})

    def update_pool(self, name: str, slots: int, description: str = None) -> dict:
        body: dict = {"name": name, "slots": slots}
        if description is not None:
            body["description"] = description
        return self._patch(f"/pools/{name}", body)

    def delete_pool(self, pool_name: str) -> int:
        return self._delete(f"/pools/{pool_name}")

    # ════════════════════════════════════════════════════════════════
    # XCom Operations
    # ════════════════════════════════════════════════════════════════

    def get_xcom_entry(self, dag_id: str, dag_run_id: str,
                       task_id: str, xcom_key: str = "return_value",
                       deserialize: bool = True) -> dict:
        return self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}"
            f"/taskInstances/{task_id}/xcomEntries/{xcom_key}",
            {"deserialize": deserialize}
        )

    def list_xcom_entries(self, dag_id: str, dag_run_id: str,
                          task_id: str, limit: int = 20) -> List[dict]:
        return self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}"
            f"/taskInstances/{task_id}/xcomEntries",
            {"limit": limit}
        ).get("xcom_entries", [])

    # ════════════════════════════════════════════════════════════════
    # System / Config
    # ════════════════════════════════════════════════════════════════

    def health_check(self) -> dict:
        try:
            data = self._get("/health")
            return {"status": "ok", "details": data}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def get_version(self) -> dict:
        return self._get("/version")

    def get_config(self, section: str = None) -> dict:
        """Requires expose_config = True in airflow.cfg."""
        params = {}
        if section:
            params["section"] = section
        return self._get("/config", params=params)

    # ── Context manager ───────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

"""
FlowForge — PyTest Export Router (Sprint 2)

Sprint 2 upgrade: two export modes.

  /generate            — DSL-only (generic, no run history required)
  /generate-from-run   — Run-powered (embeds real dag_run_ids, SQL rows,
                         HTTP responses, captured logs from a specific run)
  /generate-zip        — Full CI package (test + conftest + .env + GitHub Actions)

The run-powered export is the differentiator over n8n:
  generated tests reference ACTUAL data from a real execution — not stubs.
"""

import io
import re
import json
import zipfile
import textwrap
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db, Workflow, WorkflowRun, NodeRun

router = APIRouter()


# ── Request models ────────────────────────────────────────────────────────────

class ExportFromDSLRequest(BaseModel):
    workflow_dsl:         dict
    credential_map:       Optional[Dict[str, Dict[str, Any]]] = None
    include_conftest:     bool = True
    include_requirements: bool = True
    include_github_ci:    bool = True


class ExportFromRunRequest(BaseModel):
    workflow_id:          str
    run_id:               Optional[str] = None   # latest run if omitted
    include_conftest:     bool = True
    include_requirements: bool = True
    include_github_ci:    bool = True


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_") or "workflow"


def _q(s: str) -> str:
    """Triple-quote a string safely inside Python source."""
    return repr(str(s))


def _json_literal(val: Any, indent: int = 8) -> str:
    pad = " " * indent
    return json.dumps(val, indent=4, default=str).replace("\n", "\n" + pad)


# ── DSL-only generator (Sprint 1 behaviour, improved) ────────────────────────

def _generate_from_dsl(dsl: dict, credential_map: dict = None) -> str:
    name      = dsl.get("name", "workflow")
    nodes     = dsl.get("nodes", [])
    safe      = _safe(name)
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    imports  = {"import os", "import pytest", "import logging", "import time"}
    steps    = []
    ind_tests= []

    for i, node in enumerate(nodes, 1):
        ntype = node.get("type", "")
        props = node.get("props", {})
        title = node.get("title", f"Node {i}")
        sn    = _safe(title)

        if ntype == "airflow":
            imports.add("from connectors.airflow_mcp import AirflowMCP")
            dag_id  = props.get("dag_id", "my_dag")
            timeout = props.get("timeout", 3600)
            validate_tasks = props.get("validate_tasks") or []
            assert_log     = props.get("assert_log_contains", "")
            vt_lines = ""
            if validate_tasks:
                vt_lines = (
                    f"    _task_instances = _af.get_task_instances({_q(dag_id)}, _run_id)\n"
                    f"    _task_states = {{ti['task_id']: ti['state'] for ti in _task_instances}}\n"
                )
                for vt in (validate_tasks if isinstance(validate_tasks, list) else []):
                    vt_lines += f"    assert _task_states.get({_q(vt)}) == 'success', f'Task {_q(vt)} not successful: {{_task_states}}'\n"
            log_lines = ""
            if assert_log:
                log_lines = (
                    f"    _all_logs = ' '.join(_af.get_task_log({_q(dag_id)}, _run_id, ti['task_id']) for ti in _af.get_task_instances({_q(dag_id)}, _run_id))\n"
                    f"    assert {_q(assert_log)} in _all_logs, f'Log missing: {_q(assert_log)}'\n"
                )
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}
                    _af = AirflowMCP(
                        base_url=os.environ['AIRFLOW_URL'],
                        username=os.environ['AIRFLOW_USER'],
                        password=os.environ['AIRFLOW_PASS'],
                    )
                    _run_id = _af.trigger_dag({_q(dag_id)})
                    ctx['dag_run_id_{i}'] = _run_id
                    _state = _af.wait_for_completion({_q(dag_id)}, _run_id, timeout={timeout})
                    assert _state == 'success', f'DAG {_q(dag_id)} ended: {{_state}}'
                """) + vt_lines + log_lines)
            ind_tests.append(textwrap.dedent(f"""\
                def test_{sn}_airflow_health():
                    af = AirflowMCP(
                        base_url=os.environ['AIRFLOW_URL'],
                        username=os.environ['AIRFLOW_USER'],
                        password=os.environ['AIRFLOW_PASS'],
                    )
                    h = af.health_check()
                    assert h['status'] == 'ok', f'Airflow unhealthy: {{h}}'
                    dags = [d['dag_id'] for d in af.list_dags()]
                    assert {_q(dag_id)} in dags, f'DAG {_q(dag_id)} missing. Got: {{dags}}'
                """))

        elif ntype == "sql":
            imports.add("from connectors.mssql_mcp import MSSQLMCP")
            query    = props.get("query", "SELECT 1")
            exp_rows = props.get("expected_row_count")
            min_rows = props.get("min_row_count")
            no_rows  = props.get("assert_no_rows", False)
            scalar   = props.get("assert_scalar")
            assertions = ""
            if exp_rows is not None:
                assertions += f"    assert len(_rows) == {int(exp_rows)}, f'Row count: expected {int(exp_rows)}, got {{len(_rows)}}'\n"
            if min_rows is not None:
                assertions += f"    assert len(_rows) >= {int(min_rows)}, f'Min rows: expected >={int(min_rows)}, got {{len(_rows)}}'\n"
            if no_rows:
                assertions += f"    assert len(_rows) == 0, f'Expected 0 rows, got {{len(_rows)}}. First: {{_rows[:3]}}'\n"
            if scalar is not None:
                assertions += f"    assert str(_rows[0][0]) == {_q(str(scalar))}, f'Scalar: expected {_q(str(scalar))}, got {{_rows[0][0]}}'\n"
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}
                    _db = MSSQLMCP(connection_string=os.environ['MSSQL_CONN'])
                    with _db:
                        _rows = _db.execute_query({_q(query)})
                    ctx['sql_rows_{i}'] = len(_rows)
                    assert _rows is not None, 'Query returned None'
                """) + assertions)
            ind_tests.append(textwrap.dedent(f"""\
                def test_{sn}_sql_connectivity():
                    db = MSSQLMCP(connection_string=os.environ['MSSQL_CONN'])
                    h = db.health_check()
                    assert h['status'] == 'ok', f'MSSQL unhealthy: {{h}}'
                """))

        elif ntype == "http":
            imports.add("from connectors.http_mcp import HTTPConnector")
            method = props.get("method", "GET")
            url    = props.get("url", "")
            status = props.get("expected_status", 200)
            body_c = props.get("assert_body_contains", "")
            jfield = props.get("assert_json_field", "")
            jval   = props.get("assert_json_value", "")
            extra  = ""
            if body_c:
                extra += f"    assert {_q(body_c)} in _resp.text, f'Body missing: {_q(body_c)}'\n"
            if jfield and jval:
                extra += (
                    f"    _json = _resp.json()\n"
                    f"    _field_val = _json\n"
                    f"    for _k in {_q(jfield)}.split('.'):\n"
                    f"        _field_val = _field_val.get(_k) if isinstance(_field_val, dict) else None\n"
                    f"    assert str(_field_val) == {_q(str(jval))}, f'JSON {_q(jfield)}={{_field_val!r}}, want {_q(str(jval))}'\n"
                )
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}
                    _http = HTTPConnector()
                    _resp = _http.request({_q(method)}, {_q(url)})
                    _http.assert_status(_resp, {status})
                    ctx['http_status_{i}'] = _resp.status_code
                """) + extra)
            ind_tests.append(textwrap.dedent(f"""\
                def test_{sn}_http_reachable():
                    http = HTTPConnector()
                    resp = http.request({_q(method)}, {_q(url)})
                    assert resp.status_code == {status}, f'Expected {status} got {{resp.status_code}}'
                """))

        elif ntype == "s3":
            imports.add("from connectors.s3_mcp import S3Connector")
            bucket = props.get("bucket", "")
            key    = props.get("key", "")
            region = props.get("region", "us-east-1")
            op     = props.get("operation", "list")
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}
                    _s3 = S3Connector(
                        access_key_id=os.environ.get('S3_KEY_ID'),
                        secret_access_key=os.environ.get('S3_SECRET'),
                        region={_q(region)},
                    )
                    _s3_result = _s3.{'object_exists' if op=='exists' else 'list_objects'}({_q(bucket)}{', '+_q(key) if op=='exists' else ''})
                    ctx['s3_{i}'] = _s3_result
                """))
            ind_tests.append(textwrap.dedent(f"""\
                def test_{sn}_s3_health():
                    s3 = S3Connector(
                        access_key_id=os.environ.get('S3_KEY_ID'),
                        secret_access_key=os.environ.get('S3_SECRET'),
                        region={_q(region)},
                    )
                    assert s3.health_check()['status'] == 'ok'
                """))

        elif ntype == "azure":
            imports.add("from connectors.azure_mcp import AzureConnector")
            container = props.get("container", "")
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}
                    _az = AzureConnector(connection_string=os.environ['AZURE_CONN'])
                    _az_blobs = _az.list_blobs({_q(container)})
                    ctx['azure_blobs_{i}'] = len(_az_blobs)
                """))
            ind_tests.append(textwrap.dedent(f"""\
                def test_{sn}_azure_health():
                    az = AzureConnector(connection_string=os.environ['AZURE_CONN'])
                    assert az.health_check()['status'] == 'ok'
                """))

        elif ntype == "sftp":
            imports.add("from connectors.sftp_mcp import SFTPConnector")
            remote = props.get("remote_path", "/")
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}
                    _sftp = SFTPConnector(
                        host=os.environ['SFTP_HOST'],
                        username=os.environ['SFTP_USER'],
                        password=os.environ.get('SFTP_PASS', ''),
                    )
                    with _sftp:
                        _sftp_items = _sftp.list_directory({_q(remote)})
                    ctx['sftp_items_{i}'] = len(_sftp_items)
                """))

        elif ntype == "wait":
            dur  = props.get("duration", 30)
            unit = props.get("unit", "seconds")
            secs = float(dur) * (60 if unit == "minutes" else 3600 if unit == "hours" else 1)
            steps.append(f"    # ── Step {i}: {title}\n    time.sleep({secs})\n")

        elif ntype == "set_variable":
            key   = props.get("key", "")
            value = props.get("value", "")
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}
                    ctx['var_{_safe(key)}'] = {_q(str(value))}
                """))

        elif ntype == "get_variable":
            key     = props.get("key", "")
            default = props.get("default", "")
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}
                    _var_{_safe(key)} = ctx.get('var_{_safe(key)}', {_q(str(default))})
                    ctx['retrieved_{_safe(key)}'] = _var_{_safe(key)}
                """))

        elif ntype == "code":
            code = props.get("code", "output['result'] = None")
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title} (inline code)
                    _code_output = {{}}
                    _code_input  = dict(ctx)
                    exec(
                        {repr(code)},
                        {{'__builtins__': {{}}, 'json': __import__('json'), 'datetime': __import__('datetime').datetime}},
                        {{'input_data': _code_input, 'output': _code_output, 'log': print}}
                    )
                    ctx['code_output_{i}'] = _code_output
                """))
            ind_tests.append(textwrap.dedent(f"""\
                def test_{sn}_code_syntax():
                    \"\"\"Verify inline code compiles without errors.\"\"\"
                    code = {repr(code)}
                    compile(code, '<code_node>', 'exec')  # raises SyntaxError if broken
                """))

        elif ntype == "call_workflow":
            sub_id = props.get("workflow_id", "")
            steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title} (sub-workflow)
                    # NOTE: call_workflow runs sub-workflow {_q(sub_id)} inline.
                    # In standalone tests, verify the sub-workflow independently.
                    ctx['call_workflow_{i}_skipped'] = True
                """))

    import_block = "\n".join(sorted(imports))
    steps_block  = "\n".join(
        ("    " + ln if not ln.startswith("    ") else ln)
        for block in steps for ln in block.splitlines()
    ) if steps else "    pass  # no executable nodes"
    ind_block = "\n\n".join(ind_tests)

    return f'''\
"""
Auto-generated by FlowForge  (DSL-only mode)
Workflow  : {name}
Generated : {timestamp}
Nodes     : {len(nodes)}

STANDALONE — does NOT require FlowForge to run.
Directly invokes MCP connectors against your real services.

Quick start:
    pip install -r requirements.txt
    cp .env.example .env  &&  vi .env
    pytest tests/test_{safe}.py -v
    pytest tests/test_{safe}.py -v -k "node_1 or airflow"
"""

{import_block}

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def _logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@pytest.fixture(scope="module")
def ctx() -> dict:
    """Shared context — passes run IDs, row counts, etc. between steps."""
    return {{}}


# ────────────────────────────────────────────────────────────────────────────────
# End-to-end workflow test
# ────────────────────────────────────────────────────────────────────────────────

def test_{safe}_e2e(ctx):
    """
    Full end-to-end: {name}
    Runs {len(nodes)} node(s) in sequence.
    """
{steps_block}


# ────────────────────────────────────────────────────────────────────────────────
# Individual node smoke tests
# ────────────────────────────────────────────────────────────────────────────────

{ind_block}
'''


# ── Run-powered generator — the Sprint 2 differentiator ──────────────────────

def _generate_from_run(
    wf: Workflow,
    run: WorkflowRun,
    node_runs: List[NodeRun],
) -> str:
    """
    Generate pytest that embeds REAL data from a specific execution:
      - Airflow: actual dag_run_id, real task states from that run
      - SQL: real row_count, real column names, real sample rows
      - HTTP: real status code, real response body snapshot
      - Code: actual output dict from the run
      - Variables: real values set during the run
    """
    name      = wf.name
    safe      = _safe(name)
    run_id    = run.id
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    run_at    = run.started_at.strftime("%Y-%m-%d %H:%M UTC") if run.started_at else "unknown"
    nodes     = wf.dsl.get("nodes", [])
    nr_map    = {nr.node_id: nr for nr in node_runs}

    imports  = {"import os", "import pytest", "import logging", "import json"}
    sections: List[str] = []   # individual test functions
    e2e_steps: List[str] = []  # steps for the e2e test

    for i, node in enumerate(nodes, 1):
        ntype = node.get("type", "")
        props = node.get("props", {})
        title = node.get("title", f"Node {i}")
        sn    = _safe(title)
        nr    = nr_map.get(node["id"])
        od    = (nr.output_data or {}) if nr else {}   # real output_data from this run

        if ntype == "airflow":
            imports.add("from connectors.airflow_mcp import AirflowMCP")
            dag_id       = props.get("dag_id", od.get("dag_id", "unknown"))
            real_run_id  = od.get("dag_run_id", "")
            final_state  = od.get("final_state", "success")
            task_summary = od.get("task_summary", {})
            task_logs    = od.get("task_logs", {})
            timeout      = props.get("timeout", 3600)

            # Embed real task states as assertions
            task_assertions = ""
            for tid, tinfo in (task_summary.items() if isinstance(task_summary, dict) else []):
                state = tinfo.get("state", "success") if isinstance(tinfo, dict) else str(tinfo)
                task_assertions += (
                    f"        assert _task_states.get({_q(tid)}) == {_q(state)}, "
                    f"f'Task {_q(tid)}: expected {_q(state)}, got {{_task_states.get({_q(tid)})}}'\n"
                )
            # Embed real log keyword hints from captured logs
            log_hints = ""
            for tid, log_txt in (task_logs.items() if isinstance(task_logs, dict) else []):
                lines = [l.strip() for l in str(log_txt).splitlines() if l.strip() and "INFO" not in l[:20]]
                if lines:
                    # Pick last meaningful log line as a real-world assertion hint
                    snippet = lines[-1][:80].replace("'", "\\'")
                    log_hints += f"    # Real log from {tid} (run {real_run_id}): {snippet}\n"

            e2e_steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}  [from run {run_id}]
                    _af_{i} = AirflowMCP(
                        base_url=os.environ['AIRFLOW_URL'],
                        username=os.environ['AIRFLOW_USER'],
                        password=os.environ['AIRFLOW_PASS'],
                    )
                    _run_id_{i} = _af_{i}.trigger_dag({_q(dag_id)})
                    ctx['dag_run_id_{i}'] = _run_id_{i}
                    _state_{i} = _af_{i}.wait_for_completion({_q(dag_id)}, _run_id_{i}, timeout={timeout})
                    assert _state_{i} == {_q(final_state)}, f'DAG ended: {{_state_{i}}}'
                """))

            sections.append(textwrap.dedent(f"""\
                # ── Node {i}: {title} ─────────────────────────────────────────────────────────
                # Derived from run {run_id} at {run_at}
                # DAG run ID was: {real_run_id!r}
                # Final state  : {final_state!r}
                # Task states  : {json.dumps(task_summary, default=str)}

                {log_hints}
                def test_{sn}_dag_exists():
                    af = AirflowMCP(
                        base_url=os.environ['AIRFLOW_URL'],
                        username=os.environ['AIRFLOW_USER'],
                        password=os.environ['AIRFLOW_PASS'],
                    )
                    dags = [d['dag_id'] for d in af.list_dags()]
                    assert {_q(dag_id)} in dags, f'DAG {_q(dag_id)} not found. Got: {{dags}}'


                def test_{sn}_dag_states():
                    \"\"\"Verifies each task hits the same state observed in run {run_id}.\"\"\"
                    af = AirflowMCP(
                        base_url=os.environ['AIRFLOW_URL'],
                        username=os.environ['AIRFLOW_USER'],
                        password=os.environ['AIRFLOW_PASS'],
                    )
                    run_id = af.trigger_dag({_q(dag_id)})
                    state  = af.wait_for_completion({_q(dag_id)}, run_id, timeout={timeout})
                    assert state == {_q(final_state)}, f'Final state {{state!r}}, expected {_q(final_state)!r}'
                    task_instances = af.get_task_instances({_q(dag_id)}, run_id)
                    _task_states   = {{ti['task_id']: ti['state'] for ti in task_instances}}
                {textwrap.indent(task_assertions, '    ')}
                """))

        elif ntype == "sql":
            imports.add("from connectors.mssql_mcp import MSSQLMCP")
            query        = props.get("query", od.get("query", "SELECT 1"))
            real_rows    = od.get("rows_returned", 0)
            real_cols    = od.get("columns", [])
            real_sample  = od.get("rows_sample", [])
            real_qt      = od.get("query_time_s", 0)

            # Build column validation from real run
            col_check = ""
            if real_cols:
                col_check = (
                    f"    assert _cols == {repr(real_cols)}, "
                    f"f'Column mismatch: expected {repr(real_cols)}, got {{_cols}}'\n"
                )
            # Embed first real row as snapshot
            snapshot = ""
            if real_sample:
                snapshot = (
                    f"\n    # Snapshot: first row from run {run_id}\n"
                    f"    EXPECTED_FIRST_ROW = {repr(real_sample[0])}\n"
                    f"    if _rows:\n"
                    f"        # Uncomment to assert exact data snapshot:\n"
                    f"        # assert _rows[0] == EXPECTED_FIRST_ROW, f'Row 0 changed: {{_rows[0]}}'\n"
                    f"        pass  # data snapshot available above\n"
                )

            e2e_steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}  [from run {run_id}]
                    # Reference execution returned {real_rows} row(s) in {real_qt}s
                    _db_{i} = MSSQLMCP(connection_string=os.environ['MSSQL_CONN'])
                    with _db_{i}:
                        _rows_{i} = _db_{i}.execute_query({_q(query)})
                    ctx['sql_rows_{i}'] = len(_rows_{i})
                    assert len(_rows_{i}) == {real_rows}, f'Row count: expected {real_rows}, got {{len(_rows_{i})}}'
                """))

            sections.append(textwrap.dedent(f"""\
                # ── Node {i}: {title} ─────────────────────────────────────────────────────────
                # Derived from run {run_id} at {run_at}
                # Rows returned : {real_rows}
                # Columns       : {real_cols}
                # Query time    : {real_qt}s
                # Sample row 0  : {repr(real_sample[0]) if real_sample else 'N/A'}

                def test_{sn}_sql_row_count():
                    \"\"\"Assert exact row count seen in run {run_id}: {real_rows} rows.\"\"\"
                    db = MSSQLMCP(connection_string=os.environ['MSSQL_CONN'])
                    with db:
                        rows = db.execute_query({_q(query)})
                        _cols = db.last_column_names
                    assert len(rows) == {real_rows}, f'Row count: expected {real_rows}, got {{len(rows)}}'
                {col_check}
                {snapshot}

                def test_{sn}_sql_perf():
                    \"\"\"Query should complete in under {max(real_qt * 3, 5):.1f}s (3× the {real_qt}s seen in run {run_id}).\"\"\"
                    import time as _t
                    db = MSSQLMCP(connection_string=os.environ['MSSQL_CONN'])
                    with db:
                        t0 = _t.time()
                        db.execute_query({_q(query)})
                        elapsed = _t.time() - t0
                    assert elapsed < {max(real_qt * 3, 5):.1f}, f'Query too slow: {{elapsed:.2f}}s'
                """))

        elif ntype == "http":
            imports.add("from connectors.http_mcp import HTTPConnector")
            method       = props.get("method", "GET")
            url          = props.get("url", "")
            real_status  = od.get("status_code", props.get("expected_status", 200))
            real_rt      = od.get("response_time", 0)
            real_resp    = od.get("response", {})
            body_snippet = json.dumps(real_resp, default=str)[:300] if real_resp else ""

            e2e_steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}  [from run {run_id}]
                    _http_{i} = HTTPConnector()
                    _resp_{i} = _http_{i}.request({_q(method)}, {_q(url)})
                    _http_{i}.assert_status(_resp_{i}, {real_status})
                    ctx['http_status_{i}'] = _resp_{i}.status_code
                """))

            sections.append(textwrap.dedent(f"""\
                # ── Node {i}: {title} ─────────────────────────────────────────────────────────
                # Derived from run {run_id} at {run_at}
                # Status code  : {real_status}
                # Response time: {real_rt}s
                # Response (truncated): {body_snippet!r}

                def test_{sn}_http_status():
                    \"\"\"Assert HTTP {real_status} as observed in run {run_id}.\"\"\"
                    http = HTTPConnector()
                    resp = http.request({_q(method)}, {_q(url)})
                    assert resp.status_code == {real_status}, f'Status: expected {real_status}, got {{resp.status_code}}'


                def test_{sn}_http_perf():
                    \"\"\"Response should be under {max(real_rt * 3, 2):.1f}s (3× the {real_rt}s from run {run_id}).\"\"\"
                    import time as _t
                    http = HTTPConnector()
                    t0 = _t.time()
                    http.request({_q(method)}, {_q(url)})
                    elapsed = _t.time() - t0
                    assert elapsed < {max(real_rt * 3, 2):.1f}, f'Too slow: {{elapsed:.2f}}s'
                """))

        elif ntype == "code":
            imports.add("import ast")
            code        = props.get("code", "")
            real_output = od.get("output", od)
            elapsed     = od.get("elapsed_seconds", 0)

            e2e_steps.append(textwrap.dedent(f"""\
                    # ── Step {i}: {title}  [from run {run_id}]
                    # Previous run output: {repr(real_output)[:120]}
                    _code_ns = {{'input_data': dict(ctx), 'output': {{}}, 'log': print,
                                  'json': __import__('json'), 'datetime': __import__('datetime').datetime}}
                    exec({repr(code)}, {{'__builtins__': {{}}}}, _code_ns)
                    ctx['code_output_{i}'] = _code_ns['output']
                """))

            real_out_repr = repr(real_output)[:500]
            sections.append(textwrap.dedent(f"""\
                # ── Node {i}: {title} ─────────────────────────────────────────────────────────
                # Derived from run {run_id} at {run_at}
                # Execution time : {elapsed}s
                # Output keys    : {list(real_output.keys()) if isinstance(real_output, dict) else 'N/A'}
                # Output snapshot: {real_out_repr}

                def test_{sn}_code_syntax():
                    \"\"\"Code must compile cleanly (no syntax errors).\"\"\"
                    code = {repr(code)}
                    try:
                        compile(code, '<code_node>', 'exec')
                    except SyntaxError as e:
                        pytest.fail(f'Code syntax error: {{e}}')


                def test_{sn}_code_output_keys():
                    \"\"\"Code output should contain same keys as run {run_id}.\"\"\"
                    EXPECTED_KEYS = {repr(sorted(real_output.keys()) if isinstance(real_output, dict) else [])}
                    ns = {{'input_data': {{}}, 'output': {{}}, 'log': lambda *a: None,
                           'json': __import__('json'), 'datetime': __import__('datetime').datetime}}
                    exec({repr(code)}, {{'__builtins__': {{}}}}, ns)
                    assert sorted(ns['output'].keys()) == EXPECTED_KEYS, \\
                        f'Output keys changed: {{sorted(ns[\"output\"].keys())}}'
                """))

        elif ntype in ("set_variable", "get_variable"):
            key   = props.get("key", "")
            value = od.get("value")
            e2e_steps.append(
                f"    # ── Step {i}: {title} — variable ${key!r} = {value!r}\n"
                f"    ctx['var_{_safe(key)}'] = {repr(value)}\n"
            )

    import_block = "\n".join(sorted(imports))
    e2e_block = "\n".join(
        ("    " + ln if not ln.startswith("    ") else ln)
        for block in e2e_steps for ln in block.splitlines()
    ) if e2e_steps else "    pass  # no executable nodes"

    sections_block = "\n\n".join(sections)

    return f'''\
"""
Auto-generated by FlowForge  (Run-powered mode)
Workflow   : {name}
Run ID     : {run_id}
Executed at: {run_at}
Generated  : {timestamp}
Nodes      : {len(nodes)}

STANDALONE — does NOT require FlowForge.
All assertions derived from REAL execution data — not stubs.

Quick start:
    pip install -r requirements.txt
    cp .env.example .env  &&  vi .env
    pytest tests/test_{safe}.py -v
    pytest tests/test_{safe}.py -v -k "sql or airflow"
    pytest tests/test_{safe}.py -v --html=report.html
"""

{import_block}

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def _logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@pytest.fixture(scope="module")
def ctx() -> dict:
    """Shared context — passes run IDs, row counts, etc. between steps."""
    return {{}}


# SOURCE RUN METADATA
RUN_ID         = {_q(run_id)}
RUN_STATUS     = {_q(run.status or 'unknown')}
RUN_DURATION_S = {run.duration_seconds or 0}
RUN_TRIGGERED  = {_q(run.triggered_by or 'manual')}


# ────────────────────────────────────────────────────────────────────────────────
# End-to-end test  (replicates run {run_id})
# ────────────────────────────────────────────────────────────────────────────────

def test_{safe}_e2e(ctx):
    """
    End-to-end: {name}
    Replicates run {run_id} — all assertions derived from real execution data.
    """
{e2e_block}


# ────────────────────────────────────────────────────────────────────────────────
# Per-node tests  (each runnable independently, no FlowForge needed)
# ────────────────────────────────────────────────────────────────────────────────

{sections_block}
'''


# ── CI artefacts ──────────────────────────────────────────────────────────────

def _env_example() -> str:
    return (
        "# FlowForge — Environment Variables\n"
        "# Copy to .env and fill in real values\n\n"
        "AIRFLOW_URL=http://localhost:8080\n"
        "AIRFLOW_USER=admin\n"
        "AIRFLOW_PASS=admin\n\n"
        "MSSQL_CONN=DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=mydb;UID=sa;PWD=pass\n\n"
        "S3_KEY_ID=AKIAIOSFODNN7EXAMPLE\n"
        "S3_SECRET=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n"
        "S3_REGION=us-east-1\n\n"
        "AZURE_CONN=DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net\n\n"
        "SFTP_HOST=sftp.example.com\nSFTP_USER=user\nSFTP_PASS=password\nSFTP_PORT=22\n"
    )


def _conftest(wf_name: str) -> str:
    return (
        f'"""FlowForge conftest — {wf_name}"""\n'
        "import pytest, logging, os\n\n"
        "def pytest_configure(config):\n"
        "    logging.basicConfig(level=logging.INFO)\n\n"
        "@pytest.fixture(scope='session')\n"
        "def env():\n"
        "    class _E:\n"
        "        def require(self, k):\n"
        "            v = os.environ.get(k)\n"
        "            if v is None: raise EnvironmentError(f'Missing env var: {k}')\n"
        "            return v\n"
        "    return _E()\n"
    )


def _requirements() -> str:
    return (
        "pytest>=7.4.0\npytest-html>=4.0.0\nrequests>=2.31.0\n"
        "pyodbc>=5.0.0\nboto3>=1.34.0\nparamiko>=3.4.0\n"
        "azure-storage-blob>=12.19.0\nopenpyxl>=3.1.0\ncryptography>=42.0.0\n"
    )


def _github_ci(wf_name: str, safe: str) -> str:
    return (
        f"name: FlowForge — {wf_name}\n\n"
        "on:\n  push:\n    branches: [main, develop]\n  pull_request:\n"
        "  schedule:\n    - cron: '0 6 * * *'\n\n"
        "jobs:\n  workflow-test:\n    runs-on: ubuntu-latest\n    steps:\n"
        "      - uses: actions/checkout@v4\n"
        "      - uses: actions/setup-python@v5\n        with:\n          python-version: '3.11'\n"
        "      - run: pip install -r requirements.txt\n"
        "      - name: Run FlowForge tests\n        env:\n"
        "          AIRFLOW_URL: ${{ secrets.AIRFLOW_URL }}\n"
        "          AIRFLOW_USER: ${{ secrets.AIRFLOW_USER }}\n"
        "          AIRFLOW_PASS: ${{ secrets.AIRFLOW_PASS }}\n"
        "          MSSQL_CONN: ${{ secrets.MSSQL_CONN }}\n"
        "          S3_KEY_ID: ${{ secrets.S3_KEY_ID }}\n"
        "          S3_SECRET: ${{ secrets.S3_SECRET }}\n"
        "          AZURE_CONN: ${{ secrets.AZURE_CONN }}\n"
        "          SFTP_HOST: ${{ secrets.SFTP_HOST }}\n"
        "          SFTP_USER: ${{ secrets.SFTP_USER }}\n"
        "          SFTP_PASS: ${{ secrets.SFTP_PASS }}\n"
        f"        run: |\n"
        f"          pytest tests/test_{safe}.py -v --tb=short --html=report.html --junitxml=results.xml\n"
        "      - uses: actions/upload-artifact@v4\n        if: always()\n        with:\n"
        f"          name: test-results\n          path: |\n            results.xml\n            report.html\n"
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/generate", response_class=PlainTextResponse)
async def generate_from_dsl(body: ExportFromDSLRequest):
    """Generate pytest from DSL only (no run history needed)."""
    code = _generate_from_dsl(body.workflow_dsl, body.credential_map)
    safe = _safe(body.workflow_dsl.get("name", "workflow"))
    return PlainTextResponse(
        code, media_type="text/x-python",
        headers={"Content-Disposition": f'attachment; filename="test_{safe}.py"'},
    )


@router.post("/generate-from-run", response_class=PlainTextResponse)
async def generate_from_run(
    body:         ExportFromRunRequest,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """
    Generate pytest embedding REAL data from a specific execution.
    If run_id is omitted, the most recent successful run is used.
    """
    wf = db.query(Workflow).filter_by(id=body.workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")

    if body.run_id:
        run = db.query(WorkflowRun).filter_by(id=body.run_id, workflow_id=body.workflow_id).first()
        if not run:
            raise HTTPException(404, f"Run {body.run_id!r} not found for this workflow")
    else:
        # Latest successful run
        run = (
            db.query(WorkflowRun)
            .filter_by(workflow_id=body.workflow_id, status="success")
            .order_by(WorkflowRun.started_at.desc())
            .first()
        )
        if not run:
            # Fall back to any run
            run = (
                db.query(WorkflowRun)
                .filter_by(workflow_id=body.workflow_id)
                .order_by(WorkflowRun.started_at.desc())
                .first()
            )
        if not run:
            raise HTTPException(
                404,
                "No executions found for this workflow. "
                "Run the workflow at least once before exporting run-powered tests, "
                "or use /generate for DSL-only tests."
            )

    node_runs = (
        db.query(NodeRun)
        .filter_by(workflow_run_id=run.id)
        .order_by(NodeRun.started_at)
        .all()
    )
    code = _generate_from_run(wf, run, node_runs)
    safe = _safe(wf.name)
    return PlainTextResponse(
        code, media_type="text/x-python",
        headers={"Content-Disposition": f'attachment; filename="test_{safe}_run_{run.id}.py"'},
    )


@router.post("/generate-zip")
async def generate_zip(
    body:         ExportFromRunRequest,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """
    Download a complete CI-ready ZIP package.
    Uses run-powered test if executions exist, else DSL-only.
    Package contains:
      tests/test_<name>.py     — the test file (run-powered if possible)
      tests/conftest.py
      requirements.txt
      .env.example
      .github/workflows/flowforge-tests.yml
      README.md
    """
    wf = db.query(Workflow).filter_by(id=body.workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")

    safe     = _safe(wf.name)
    run      = None
    run_mode = "dsl"

    if body.run_id:
        run = db.query(WorkflowRun).filter_by(id=body.run_id, workflow_id=body.workflow_id).first()
        if run: run_mode = "run"
    else:
        run = (
            db.query(WorkflowRun)
            .filter_by(workflow_id=body.workflow_id, status="success")
            .order_by(WorkflowRun.started_at.desc())
            .first()
        )
        if run: run_mode = "run"

    if run_mode == "run":
        node_runs = (
            db.query(NodeRun)
            .filter_by(workflow_run_id=run.id)
            .order_by(NodeRun.started_at)
            .all()
        )
        code = _generate_from_run(wf, run, node_runs)
    else:
        code = _generate_from_dsl(wf.dsl)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"tests/test_{safe}.py",            code)
        zf.writestr("tests/conftest.py",                _conftest(wf.name))
        zf.writestr("requirements.txt",                 _requirements())
        zf.writestr(".env.example",                     _env_example())
        zf.writestr(".github/workflows/flowforge-tests.yml", _github_ci(wf.name, safe))
        zf.writestr("README.md", (
            f"# FlowForge Test Package — {wf.name}\n\n"
            f"Mode: **{'Run-powered' if run_mode=='run' else 'DSL-only'}**"
            + (f"  (from run `{run.id}`)" if run else "") + "\n\n"
            f"Generated by FlowForge Sprint 2.\n\n"
            f"## Quick start\n\n```bash\npip install -r requirements.txt\n"
            f"cp .env.example .env  # fill in real credentials\n"
            f"pytest tests/test_{safe}.py -v\n"
            f"pytest tests/test_{safe}.py -v --html=report.html\n```\n\n"
            f"## Run a single node test\n\n```bash\n"
            f"pytest tests/ -k 'airflow or sql' -v\n```\n"
        ))
    buf.seek(0)
    return StreamingResponse(
        buf, media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="flowforge_{safe}_tests.zip"'},
    )

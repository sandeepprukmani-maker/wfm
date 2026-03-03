"""
FlowForge — Workflow Execution Engine v4 (Sprint 2)

Sprint 2 additions:
  - set_variable / get_variable nodes  — persistent key-value store per workflow
  - code node                          — inline Python execution in sandbox
  - call_workflow node                 — execute a sub-workflow inline, chain output
  - Error workflow routing             — on failure, fire Workflow.settings.error_workflow_id
  - All blocking I/O in asyncio.to_thread (_t helper) — unchanged from Sprint 1
"""

import asyncio
import builtins
import functools
import json
import logging
import re
import textwrap
import time
import traceback
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Thread-pool helper ────────────────────────────────────────────────────────

async def _t(func, *args, **kwargs):
    """Run a blocking function in the default thread-pool executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))


# ── Handler registry ──────────────────────────────────────────────────────────

_HANDLERS: Dict[str, Any] = {}

def node_handler(t):
    def dec(f):
        _HANDLERS[t] = f
        return f
    return dec


# ── Node Logger ───────────────────────────────────────────────────────────────

def _ts():
    return datetime.utcnow().strftime("%H:%M:%S")

class NodeLogger:
    def __init__(self):
        self._lines = []

    def info(self, msg):  self._lines.append(f"[{_ts()}] [INFO]  {msg}")
    def ok(self, msg):    self._lines.append(f"[{_ts()}] [OK]    {msg}"); logger.info(msg)
    def warn(self, msg):  self._lines.append(f"[{_ts()}] [WARN]  {msg}"); logger.warning(msg)
    def error(self, msg): self._lines.append(f"[{_ts()}] [ERROR] {msg}"); logger.error(msg)

    def section(self, title):
        sep = "─" * 52
        self._lines += [f"\n{sep}", f"  {title}", sep]

    def raw(self, text, prefix="  │ "):
        for line in str(text).splitlines():
            self._lines.append(f"{prefix}{line}")

    def stdout(self):
        return "\n".join(self._lines)


# ── Expression Resolver ───────────────────────────────────────────────────────

class ExpressionResolver:
    def __init__(self, node_outputs, workflow_name="", execution_id="", variables=None):
        self._out  = node_outputs
        self._wfn  = workflow_name
        self._eid  = execution_id
        self._vars = variables or {}

    def resolve(self, value):
        if not isinstance(value, str) or "{{" not in value:
            return value
        return re.sub(r"\{\{(.+?)\}\}", self._sub, value)

    def _sub(self, m):
        expr = m.group(1).strip()
        try:
            # $node.Title.output.field
            n = re.match(r"\$node\.(.+?)\.output\.(.+)", expr)
            if n:
                out = self._out.get(n.group(1), {})
                for p in n.group(2).split("."):
                    out = out.get(p, "") if isinstance(out, dict) else ""
                return str(out)
            # $var.key_name
            v = re.match(r"\$var\.(.+)", expr)
            if v:
                return str(self._vars.get(v.group(1), ""))
            if expr == "$workflow.name": return self._wfn
            if expr == "$execution.id":  return self._eid
        except Exception:
            pass
        return m.group(0)

    def resolve_props(self, props):
        return {k: self.resolve(v) for k, v in props.items()}


# ── Topological Sort ──────────────────────────────────────────────────────────

def topo_sort(nodes, edges):
    from collections import defaultdict, deque
    ids   = {n["id"] for n in nodes}
    indeg = {i: 0 for i in ids}
    adj   = defaultdict(list)
    for e in edges:
        s = e.get("from") or e.get("source")
        d = e.get("to")   or e.get("target")
        if s in ids and d in ids:
            adj[s].append(d)
            indeg[d] += 1
    q, res = deque(i for i in indeg if indeg[i] == 0), []
    while q:
        n = q.popleft(); res.append(n)
        for nb in adj[n]:
            indeg[nb] -= 1
            if indeg[nb] == 0: q.append(nb)
    if len(res) < len(ids):
        cycle_nodes = ids - set(res)
        raise ValueError(
            f"Workflow contains a cycle — nodes {cycle_nodes} form a loop and cannot execute. "
            f"Remove the circular edges in the editor."
        )
    return res


# ── Variable helpers ──────────────────────────────────────────────────────────

def _load_variables(db, owner_id: str, workflow_id: str) -> Dict[str, Any]:
    """Load all variables (workflow-scoped + global) into a flat dict."""
    from database import WorkflowVariable
    rows = db.query(WorkflowVariable).filter(
        WorkflowVariable.owner_id == owner_id,
        WorkflowVariable.workflow_id.in_([workflow_id, None])
    ).all()
    return {r.key: r.value for r in rows}


def _set_variable(db, owner_id: str, workflow_id: str, key: str,
                  value: Any, scope: str = "workflow") -> None:
    """Upsert a variable in the database."""
    from database import WorkflowVariable
    wf_id = workflow_id if scope == "workflow" else None
    row = db.query(WorkflowVariable).filter_by(
        owner_id=owner_id, workflow_id=wf_id, key=key
    ).first()
    if row:
        row.value      = value
        row.updated_at = datetime.utcnow()
    else:
        row = WorkflowVariable(owner_id=owner_id, workflow_id=wf_id, key=key, value=value)
        db.add(row)
    db.commit()


# ── Main Engine ───────────────────────────────────────────────────────────────

class WorkflowEngine:
    def __init__(self, db_session=None, credential_manager=None):
        self._db    = db_session
        self._creds = credential_manager

    async def execute(
        self,
        workflow_id: str,
        owner_id: str,
        run_id: str,
        trigger_data: dict = None,
        _depth: int = 0,             # call_workflow recursion guard
    ) -> dict:
        from database import Workflow, WorkflowRun, NodeRun

        if _depth > 5:
            raise RecursionError("call_workflow depth limit (5) exceeded")

        wf = self._db.query(Workflow).filter_by(id=workflow_id).first()
        if not wf:
            raise ValueError(f"Workflow {workflow_id!r} not found")

        dsl   = wf.dsl
        nodes = {n["id"]: n for n in dsl.get("nodes", [])}
        edges = dsl.get("edges", [])
        order = topo_sort(list(nodes.values()), edges)

        run = self._db.query(WorkflowRun).filter_by(id=run_id).first()
        if not run:
            raise ValueError(f"Run {run_id!r} not found")
        run.status     = "running"
        run.started_at = datetime.utcnow()
        self._db.commit()

        # Load persisted variables for expression resolution
        variables    = _load_variables(self._db, owner_id, workflow_id)
        node_outputs: Dict[str, Any] = {}
        if trigger_data:
            node_outputs["__trigger_data"] = trigger_data

        failed        = False
        fail_error    = None
        fail_node     = None

        # ── Branch-aware execution setup ──────────────────────────────────
        # Build two indexes from the edge list:
        #   _branch_in[node_id]  = [{source, branch}]  — labelled incoming edges
        #   _all_in[node_id]     = [source_id, ...]    — all incoming edges
        #
        # A node is SUPPRESSED (skipped) when all its incoming edges carry branch
        # labels and none of those labels match the IF branch that actually fired.
        # Suppression cascades: a node whose every parent is suppressed is also
        # suppressed even if it has no branch label of its own.
        # Suppressed nodes are written to the DB as status='skipped' so the UI
        # can display them greyed-out in the run timeline.

        from collections import defaultdict as _dd
        _branch_in: dict = _dd(list)   # target_id -> [{source, branch}]
        _all_in:    dict = _dd(list)   # target_id -> [source_id, ...]
        _if_outputs: dict = {}         # if_node_id -> 'true'|'false'
        suppressed: set  = set()       # node IDs to skip this run

        for _e in edges:
            _src = _e.get('from') or _e.get('source', '')
            _dst = _e.get('to')   or _e.get('target', '')
            if not _src or not _dst:
                continue
            _all_in[_dst].append(_src)
            if _e.get('branch') in ('true', 'false'):
                _branch_in[_dst].append({'source': _src, 'branch': _e['branch']})

        def _is_suppressed(_nid: str) -> bool:
            _parents = _all_in.get(_nid, [])
            # Cascade: all parents suppressed -> suppress this node too
            if _parents and all(_p in suppressed for _p in _parents):
                return True
            # Branch gate: only applies when ALL incoming edges are labelled
            _bi = _branch_in.get(_nid, [])
            if not _bi:
                return False
            _branch_sources = {_b['source'] for _b in _bi}
            if any(_p not in _branch_sources for _p in _parents):
                return False  # has at least one unconditional parent -> always run
            # Check if any labelled edge's branch actually fired
            for _b in _bi:
                _src_node  = nodes.get(_b['source'], {})
                _src_title = _src_node.get('title', _b['source'])
                _actual    = _if_outputs.get(_b['source']) or \
                             (node_outputs.get(_src_title) or {}).get('branch')
                if _actual == _b['branch']:
                    return False  # this edge fired -> run the node
            return True           # no matching branch -> suppress
        # ──────────────────────────────────────────────────────────────────

        for node_id in order:
            node = nodes.get(node_id)
            if not node:
                continue

            # ── Skip suppressed branch nodes ──────────────────────────────
            if _is_suppressed(node_id):
                suppressed.add(node_id)
                nr_skip = NodeRun(
                    workflow_run_id=run_id,
                    node_id=node_id,
                    node_type=node['type'],
                    node_title=node.get('title', node_id),
                    status='skipped',
                    started_at=datetime.utcnow(),
                    completed_at=datetime.utcnow(),
                    duration_seconds=0,
                    stdout_log='[skipped — branch condition not met]',
                )
                self._db.add(nr_skip)
                self._db.commit()
                continue
            # ─────────────────────────────────────────────────────────────

            props     = node.get("props", {})
            on_fail   = props.get("on_failure", "stop")
            max_retry = int(props.get("retries", 0))

            nr = NodeRun(
                workflow_run_id=run_id,
                node_id=node_id,
                node_type=node["type"],
                node_title=node.get("title", node_id),
                status="running",
                started_at=datetime.utcnow(),
                attempt=1,
            )
            self._db.add(nr)
            self._db.commit()

            resolver = ExpressionResolver(node_outputs, wf.name, run_id, variables)
            rnode    = {**node, "props": resolver.resolve_props(props)}
            nlog     = NodeLogger()
            t0       = time.time()
            output   = None
            last_err = None

            for attempt in range(max_retry + 1):
                nr.attempt = attempt + 1
                if attempt > 0:
                    wait = 2 ** attempt
                    nlog.warn(f"Retry {attempt + 1}/{max_retry + 1} — waiting {wait}s")
                    await asyncio.sleep(wait)
                try:
                    handler = _HANDLERS.get(node["type"])
                    if handler:
                        # Pass engine reference for call_workflow and variable nodes
                        output = await handler(
                            rnode, self._creds, owner_id,
                            node_outputs, nlog,
                            engine=self, db=self._db, workflow_id=workflow_id, depth=_depth,
                        )
                    else:
                        nlog.warn(f"No handler for node type {node['type']!r}")
                        output = {"warning": f"no handler for {node['type']}"}
                    last_err = None
                    break
                except Exception as exc:
                    last_err = exc
                    nlog.error(f"Attempt {attempt + 1} failed: {exc}")

            nr.duration_seconds = round(time.time() - t0, 3)
            nr.completed_at     = datetime.utcnow()

            if last_err:
                nlog.error(f"Node FAILED after {nr.attempt} attempt(s)")
                nr.status        = "failed"
                nr.error_message = str(last_err)
                nr.stdout_log    = nlog.stdout()
                self._db.commit()
                failed     = True
                fail_error = last_err
                fail_node  = node.get("title", node_id)
                if on_fail == "stop":
                    break
            else:
                nlog.ok(f"Completed in {nr.duration_seconds}s")
                nr.status      = "success"
                nr.output_data = output or {}
                nr.stdout_log  = nlog.stdout()
                # If set_variable handler updated variables dict, reload
                if node["type"] == "set_variable" and output:
                    variables[output.get("key", "")] = output.get("value")
                # Capture IF branch result for downstream suppression logic
                if node["type"] == "if" and output:
                    _if_outputs[node_id] = output.get("branch", "")
                node_outputs[node.get("title", node_id)] = output or {}
                self._db.commit()

        run.status           = "failed" if failed else "success"
        run.completed_at     = datetime.utcnow()
        run.duration_seconds = (run.completed_at - run.started_at).total_seconds()
        self._db.commit()

        # ── Error workflow routing ────────────────────────────────────────────
        if failed and _depth == 0:
            error_wf_id = (wf.settings or {}).get("error_workflow_id")
            if error_wf_id:
                await self._fire_error_workflow(
                    error_wf_id, owner_id, run_id, workflow_id,
                    wf.name, str(fail_error), fail_node,
                )

        return {
            "run_id":   run_id,
            "status":   run.status,
            "duration": run.duration_seconds,
            "outputs":  node_outputs,
        }

    async def _fire_error_workflow(
        self, error_wf_id: str, owner_id: str,
        failed_run_id: str, failed_wf_id: str,
        failed_wf_name: str, error_msg: str, failed_node: str,
    ):
        """Fire the error-handler workflow with failure context as trigger_data."""
        import uuid
        from database import WorkflowRun, Workflow
        err_wf = self._db.query(Workflow).filter_by(id=error_wf_id).first()
        if not err_wf:
            logger.warning(f"Error workflow {error_wf_id!r} not found — skipping")
            return
        err_run_id = f"run-{str(uuid.uuid4())[:8]}"
        err_run = WorkflowRun(
            id=err_run_id,
            workflow_id=error_wf_id,
            triggered_by="error_handler",
            trigger_data={
                "failed_run_id":  failed_run_id,
                "failed_workflow": failed_wf_name,
                "failed_node":    failed_node,
                "error_message":  error_msg,
                "timestamp":      datetime.utcnow().isoformat(),
            },
            status="pending",
            started_at=datetime.utcnow(),
        )
        self._db.add(err_run)
        self._db.commit()
        logger.info(f"Firing error workflow {error_wf_id!r} → run {err_run_id}")
        try:
            await self.execute(error_wf_id, owner_id, err_run_id, _depth=1)
        except Exception as e:
            logger.error(f"Error workflow {error_wf_id!r} itself failed: {e}")

    async def stream_execution(self, execution_id: str) -> AsyncGenerator[dict, None]:
        from database import WorkflowRun, NodeRun
        seen = set()
        for _ in range(720):
            run = self._db.query(WorkflowRun).filter_by(id=execution_id).first()
            if not run:
                yield {"type": "error", "message": f"Execution {execution_id!r} not found"}
                return
            for nr in self._db.query(NodeRun).filter_by(workflow_run_id=execution_id).all():
                key = (nr.node_id, nr.status, nr.attempt)
                if key not in seen:
                    seen.add(key)
                    yield {
                        "type":     "node_update",
                        "node_id":  nr.node_id,
                        "status":   nr.status,
                        "duration": nr.duration_seconds,
                        "attempt":  nr.attempt,
                        "log":      (nr.stdout_log or "")[-1200:],
                    }
            if run.status in ("success", "failed", "cancelled"):
                yield {
                    "type":     "run_complete",
                    "run_id":   run.id,
                    "status":   run.status,
                    "duration": run.duration_seconds,
                }
                return
            await asyncio.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# NODE HANDLERS
# ─────────────────────────────────────────────────────────────────────────────
# All handlers have signature:
#   async def handle_X(node, creds, owner_id, ctx, nlog, *, engine, db, workflow_id, depth)

@node_handler("trigger")
async def handle_trigger(node, creds, owner_id, ctx, nlog, **kw):
    props = node.get("props", {})
    nlog.info(f"Trigger fired — cron={props.get('cron','manual')} tz={props.get('timezone','UTC')}")
    return {"triggered_at": datetime.utcnow().isoformat(), "cron": props.get("cron", "manual")}


@node_handler("webhook")
async def handle_webhook(node, creds, owner_id, ctx, nlog, **kw):
    payload = ctx.get("__trigger_data", {})
    nlog.info(f"Webhook payload keys: {list(payload.keys()) if payload else '(none)'}")
    return {"webhook_received": True, "payload": payload}


@node_handler("wait")
async def handle_wait(node, creds, owner_id, ctx, nlog, **kw):
    props   = node.get("props", {})
    dur     = float(props.get("duration", 30))
    unit    = props.get("unit", "seconds")
    seconds = dur * (60 if unit == "minutes" else 3600 if unit == "hours" else 1)
    nlog.info(f"Waiting {dur} {unit} ({seconds:.0f}s)…")
    await asyncio.sleep(min(seconds, 300))
    nlog.ok("Wait complete")
    return {"waited_seconds": seconds}


@node_handler("if")
async def handle_if(node, creds, owner_id, ctx, nlog, **kw):
    props  = node.get("props", {})
    left   = str(props.get("left_value", ""))
    right  = str(props.get("right_value", ""))
    op     = props.get("operator", "equals")
    nlog.info(f"IF: [{left}] {op} [{right}]")
    result = False
    try:
        if   op == "equals":       result = left.lower() == right.lower()
        elif op == "not_equals":   result = left.lower() != right.lower()
        elif op == "contains":     result = right in left
        elif op == "greater_than": result = float(left or 0) > float(right or 0)
        elif op == "less_than":    result = float(left or 0) < float(right or 0)
        elif op == "is_empty":     result = not left.strip()
        elif op == "not_empty":    result = bool(left.strip())
    except (ValueError, TypeError) as e:
        nlog.warn(f"Comparison error: {e}")
    branch = "true" if result else "false"
    nlog.ok(f"Result: {result} → branch={branch}")
    if props.get("assert_true") and not result:
        raise AssertionError(f"IF assertion: [{left}] {op} [{right}] = false (expected true)")
    if props.get("assert_false") and result:
        raise AssertionError(f"IF assertion: [{left}] {op} [{right}] = true (expected false)")
    return {"branch": branch, "condition_result": result, "left": left, "right": right}


# ── SET VARIABLE ─────────────────────────────────────────────────────────────

@node_handler("set_variable")
async def handle_set_variable(node, creds, owner_id, ctx, nlog, **kw):
    props      = node.get("props", {})
    key        = props.get("key", "").strip()
    value      = props.get("value", "")
    scope      = props.get("scope", "workflow")     # workflow | global
    cast       = props.get("cast", "auto")          # auto | string | number | boolean | json

    if not key:
        raise ValueError("set_variable node missing 'key'")

    db          = kw.get("db")
    workflow_id = kw.get("workflow_id")

    # Cast value
    typed_value = _cast_value(value, cast)
    nlog.section(f"Set Variable: {key}")
    nlog.info(f"Scope : {scope}")
    nlog.info(f"Cast  : {cast}")
    nlog.info(f"Value : {typed_value!r}")

    if db:
        await _t(_set_variable, db, owner_id, workflow_id, key, typed_value, scope)
    nlog.ok(f"Variable ${key} = {typed_value!r} saved ({scope} scope)")
    return {"key": key, "value": typed_value, "scope": scope}


# ── GET VARIABLE ─────────────────────────────────────────────────────────────

@node_handler("get_variable")
async def handle_get_variable(node, creds, owner_id, ctx, nlog, **kw):
    props       = node.get("props", {})
    key         = props.get("key", "").strip()
    default_val = props.get("default", None)
    assert_set  = props.get("assert_set", False)

    if not key:
        raise ValueError("get_variable node missing 'key'")

    db          = kw.get("db")
    workflow_id = kw.get("workflow_id")

    nlog.section(f"Get Variable: {key}")

    value = None
    if db:
        from database import WorkflowVariable
        row = db.query(WorkflowVariable).filter(
            WorkflowVariable.owner_id == owner_id,
            WorkflowVariable.key == key,
            WorkflowVariable.workflow_id.in_([workflow_id, None])
        ).order_by(WorkflowVariable.workflow_id.desc()).first()
        value = row.value if row else default_val
    else:
        value = default_val

    if assert_set and value is None:
        nlog.error(f"FAIL: variable ${key} is not set")
        raise AssertionError(f"Variable ${key} is not set and assert_set=true")

    nlog.ok(f"${key} = {value!r}")
    return {"key": key, "value": value, "found": value is not None}


def _cast_value(value: Any, cast: str) -> Any:
    """Cast a value to the requested type."""
    if cast == "number":
        try:    return float(value) if "." in str(value) else int(value)
        except: return 0
    elif cast == "boolean":
        return str(value).lower() in ("true", "1", "yes", "on")
    elif cast == "json":
        if isinstance(value, (dict, list)): return value
        try:    return json.loads(value)
        except: return value
    elif cast == "string":
        return str(value)
    else:  # auto
        if isinstance(value, (dict, list, bool, int, float)): return value
        s = str(value).strip()
        if s.lower() == "true":  return True
        if s.lower() == "false": return False
        try:    return int(s)
        except: pass
        try:    return float(s)
        except: pass
        try:    return json.loads(s)
        except: pass
        return s


# ── CODE NODE ────────────────────────────────────────────────────────────────

@node_handler("code")
async def handle_code(node, creds, owner_id, ctx, nlog, **kw):
    """
    Execute inline Python code in a restricted sandbox.
    The code has access to:
      - input_data: dict  — merged outputs of all upstream nodes
      - output: dict      — write your results here, they flow downstream
    Example:
        total = sum(int(r['count']) for r in input_data.get('rows_sample', []))
        output['total'] = total
        output['status'] = 'ok' if total > 0 else 'empty'
    """
    props   = node.get("props", {})
    code    = props.get("code", "output['result'] = 'no code provided'")
    timeout = int(props.get("timeout", 30))
    lang    = props.get("language", "python")

    if lang != "python":
        raise NotImplementedError(f"Code node only supports Python (got {lang!r})")

    nlog.section("Code Node — Python")
    nlog.info(f"Timeout : {timeout}s")
    nlog.info(f"Code ({len(code)} chars):")
    nlog.raw(textwrap.indent(code, "  "))

    # Build safe namespace
    safe_globals = {
        "__builtins__": {
            k: getattr(builtins, k) for k in [
                "print","len","range","enumerate","zip","map","filter","sorted",
                "list","dict","set","tuple","str","int","float","bool","bytes",
                "sum","min","max","abs","round","any","all","isinstance","type",
                "repr","format","vars","dir","hasattr","getattr","setattr",
                "Exception","ValueError","TypeError","KeyError","IndexError",
            ]
        },
        "json": json,
        "datetime": datetime,
        "re": re,
    }
    safe_locals = {
        "input_data": dict(ctx),
        "output": {},
        "log": nlog.info,
    }

    def _run_code():
        exec(compile(code, "<code_node>", "exec"), safe_globals, safe_locals)  # noqa: S102
        return dict(safe_locals["output"])

    nlog.info("Executing…")
    t0 = time.time()
    try:
        output = await asyncio.wait_for(_t(_run_code), timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutError(f"Code node timed out after {timeout}s")
    except Exception as e:
        tb = traceback.format_exc()
        nlog.error(f"Code error: {e}")
        nlog.raw(tb)
        raise RuntimeError(f"Code node error: {e}") from e

    elapsed = round(time.time() - t0, 3)
    nlog.ok(f"Executed in {elapsed}s → output keys: {list(output.keys())}")
    nlog.section("Output")
    nlog.raw(json.dumps(output, indent=2, default=str)[:1000])
    return {"output": output, "elapsed_seconds": elapsed, **output}


# ── CALL WORKFLOW ─────────────────────────────────────────────────────────────

@node_handler("call_workflow")
async def handle_call_workflow(node, creds, owner_id, ctx, nlog, **kw):
    """
    Execute another workflow inline and wait for its result.
    The called workflow's final node_outputs are merged into this node's output.
    """
    import uuid
    from database import Workflow, WorkflowRun

    props       = node.get("props", {})
    sub_wf_id   = props.get("workflow_id", "")
    input_data  = props.get("input_data", {})   # passed as trigger_data
    fail_parent = props.get("fail_on_error", True)
    engine      = kw.get("engine")
    db          = kw.get("db")
    depth       = kw.get("depth", 0)

    if not sub_wf_id:
        raise ValueError("call_workflow node missing 'workflow_id'")
    if not engine or not db:
        raise RuntimeError("call_workflow requires engine and db context")

    sub_wf = db.query(Workflow).filter_by(id=sub_wf_id).first()
    if not sub_wf:
        raise ValueError(f"Sub-workflow {sub_wf_id!r} not found")

    nlog.section(f"Call Workflow: {sub_wf.name}")
    nlog.info(f"Sub-workflow ID : {sub_wf_id}")
    nlog.info(f"Input data keys : {list(input_data.keys()) if input_data else '(none)'}")
    nlog.info(f"Call depth      : {depth + 1}")

    sub_run_id = f"run-{str(uuid.uuid4())[:8]}"
    sub_run = WorkflowRun(
        id=sub_run_id,
        workflow_id=sub_wf_id,
        triggered_by="call_workflow",
        trigger_data={**ctx, **(input_data or {})},
        status="pending",
        started_at=datetime.utcnow(),
    )
    db.add(sub_run)
    db.commit()

    nlog.info(f"Sub-run created: {sub_run_id}")

    result = await engine.execute(
        sub_wf_id, owner_id, sub_run_id,
        trigger_data=input_data or {},
        _depth=depth + 1,
    )

    if result["status"] == "failed" and fail_parent:
        nlog.error(f"Sub-workflow {sub_wf.name!r} failed → failing parent node")
        raise RuntimeError(f"Sub-workflow {sub_wf.name!r} failed (run={sub_run_id})")

    nlog.ok(f"Sub-workflow finished: {result['status']} in {result['duration']:.1f}s")
    sub_outputs = result.get("outputs", {})
    nlog.info(f"Output keys from sub-workflow: {list(sub_outputs.keys())}")

    return {
        "sub_workflow_id":   sub_wf_id,
        "sub_workflow_name": sub_wf.name,
        "sub_run_id":        sub_run_id,
        "sub_status":        result["status"],
        "sub_duration":      result["duration"],
        "sub_outputs":       sub_outputs,
    }


# ── AIRFLOW ───────────────────────────────────────────────────────────────────

@node_handler("airflow")
async def handle_airflow(node, creds, owner_id, ctx, nlog, **kw):
    props   = node.get("props", {})
    cred    = props.get("credential", "")
    dag_id  = props.get("dag_id", "")
    timeout = int(props.get("timeout", 3600))
    conf    = props.get("conf") or {}

    validate_tasks         = props.get("validate_tasks") or []
    validate_logs          = props.get("validate_logs") or {}
    assert_log_contains    = props.get("assert_log_contains", "")
    assert_no_failed_tasks = props.get("assert_no_failed_tasks", True)
    # validate_task_patterns: list of name prefixes — all matching tasks must succeed
    # e.g. ["QA"] → all tasks starting with "QA" must be success/skipped

    if not dag_id: raise ValueError("Airflow node missing dag_id")
    if not cred:   raise ValueError("Airflow node missing credential")

    # Read wait behaviour from node props (set by DSL generator)
    wait_for_completion = bool(props.get("wait_for_completion", True))
    wait_if_running     = bool(props.get("wait_if_running", False))

    nlog.section(f"Airflow DAG: {dag_id}")
    nlog.info(f"Credential: {cred}  Timeout: {timeout}s  wait={wait_for_completion}  preflight={wait_if_running}")
    connector = creds.build_connector(cred, owner_id)

    # Pre-flight: if dag is already running, wait for it before triggering a new run
    if wait_if_running:
        nlog.info(f"Pre-flight: checking for active runs of {dag_id!r}…")
        try:
            waited = await _t(connector.wait_if_running, dag_id, timeout=timeout)
            if waited:
                nlog.ok(f"Pre-flight: {len(waited)} prior run(s) finished, now triggering")
            else:
                nlog.info("Pre-flight: DAG is idle, triggering immediately")
        except Exception as pf_err:
            nlog.warn(f"Pre-flight check failed (continuing anyway): {pf_err}")

    # trigger_dag returns a dict {dag_run_id, dag_id, state, ...} — extract the run ID
    run_data   = await _t(connector.trigger_dag, dag_id, conf=conf)
    dag_run_id = run_data.get("dag_run_id") if isinstance(run_data, dict) else str(run_data)
    nlog.ok(f"Run created: {dag_run_id}")

    t0          = time.time()
    final_state = run_data.get("state", "queued") if isinstance(run_data, dict) else "queued"
    elapsed     = 0.0

    if wait_for_completion:
        nlog.section("Polling for completion")
        final_state = await _t(connector.wait_for_completion, dag_id, dag_run_id, timeout=timeout)
        elapsed     = round(time.time() - t0, 1)
        nlog.info(f"Final state: {final_state}  ({elapsed}s)")
    else:
        nlog.info(f"Not waiting for completion (wait_for_completion=False) — state: {final_state}")

    nlog.section("Task Instances")
    task_instances = await _t(connector.get_task_instances, dag_id, dag_run_id)
    task_summary   = {}
    for ti in task_instances:
        tid   = ti.get("task_id", "?")
        state = ti.get("state", "?")
        dur   = ti.get("duration") or 0
        nlog.info(f"  {tid:<40s}  state={state:<12s}  {dur:.1f}s")
        task_summary[tid] = {"state": state, "duration": dur}

    if assert_no_failed_tasks:
        bad = [t for t, v in task_summary.items()
               if v["state"] not in ("success", "skipped", "upstream_failed", "none")]
        if bad:
            nlog.error(f"FAIL: failed tasks: {bad}")
            raise AssertionError(f"DAG has failed tasks: {bad}")
        nlog.ok(f"PASS: no failed tasks ({len(task_summary)} total)")

    nlog.section("Task Logs")
    all_task_logs = {}
    for ti in task_instances:
        tid = ti.get("task_id", "?")
        nlog.info(f"\n--- Task: {tid} ---")
        try:
            log_text = await _t(connector.get_task_log, dag_id, dag_run_id, tid)
            all_task_logs[tid] = log_text
            lines = log_text.splitlines()
            nlog.raw("\n".join(lines[:100]))
            if len(lines) > 100: nlog.raw(f"… ({len(lines) - 100} more lines)")
        except Exception as e:
            nlog.warn(f"Could not fetch log for {tid}: {e}")
            all_task_logs[tid] = f"(unavailable: {e})"

    if validate_tasks:
        nlog.section("Task State Assertions")
        for vtask in validate_tasks:
            state = task_summary.get(vtask, {}).get("state", "not_found")
            if state == "success": nlog.ok(f"PASS: {vtask!r} == success")
            else:
                nlog.error(f"FAIL: {vtask!r} state={state!r}")
                raise AssertionError(f"Task {vtask!r}: expected success, got {state!r}")

    if validate_logs:
        nlog.section("Log Content Assertions")
        for vtask, keyword in validate_logs.items():
            log = all_task_logs.get(vtask, "")
            if keyword in log: nlog.ok(f"PASS: {vtask!r} log contains {keyword!r}")
            else:
                nlog.error(f"FAIL: {vtask!r} log missing {keyword!r}")
                raise AssertionError(f"Task {vtask!r} log missing: {keyword!r}")

    if assert_log_contains:
        nlog.section("Global Log Assertion")
        found = any(assert_log_contains in log for log in all_task_logs.values())
        if found: nlog.ok(f"PASS: a task log contains {assert_log_contains!r}")
        else:
            nlog.error(f"FAIL: no task log contains {assert_log_contains!r}")
            raise AssertionError(f"No task log contained: {assert_log_contains!r}")

    # ── Task Pattern Validation ───────────────────────────────────────────────
    # validate_task_pattern: check all tasks whose names start with / match a prefix
    # e.g. "QA" → all tasks named QA_check, QA_validate, QA_* must have state=success
    # Outputs: {prefix}_tasks_all_passed (bool), {prefix}_tasks (list), {prefix}_failed_tasks
    pattern_results = {}
    validate_task_patterns = props.get("validate_task_patterns") or []
    if isinstance(validate_task_patterns, str):
        validate_task_patterns = [validate_task_patterns]

    if validate_task_patterns:
        nlog.section("Task Pattern Validation")
        for pattern in validate_task_patterns:
            pat_upper = pattern.upper()
            # Match tasks whose id starts with the pattern (case-insensitive)
            matched = {tid: info for tid, info in task_summary.items()
                       if tid.upper().startswith(pat_upper)}
            nlog.info(f"Pattern {pattern!r}: matched {len(matched)} task(s): {list(matched.keys())}")

            if not matched:
                nlog.warn(f"No tasks found matching prefix {pattern!r} — treating as passed")
                key = pattern.lower().replace(" ", "_")
                pattern_results[f"{key}_tasks_all_passed"] = True
                pattern_results[f"{key}_tasks"] = []
                pattern_results[f"{key}_failed_tasks"] = []
                continue

            failed = [tid for tid, info in matched.items()
                      if info["state"] not in ("success", "skipped")]
            all_passed = len(failed) == 0

            key = pattern.lower().replace(" ", "_").rstrip("_")
            pattern_results[f"{key}_tasks_all_passed"] = all_passed
            pattern_results[f"{key}_tasks"]            = list(matched.keys())
            pattern_results[f"{key}_failed_tasks"]     = failed

            if all_passed:
                nlog.ok(f"PASS: all {len(matched)} task(s) matching {pattern!r} succeeded")
            else:
                nlog.error(f"FAIL: {len(failed)} task(s) matching {pattern!r} did not succeed: {failed}")
                if props.get("assert_task_pattern_success", False):
                    raise AssertionError(
                        f"Tasks matching {pattern!r} failed: {failed}"
                    )

    if final_state != "success":
        raise RuntimeError(f"DAG {dag_id!r} ended with state={final_state!r}")

    nlog.ok("All Airflow assertions passed ✓")
    return {
        "dag_id":          dag_id,
        "dag_run_id":      dag_run_id,
        "final_state":     final_state,
        "elapsed_seconds": elapsed,
        "task_count":      len(task_instances),
        "task_summary":    task_summary,
        "task_logs":       {tid: log[:3000] for tid, log in all_task_logs.items()},
        **pattern_results,   # qa_tasks_all_passed, qa_tasks, qa_failed_tasks, etc.
    }


# ── SQL ───────────────────────────────────────────────────────────────────────

@node_handler("sql")
async def handle_sql(node, creds, owner_id, ctx, nlog, **kw):
    props = node.get("props", {})
    cred  = props.get("credential", "")
    query = props.get("query", "SELECT 1")
    fmt   = props.get("export_format", "none")

    expected_row_count  = props.get("expected_row_count")
    min_row_count       = props.get("min_row_count")
    assert_no_rows_flag = props.get("assert_no_rows", False)
    assert_scalar       = props.get("assert_scalar")
    max_sample          = int(props.get("max_sample_rows", 50))

    if not cred: raise ValueError("SQL node missing credential")

    nlog.section("SQL Query")
    nlog.info(f"Credential: {cred}")
    nlog.info(f"Query:\n  {query.strip()}")

    connector = creds.build_connector(cred, owner_id)

    def _run_sql():
        connector.connect()
        try:
            t0   = time.time()
            rows = connector.execute_query(query)
            qt   = round(time.time() - t0, 3)
            cols = list(connector.last_column_names)
            output_path = None
            if fmt == "csv":
                output_path = f"/tmp/sql_{int(time.time())}.csv"
                connector.export_to_csv(query, output_path)
            elif fmt == "xlsx":
                output_path = f"/tmp/sql_{int(time.time())}.xlsx"
                connector.export_to_xlsx(query, output_path)
            return rows, cols, qt, output_path
        finally:
            connector.disconnect()

    rows, col_names, qt, output_path = await _t(_run_sql)
    nlog.ok(f"Executed in {qt}s → {len(rows)} row(s)")

    if rows:
        nlog.section("Result Sample")
        if col_names:
            nlog.info("  " + " | ".join(f"{str(c):<18}" for c in col_names))
            nlog.info("  " + "-+-".join("-" * 18 for _ in col_names))
        for row in rows[:25]:
            nlog.info("  " + " | ".join(f"{str(v):<18}"[:18] for v in row))
        if len(rows) > 25: nlog.info(f"  … ({len(rows) - 25} more rows)")

    nlog.section("Assertions")
    if expected_row_count is not None:
        exp = int(expected_row_count)
        if len(rows) == exp: nlog.ok(f"PASS: row count == {exp}")
        else:
            nlog.error(f"FAIL: expected {exp} rows, got {len(rows)}")
            raise AssertionError(f"Row count: expected {exp}, got {len(rows)}")

    if min_row_count is not None:
        mn = int(min_row_count)
        if len(rows) >= mn: nlog.ok(f"PASS: {len(rows)} >= {mn}")
        else:
            nlog.error(f"FAIL: expected >= {mn} rows, got {len(rows)}")
            raise AssertionError(f"Min row count failed: expected >= {mn}, got {len(rows)}")

    if assert_no_rows_flag:
        if len(rows) == 0: nlog.ok("PASS: 0 rows (expected)")
        else:
            nlog.error(f"FAIL: expected 0 rows, got {len(rows)}")
            raise AssertionError(f"assert_no_rows: got {len(rows)} rows")

    if assert_scalar is not None:
        actual = rows[0][0] if rows else None
        if str(actual) == str(assert_scalar): nlog.ok(f"PASS: scalar == {assert_scalar!r}")
        else:
            nlog.error(f"FAIL: scalar expected {assert_scalar!r}, got {actual!r}")
            raise AssertionError(f"Scalar: expected {assert_scalar!r}, got {actual!r}")

    # Row-count level: assert_greater_than / assert_less_than / assert_equals
    # These are set by the DSL generator for prompts like "more than 300 entries"
    # (column-level versions of these are handled separately in _extract_one below)
    _agt = props.get("assert_greater_than")
    _alt = props.get("assert_less_than")
    _aeq = props.get("assert_equals")
    _extract_col = props.get("extract_column")  # if set, assertions apply to column value, not row count

    if _agt is not None and not _extract_col:
        try:
            thr = float(_agt)
            if len(rows) > thr: nlog.ok(f"PASS: row count {len(rows)} > {thr}")
            else:
                nlog.error(f"FAIL: row count {len(rows)} NOT > {thr}")
                raise AssertionError(f"assert_greater_than: row count {len(rows)} is not > {thr}")
        except (TypeError, ValueError): pass

    if _alt is not None and not _extract_col:
        try:
            thr = float(_alt)
            if len(rows) < thr: nlog.ok(f"PASS: row count {len(rows)} < {thr}")
            else:
                nlog.error(f"FAIL: row count {len(rows)} NOT < {thr}")
                raise AssertionError(f"assert_less_than: row count {len(rows)} is not < {thr}")
        except (TypeError, ValueError): pass

    if _aeq is not None and not _extract_col:
        try:
            exp = int(_aeq)
            if len(rows) == exp: nlog.ok(f"PASS: row count {len(rows)} == {exp}")
            else:
                nlog.error(f"FAIL: row count {len(rows)} != {exp}")
                raise AssertionError(f"assert_equals: row count {len(rows)} != {exp}")
        except (TypeError, ValueError): pass

    if output_path: nlog.ok(f"Exported {len(rows)} rows → {output_path}")
    nlog.ok("All SQL assertions passed ✓")

    row_sample = []
    for row in rows[:max_sample]:
        if col_names: row_sample.append(dict(zip([str(c) for c in col_names], [str(v) for v in row])))
        else:         row_sample.append([str(v) for v in row])

    # ── Column Extraction ─────────────────────────────────────────────────────
    # Allows downstream nodes to reference specific column values directly via
    # {{$node.My SQL.output.customer_id}} without needing a code node.
    #
    # Single-column props:
    #   extract_column:       "customer_id"          — column name to read
    #   extract_row:          0                       — row index (default 0)
    #   output_as:            "customer_id"           — key in output dict
    #   assert_column_equals: "ACTIVE"                — optional equality check
    #   assert_column_not_null: true                  — fail if null/empty
    #   assert_column_in:     ["ACTIVE","PENDING"]    — whitelist check
    #
    # Multi-column props (extract_columns overrides single-column):
    #   extract_columns: [
    #     {"column": "customer_id", "row": 0, "output_as": "cust_id"},
    #     {"column": "region",      "row": 0, "output_as": "region",
    #      "assert_not_null": true, "assert_in": ["EU","US","APAC"]},
    #     {"column": "amount",      "row": 0, "output_as": "amount",
    #      "assert_greater_than": 0},
    #   ]

    extracted = {}

    def _extract_one(spec: dict):
        """Extract one column value from row_sample and run any column assertions."""
        col       = spec.get("column") or spec.get("extract_column", "")
        row_idx   = int(spec.get("row",    spec.get("extract_row", 0)))
        out_key   = spec.get("output_as") or col
        assert_eq = spec.get("assert_equals",   spec.get("assert_column_equals"))
        not_null  = spec.get("assert_not_null",  spec.get("assert_column_not_null", False))
        assert_in = spec.get("assert_in",        spec.get("assert_column_in"))
        gt        = spec.get("assert_greater_than")
        lt        = spec.get("assert_less_than")

        if not col:
            return

        if not row_sample:
            raise AssertionError(f"extract_column={col!r}: query returned 0 rows")
        if row_idx >= len(row_sample):
            raise AssertionError(
                f"extract_column={col!r}: row {row_idx} out of range "
                f"(query returned {len(row_sample)} rows)"
            )

        row = row_sample[row_idx]
        if isinstance(row, dict):
            # Try exact name first, then case-insensitive fallback
            if col in row:
                value = row[col]
            else:
                col_lower = col.lower()
                match = next((v for k, v in row.items() if k.lower() == col_lower), None)
                if match is None and col_lower not in [k.lower() for k in row]:
                    raise AssertionError(
                        f"extract_column={col!r} not found in result columns: {list(row.keys())}"
                    )
                value = match
        else:
            # List row — match by column index
            try:
                col_names_lower = [str(c).lower() for c in col_names]
                idx = col_names_lower.index(col.lower())
                value = row[idx]
            except (ValueError, IndexError):
                raise AssertionError(
                    f"extract_column={col!r} not found in columns: {col_names}"
                )

        nlog.section(f"Column Extraction → {out_key}")
        nlog.info(f"  column={col!r}  row={row_idx}  value={value!r}")

        # Column-level assertions
        if not_null:
            if value is None or str(value).strip() in ("", "None", "NULL"):
                nlog.error(f"FAIL: {col!r} is null/empty at row {row_idx}")
                raise AssertionError(f"assert_column_not_null: {col!r} is null at row {row_idx}")
            nlog.ok(f"PASS: {col!r} not null")

        if assert_eq is not None:
            if str(value) != str(assert_eq):
                nlog.error(f"FAIL: {col!r} == {value!r}, expected {assert_eq!r}")
                raise AssertionError(
                    f"assert_column_equals: {col!r} expected {assert_eq!r}, got {value!r}"
                )
            nlog.ok(f"PASS: {col!r} == {assert_eq!r}")

        if assert_in is not None:
            allowed = [str(x) for x in assert_in]
            if str(value) not in allowed:
                nlog.error(f"FAIL: {col!r} = {value!r}, not in {allowed}")
                raise AssertionError(
                    f"assert_column_in: {col!r} = {value!r}, expected one of {allowed}"
                )
            nlog.ok(f"PASS: {col!r} in {allowed}")

        if gt is not None:
            try:
                if float(value) <= float(gt):
                    nlog.error(f"FAIL: {col!r} = {value} not > {gt}")
                    raise AssertionError(f"assert_greater_than: {col!r} = {value}, expected > {gt}")
                nlog.ok(f"PASS: {col!r} = {value} > {gt}")
            except (TypeError, ValueError):
                pass  # non-numeric — skip numeric assertion

        if lt is not None:
            try:
                if float(value) >= float(lt):
                    nlog.error(f"FAIL: {col!r} = {value} not < {lt}")
                    raise AssertionError(f"assert_less_than: {col!r} = {value}, expected < {lt}")
                nlog.ok(f"PASS: {col!r} = {value} < {lt}")
            except (TypeError, ValueError):
                pass

        extracted[out_key] = value
        nlog.ok(f"  → output[{out_key!r}] = {value!r}")

    # Multi-column extraction takes precedence
    extract_columns = props.get("extract_columns")
    if extract_columns and isinstance(extract_columns, list):
        nlog.section("Multi-Column Extraction")
        for spec in extract_columns:
            _extract_one(spec)
    elif props.get("extract_column"):
        _extract_one({
            "column":             props.get("extract_column"),
            "row":                props.get("extract_row", 0),
            "output_as":          props.get("output_as") or props.get("extract_column"),
            "assert_not_null":    props.get("assert_column_not_null", False),
            "assert_equals":      props.get("assert_column_equals"),
            "assert_in":          props.get("assert_column_in"),
            "assert_greater_than":props.get("assert_greater_than"),
            "assert_less_than":   props.get("assert_less_than"),
        })

    return {
        "rows_returned": len(rows),
        "query_time_s":  qt,
        "columns":       [str(c) for c in col_names],
        "rows_sample":   row_sample,
        "export_path":   output_path,
        "query":         query[:200],
        **extracted,        # ← extracted column values promoted to top-level output keys
    }


# ── HTTP ──────────────────────────────────────────────────────────────────────

@node_handler("http")
async def handle_http(node, creds, owner_id, ctx, nlog, **kw):
    from connectors.http_mcp import HTTPConnector
    props      = node.get("props", {})
    method     = props.get("method", "GET")
    url        = props.get("url", "")
    exp_status = int(props.get("expected_status", 200))
    cred_name  = props.get("credential", "")

    assert_json_field    = props.get("assert_json_field")
    assert_json_value    = props.get("assert_json_value")
    assert_body_contains = props.get("assert_body_contains")

    if not url: raise ValueError("HTTP node missing url")

    try:    headers = json.loads(props.get("headers") or "{}") if isinstance(props.get("headers"), str) else (props.get("headers") or {})
    except: headers = {}
    try:    body = json.loads(props.get("body") or "") if props.get("body") else None
    except: body = props.get("body") or None

    nlog.section(f"HTTP {method} {url}")
    connector = creds.build_connector(cred_name, owner_id) if cred_name else HTTPConnector(headers=headers)

    def _do():
        return connector.request(
            method, url,
            json=body if method in ("POST","PUT","PATCH") and isinstance(body, dict) else None,
            params=body if method == "GET" and isinstance(body, dict) else None,
        )

    t0 = time.time(); resp = await _t(_do); rt = round(time.time() - t0, 3)
    nlog.info(f"Response: HTTP {resp.status_code}  ({rt}s)")

    try:    resp_json = resp.json(); body_text = json.dumps(resp_json, indent=2)
    except: resp_json = None;       body_text = resp.text or ""

    nlog.section("Response Body")
    nlog.raw(body_text[:3000])
    if len(body_text) > 3000: nlog.raw(f"… ({len(body_text) - 3000} chars more)")

    nlog.section("Assertions")
    connector.assert_status(resp, exp_status)
    nlog.ok(f"PASS: status == {exp_status}")

    if assert_body_contains:
        if assert_body_contains in body_text: nlog.ok(f"PASS: body contains {assert_body_contains!r}")
        else:
            nlog.error(f"FAIL: body missing {assert_body_contains!r}")
            raise AssertionError(f"HTTP body missing: {assert_body_contains!r}")

    if assert_json_field and resp_json is not None:
        val = resp_json
        for part in assert_json_field.split("."):
            val = val.get(part) if isinstance(val, dict) else (val[int(part)] if isinstance(val, list) and part.isdigit() else None)
        if assert_json_value is not None:
            if str(val) == str(assert_json_value): nlog.ok(f"PASS: {assert_json_field} == {assert_json_value!r}")
            else:
                nlog.error(f"FAIL: {assert_json_field} expected {assert_json_value!r}, got {val!r}")
                raise AssertionError(f"JSON field {assert_json_field}={val!r}, expected {assert_json_value!r}")

    nlog.ok("All HTTP assertions passed ✓")
    return {"status_code": resp.status_code, "url": url, "method": method,
            "response_time": rt, "response": resp_json if resp_json is not None else body_text[:1000],
            "headers": dict(list(resp.headers.items())[:20])}


# ── S3 ────────────────────────────────────────────────────────────────────────

@node_handler("s3")
async def handle_s3(node, creds, owner_id, ctx, nlog, **kw):
    props  = node.get("props", {})
    cred   = props.get("credential", "")
    op     = props.get("operation", "list")
    bucket = props.get("bucket", "")
    key    = props.get("key", "")
    if not cred: raise ValueError("S3 node missing credential")
    nlog.section(f"S3 {op.upper()} — s3://{bucket}/{key}")
    c = creds.build_connector(cred, owner_id)
    if op == "upload":
        uri = await _t(c.upload_file, props.get("local_path",""), bucket, key)
        nlog.ok(f"Uploaded: {uri}")
        return {"operation":"upload","uri":uri,"bucket":bucket,"key":key}
    elif op == "download":
        path = await _t(c.download_file, bucket, key, props.get("local_path","/tmp/"))
        nlog.ok(f"Downloaded → {path}")
        return {"operation":"download","local_path":path}
    elif op == "list":
        items = await _t(c.list_objects, bucket, prefix=key)
        nlog.ok(f"Found {len(items)} objects")
        for i in items[:20]: nlog.info(f"  {i.get('Key','?')}  ({i.get('Size',0)} bytes)")
        return {"operation":"list","count":len(items),"items":items[:50]}
    elif op == "exists":
        exists = await _t(c.object_exists, bucket, key)
        if exists: nlog.ok(f"PASS: s3://{bucket}/{key} exists")
        else:
            nlog.error(f"FAIL: s3://{bucket}/{key} NOT found")
            if props.get("assert_exists", True): raise AssertionError(f"S3 object missing: s3://{bucket}/{key}")
        return {"operation":"exists","exists":exists}
    elif op == "delete":
        await _t(c.delete_object, bucket, key)
        nlog.ok(f"Deleted"); return {"operation":"delete"}
    else: raise ValueError(f"Unknown S3 op: {op!r}")


# ── AZURE ─────────────────────────────────────────────────────────────────────

@node_handler("azure")
async def handle_azure(node, creds, owner_id, ctx, nlog, **kw):
    props = node.get("props", {})
    cred  = props.get("credential", "")
    op    = props.get("operation", "list")
    cont  = props.get("container", "")
    blob  = props.get("blob_name", "")
    if not cred: raise ValueError("Azure node missing credential")
    nlog.section(f"Azure Blob {op.upper()} — {cont}/{blob}")
    c = creds.build_connector(cred, owner_id)
    if op == "upload":
        uri = await _t(c.upload_file, cont, blob, props.get("local_path",""))
        nlog.ok(f"Uploaded: {uri}"); return {"operation":"upload","uri":uri}
    elif op == "download":
        path = await _t(c.download_file, cont, blob, props.get("local_path","/tmp/"))
        nlog.ok(f"Downloaded → {path}"); return {"operation":"download","local_path":path}
    elif op == "list":
        blobs = await _t(c.list_blobs, cont)
        nlog.ok(f"Found {len(blobs)} blobs")
        for b in blobs[:20]: nlog.info(f"  {b.get('name',b) if isinstance(b,dict) else b}")
        return {"operation":"list","count":len(blobs),"blobs":blobs[:50]}
    elif op == "exists":
        exists = await _t(c.blob_exists, cont, blob)
        if exists: nlog.ok(f"PASS: {cont}/{blob} exists")
        else:
            nlog.error(f"FAIL: {cont}/{blob} NOT found")
            if props.get("assert_exists",True): raise AssertionError(f"Azure blob missing: {cont}/{blob}")
        return {"operation":"exists","exists":exists}
    else: raise ValueError(f"Unknown Azure op: {op!r}")


# ── SFTP ──────────────────────────────────────────────────────────────────────

@node_handler("sftp")
async def handle_sftp(node, creds, owner_id, ctx, nlog, **kw):
    props  = node.get("props", {})
    cred   = props.get("credential", "")
    op     = props.get("operation", "list")
    remote = props.get("remote_path", "/")
    local  = props.get("local_path", "/tmp/")
    if not cred: raise ValueError("SFTP node missing credential")
    nlog.section(f"SFTP {op.upper()} — {remote}")
    c = creds.build_connector(cred, owner_id)

    def _sftp_op():
        with c:
            if op == "upload":   c.upload(local, remote);   return {"operation":"upload","remote_path":remote}
            elif op == "download": c.download(remote, local); return {"operation":"download","local_path":local}
            elif op == "list":
                items = c.list_directory(remote)
                return {"operation":"list","count":len(items),"items":items[:50]}
            elif op == "exists":
                exists = c.file_exists(remote)
                return {"operation":"exists","exists":exists,"remote_path":remote}
            elif op == "delete": c.delete(remote); return {"operation":"delete","remote_path":remote}
            else: raise ValueError(f"Unknown SFTP op: {op!r}")

    result = await _t(_sftp_op)
    if op == "exists":
        if result["exists"]: nlog.ok(f"PASS: {remote} exists")
        elif props.get("assert_exists",True):
            nlog.error(f"FAIL: {remote} NOT found")
            raise AssertionError(f"SFTP file missing: {remote}")
    nlog.ok(f"SFTP {op} done: {result}")
    return result

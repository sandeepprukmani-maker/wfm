"""
FlowForge — Chat Router (MCP Edition)

Full agentic tool-use loop:
  1. User asks a question
  2. Claude decides which MCP tools to call (Airflow, SQL, S3, Azure, SFTP, HTTP)
  3. Chat router executes tools against real connectors using CredentialManager
  4. Results sent back to Claude
  5. Claude gives final answer with real data

19 tools across 6 services. Max 10 tool rounds per message.
Falls back to rule_based_dsl() when no ANTHROPIC_API_KEY is set.
"""

import os
import re
import json
import logging
import uuid
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db, ChatSession, ChatMessage

router = APIRouter()
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MAX_TOOL_ROUNDS   = 10


SYSTEM_PROMPT = """\
You are FlowForge AI — an expert workflow automation assistant with two capabilities:

1. LIVE DATA ACCESS — You have tools to query real systems (Airflow, SQL, S3, Azure, SFTP, HTTP).
   When a user asks a data question, ALWAYS use the appropriate tool to get the real answer.
   Never guess or invent data values.

2. WORKFLOW GENERATION — Convert natural-language descriptions into FlowForge DSL JSON.

── When to use tools ──────────────────────────────────────────────────────────
Use tools whenever the user asks about:
  - Current DAG status, recent runs, task failures, task logs (→ airflow_* tools)
  - Row counts, table data, schema information (→ sql_* tools)
  - File existence or listings in S3, Azure, SFTP (→ s3_*, azure_*, sftp_* tools)
  - API health or endpoint responses (→ http_* tools)

If unsure which credential to use, ask the user. Credentials are named
strings like PROD_AIRFLOW, PROD_MSSQL, AWS_PROD that the user configured.

── When to generate workflow DSL ──────────────────────────────────────────────
When user says "create a workflow", "build a pipeline", "automate X":
Respond with 2-3 sentences + a fenced JSON block:

```json
{
  "name": "Workflow Name",
  "nodes": [{"id":"n1","type":"trigger","title":"Schedule","x":60,"y":180,"props":{"cron":"0 6 * * *"}}],
  "edges": []
}
```

Node types: trigger, webhook, airflow, sql, http, s3, azure, sftp,
            wait, if, set_variable, get_variable, code, call_workflow

SQL node supports column extraction:
  extract_column, extract_row, output_as, assert_column_equals,
  assert_column_not_null, assert_column_in, assert_greater_than, assert_less_than

Expression syntax in any prop: {{$node.Title.output.field}}  {{$var.key_name}}
"""


# ── Rule-based DSL fallback ───────────────────────────────────────────────────

def _node(nid, ntype, title, x, y, props):
    return {"id": nid, "type": ntype, "title": title, "x": x, "y": y, "props": props}

def _edge(s, d):
    return {"from": s, "to": d}


def rule_based_dsl(prompt: str) -> dict:
    p   = prompt.lower()
    wds = set(re.findall(r'\b\w+\b', p))
    nodes, edges = [], []
    x, n, prev = 60, 0, None
    y_alt = [180, 220]

    def add(ntype, title, props):
        nonlocal x, n, prev
        n += 1; nid = f"n{n}"
        cy = y_alt[(n-1)%2]
        nodes.append(_node(nid, ntype, title, x, cy, props))
        if prev: edges.append(_edge(prev, nid))
        prev = nid; x += 230
        return nid

    # Trigger
    if any(w in wds for w in ("webhook","rest","external","incoming")):
        add("webhook", "Webhook Trigger", {"method": "POST", "auth_token": ""})
    else:
        cron = "0 6 * * *"
        if "hourly" in p: cron = "0 * * * *"
        elif "weekly" in p: cron = "0 6 * * 1"
        elif "manual" in p: cron = "manual"
        else:
            m = re.search(r'every\s+(\d+)\s*min', p)
            if m: cron = f"*/{m.group(1)} * * * *"
        add("trigger", "Schedule Trigger", {"cron": cron, "timezone": "UTC"})

    # Airflow
    dag_matches = re.findall(r'\bdag[_\s]?([a-z0-9_]+)|\b([a-z0-9_]+)[_\s]?dag\b', p)
    dag_ids = list({m[0] or m[1] for m in dag_matches if m[0] or m[1]})
    if any(w in wds for w in ("dag","airflow","pipeline","etl")) or dag_ids:
        dag_id = dag_ids[0] if dag_ids else "my_dag"
        add("airflow", f"Trigger {dag_id.replace('_',' ').title()}",
            {"dag_id": dag_id, "timeout": 3600, "retries": 2,
             "credential": "PROD_AIRFLOW", "assert_no_failed_tasks": True})

    # SQL
    if any(w in wds for w in ("sql","query","select","database","db","table","row","count","validate","verify")):
        count_m = re.search(r'row\s*count.*?(\d+)', p)
        min_m   = re.search(r'at\s+least\s*(\d+)\s*rows?', p)
        no_rows = "no rows" in p or "zero rows" in p
        query   = "SELECT COUNT(*) FROM table_name"
        if "order" in wds:    query = "SELECT COUNT(*) FROM orders WHERE status='active'"
        elif "error" in wds:  query = "SELECT COUNT(*) FROM error_log WHERE created_at >= GETDATE()-1"
        elif "sale" in wds:   query = "SELECT COUNT(*) FROM sales WHERE DATE = CAST(GETDATE() AS DATE)"
        export_fmt = "csv" if "csv" in wds else ("xlsx" if "excel" in wds or "xlsx" in wds else "none")
        props = {"query": query, "credential": "PROD_MSSQL", "store_rows": True, "export_format": export_fmt}
        if no_rows:   props["assert_no_rows"] = True
        elif count_m: props["expected_row_count"] = int(count_m.group(1))
        elif min_m:   props["min_row_count"] = int(min_m.group(1))
        col_m = re.search(r'(?:get|fetch|extract|read)\s+(?:the\s+)?([a-z0-9_]+)\s+(?:column|field)', p)
        if col_m:
            col = col_m.group(1)
            props.update({"extract_column": col, "output_as": col, "extract_row": 0})
        title = "Validate Row Count" if any(w in wds for w in ("validate","verify","check","assert")) else "SQL Query"
        add("sql", title, props)

    # set_variable
    if any(p2 in p for p2 in ("set variable","save variable","persist")):
        var_m = re.search(r'(?:set|save|store)\s+(?:variable\s+)?([a-z0-9_]+)\s*(?:to|=|as)\s*(.+?)(?:\s|$)', p)
        key   = var_m.group(1) if var_m else "result"
        value = var_m.group(2).strip() if var_m else "{{$node.SQL Query.output.rows_returned}}"
        add("set_variable", f"Set ${key}", {"key": key, "value": value, "scope": "workflow", "cast": "auto"})

    # code
    if any(p2 in p for p2 in ("run code","python code","calculate","compute","transform","process data")):
        code_hint = "output['result'] = input_data.get('rows_returned', 0)"
        add("code", "Run Python", {"code": code_hint, "timeout": 30, "language": "python"})

    # call_workflow
    if any(p2 in p for p2 in ("call workflow","sub-workflow","sub workflow")):
        add("call_workflow", "Call Sub-Workflow", {"workflow_id": "", "fail_on_error": True})

    # IF
    if any(t in p for t in ("if ","check if","branch","condition")):
        sql_nodes = [nd for nd in nodes if nd["type"] == "sql"]
        left = ("{{$node." + sql_nodes[-1]["title"] + ".output.rows_returned}}") if sql_nodes else "{{$node.SQL Query.output.rows_returned}}"
        right_m = re.search(r'\b(\d+)\b.*(?:condition|than|equal|row)', p)
        add("if", "Condition Check", {"left_value": left, "operator": "greater_than", "right_value": right_m.group(1) if right_m else "0"})

    # HTTP
    if any(w in wds for w in ("http","api","notify","slack","alert","post")) and "call workflow" not in p:
        method = "POST" if any(w in wds for w in ("post","send","notify","slack","alert")) else "GET"
        url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL" if "slack" in wds else "https://api.example.com/endpoint"
        props = {"method": method, "url": url, "expected_status": 200}
        if method == "POST" and "slack" in wds:
            props["body"] = '{"text":"FlowForge: Workflow completed ✓"}'
        add("http", "Notify Slack" if "slack" in wds else "HTTP Request", props)

    # S3
    if any(w in wds for w in ("s3","bucket")) and "azure" not in wds:
        op = "download" if "download" in wds else ("list" if "list" in wds else "upload")
        bucket_m = re.search(r'bucket[_\-\s]+([a-z0-9\-_]+)', p)
        add("s3", f"S3 {op.capitalize()}", {"bucket": bucket_m.group(1) if bucket_m else "my-bucket",
            "key": "data/output.csv", "operation": op, "credential": "AWS_PROD"})

    # Azure
    if any(w in wds for w in ("azure","blob","adls")):
        op = "download" if "download" in wds else ("list" if "list" in wds else "upload")
        add("azure", f"Azure Blob {op.capitalize()}", {"container": "my-container",
            "blob_name": "data/output.csv", "operation": op, "credential": "AZURE_STORAGE"})

    # SFTP
    if any(w in wds for w in ("sftp","ftp","file transfer")):
        op = "download" if "download" in wds else ("list" if "list" in wds else "upload")
        add("sftp", f"SFTP {op.capitalize()}", {"remote_path": "/data/output.csv",
            "local_path": "/tmp/", "operation": op, "credential": "SFTP_LEGACY"})

    # Fallback
    if len(nodes) == 1:
        add("airflow", "Run DAG", {"dag_id": "my_dag", "timeout": 3600, "retries": 2,
            "credential": "PROD_AIRFLOW", "assert_no_failed_tasks": True})
        add("sql", "Validate Result", {"query": "SELECT COUNT(*) FROM result_table",
            "credential": "PROD_MSSQL", "store_rows": True})
        add("http", "Notify", {"method": "POST", "url": "https://hooks.slack.com/services/...",
            "expected_status": 200})

    name = prompt[:60].strip() if len(prompt) > 3 else "Generated Workflow"
    return {"name": name, "description": f"Auto-generated from: {prompt[:120]}", "nodes": nodes, "edges": edges}


def _describe_dsl(dsl: dict) -> str:
    nodes = dsl["nodes"]
    kinds = list({nd["type"] for nd in nodes})
    exts  = [nd["props"].get("extract_column") for nd in nodes if nd.get("props", {}).get("extract_column")]
    extra = f" Column extraction: {', '.join(exts)}." if exts else ""
    return (
        f'Workflow "{dsl["name"]}" — {len(nodes)} nodes ({", ".join(kinds)}).{extra}\n'
        f"Logs, SQL results and variable values are captured per-run."
    )


def extract_dsl(text: str):
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try: return json.loads(m.group(1))
        except Exception: pass
    try:
        s = text.find("{"); e = text.rfind("}") + 1
        if s >= 0 and e > s: return json.loads(text[s:e])
    except Exception: pass
    return None


# ── Agentic tool-use loop ─────────────────────────────────────────────────────

async def _run_tool(name, args, cred_mgr, owner_id):
    import asyncio
    from mcp_servers.tool_registry import execute_tool
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, lambda: execute_tool(name, args, cred_mgr, owner_id)
    )


async def call_with_tools(messages, cred_mgr=None, owner_id=None):
    """
    Agentic loop: Claude ↔ tools ↔ Claude until text response or MAX_TOOL_ROUNDS.
    Returns (reply_text, tool_call_log).
    """
    try:
        import anthropic
    except ImportError:
        return None, []

    from mcp_servers.tool_registry import get_tool_definitions

    client    = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    tools     = get_tool_definitions()
    tool_log  = []
    msgs      = list(messages)

    for _ in range(MAX_TOOL_ROUNDS):
        resp = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=tools,
            messages=msgs,
        )

        text_parts = [b.text for b in resp.content if b.type == "text"]
        tool_uses  = [b for b in resp.content if b.type == "tool_use"]

        if not tool_uses:
            return "\n".join(text_parts).strip(), tool_log

        # Append Claude's response (includes tool_use blocks)
        msgs.append({"role": "assistant", "content": resp.content})

        # Execute all tool calls
        results = []
        for tc in tool_uses:
            logger.info(f"[MCP] Claude → {tc.name}({list(tc.input.keys())})")
            result = await _run_tool(tc.name, tc.input, cred_mgr, owner_id)
            tool_log.append({"tool": tc.name, "args": tc.input, "result": result})
            results.append({
                "type":        "tool_result",
                "tool_use_id": tc.id,
                "content":     json.dumps(result, default=str),
            })

        msgs.append({"role": "user", "content": results})

    return "Hit tool round limit. Please try a more specific question.", tool_log


async def call_simple(messages) -> Optional[str]:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp   = client.messages.create(
            model="claude-opus-4-6", max_tokens=4000,
            system=SYSTEM_PROMPT, messages=messages,
        )
        return resp.content[0].text
    except Exception as e:
        logger.error(f"Anthropic error: {e}")
        return None


# ── Request/Response schemas ──────────────────────────────────────────────────

class ChatRequest(BaseModel):
    message:    str
    session_id: Optional[str] = None
    history:    list = []
    use_tools:  bool = True   # False = DSL generation only, no live data


class ChatResponse(BaseModel):
    reply:        str
    workflow_dsl: Optional[dict] = None
    session_id:   str
    tool_calls:   list = []   # [{tool, args, result}] for UI display


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/message", response_model=ChatResponse)
async def chat_message(
    req:          ChatRequest,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    session_id   = req.session_id or str(uuid.uuid4())[:8]
    messages     = req.history + [{"role": "user", "content": req.message}]
    reply_text   = ""
    workflow_dsl = None
    tool_calls   = []

    if ANTHROPIC_API_KEY:
        try:
            from credential_manager import CredentialManager
            cred_mgr = CredentialManager(db)
        except Exception:
            cred_mgr = None

        if req.use_tools:
            reply_text, tool_calls = await call_with_tools(
                messages, cred_mgr=cred_mgr, owner_id=current_user["sub"]
            )
            if reply_text is None:
                reply_text = "Tool execution failed. Check credentials and try again."
            workflow_dsl = extract_dsl(reply_text)
            if workflow_dsl:
                reply_text = re.sub(r"```(?:json)?.*?```", "", reply_text, flags=re.DOTALL).strip()
        else:
            ai_reply = await call_simple(messages)
            if ai_reply:
                workflow_dsl = extract_dsl(ai_reply)
                reply_text   = re.sub(r"```(?:json)?.*?```", "", ai_reply, flags=re.DOTALL).strip()
            else:
                reply_text = "AI unavailable."
    else:
        workflow_dsl = rule_based_dsl(req.message)
        reply_text   = _describe_dsl(workflow_dsl)

    # Persist
    try:
        session = db.query(ChatSession).filter_by(id=session_id).first()
        if not session:
            title   = req.message[:50].strip() + ("…" if len(req.message) > 50 else "")
            session = ChatSession(id=session_id, user_id=current_user["sub"], title=title)
            db.add(session)
            db.commit()
        db.add(ChatMessage(session_id=session_id, role="user",      content=req.message))
        db.add(ChatMessage(session_id=session_id, role="assistant", content=reply_text))
        session.updated_at = datetime.utcnow()
        db.commit()
    except Exception as e:
        logger.warning(f"Chat DB persist failed: {e}")

    return ChatResponse(reply=reply_text, workflow_dsl=workflow_dsl,
                        session_id=session_id, tool_calls=tool_calls)


@router.get("/sessions")
async def list_sessions(
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
    limit:        int     = 20,
):
    sessions = (
        db.query(ChatSession)
        .filter_by(user_id=current_user["sub"])
        .order_by(ChatSession.updated_at.desc())
        .limit(limit).all()
    )
    return {"sessions": [{"id": s.id, "title": s.title,
        "created_at": s.created_at.isoformat(), "updated_at": s.updated_at.isoformat()}
        for s in sessions]}


@router.get("/sessions/{session_id}")
async def get_session(
    session_id:   str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    from fastapi import HTTPException
    session = db.query(ChatSession).filter_by(id=session_id, user_id=current_user["sub"]).first()
    if not session:
        raise HTTPException(404, "Session not found")
    msgs = db.query(ChatMessage).filter_by(session_id=session_id).order_by(ChatMessage.created_at).all()
    return {"session_id": session_id, "title": session.title,
            "messages": [{"role": m.role, "content": m.content,
                          "created_at": m.created_at.isoformat()} for m in msgs]}


@router.delete("/sessions/{session_id}", status_code=204)
async def delete_session(
    session_id:   str,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    from fastapi import HTTPException
    session = db.query(ChatSession).filter_by(id=session_id, user_id=current_user["sub"]).first()
    if not session:
        raise HTTPException(404, "Session not found")
    db.delete(session)
    db.commit()


@router.get("/tools")
async def list_tools(current_user: dict = Depends(get_current_user)):
    """List all available MCP tools with descriptions."""
    from mcp_servers.tool_registry import get_tool_definitions
    tools = get_tool_definitions()
    return {"tools": tools, "count": len(tools)}

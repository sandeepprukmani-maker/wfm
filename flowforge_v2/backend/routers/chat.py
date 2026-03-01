"""
FlowForge — Chat Router v3
Converts plain-English prompts to full workflow DSL.

Rule-based parser handles 50+ prompt patterns including:
- Airflow log validation (assert_log_contains, validate_logs, validate_tasks)
- SQL assertions (expected_row_count, min_row_count, assert_no_rows, assert_scalar)
- HTTP assertions (expected_status, assert_json_field, assert_body_contains)
- S3/Azure/SFTP operations with existence checks
- IF/ELSE branching from natural language conditions
- Webhook triggers
- Multi-DAG patterns
- Error / no-error checks
"""

import os, re, json, logging, uuid
from typing import Optional
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from datetime import datetime
from auth import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

SYSTEM_PROMPT = """You are FlowForge AI — convert natural language workflow descriptions into structured JSON DSL.

Response format:
1. Brief confirmation (2-3 sentences)
2. JSON block: ```json { ... } ```

DSL schema:
{
  "name": "string",
  "description": "string",
  "nodes": [
    {
      "id": "n1",
      "type": "trigger|webhook|airflow|sql|http|s3|azure|sftp|wait|if",
      "title": "Display name",
      "x": 60, "y": 180,
      "props": {
        // See prop schemas below
      }
    }
  ],
  "edges": [{"from": "n1", "to": "n2"}]
}

Prop schemas by type:
trigger:  {cron, timezone}
webhook:  {method, auth_token}
airflow:  {dag_id, timeout, retries, conf, credential,
           validate_tasks: ["task_id"],
           validate_logs: {"task_id": "keyword"},
           assert_log_contains: "string",
           assert_no_failed_tasks: true}
sql:      {query, credential, expected_row_count, min_row_count,
           assert_no_rows: bool, assert_scalar,
           export_format: "none|csv|xlsx",
           store_rows: true, max_sample_rows: 50}
http:     {method, url, headers, body, expected_status, credential,
           assert_json_field, assert_json_value, assert_body_contains}
s3:       {bucket, key, region, operation: "upload|download|list|exists|delete",
           local_path, credential, assert_exists: true}
azure:    {container, blob_name, operation: "upload|download|list|exists",
           local_path, credential, assert_exists: true}
sftp:     {remote_path, local_path, operation: "upload|download|list|exists|delete",
           credential, assert_exists: true}
wait:     {duration, unit: "seconds|minutes|hours"}
if:       {left_value, operator: "equals|not_equals|contains|greater_than|less_than|is_empty|not_empty",
           right_value, assert_true: bool, assert_false: bool}

Layout: x += 230 per node, y alternates 160/220 to avoid overlap.
Always start with a trigger or webhook node.
"""


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    history: list = []


class ChatResponse(BaseModel):
    reply: str
    workflow_dsl: Optional[dict] = None
    session_id: str


# ─────────────────────────────────────────────────────────────────────────────
# Rule-based parser — handles prompts without AI key
# ─────────────────────────────────────────────────────────────────────────────

def _node(nid, ntype, title, x, y, props):
    return {"id": nid, "type": ntype, "title": title, "x": x, "y": y, "props": props}

def _edge(src, dst):
    return {"from": src, "to": dst}

def rule_based_dsl(prompt: str) -> dict:
    p   = prompt.lower()
    wds = re.findall(r'\b\w+\b', p)  # individual words for quick lookup
    nodes, edges = [], []
    x, y, n, prev = 60, 180, 0, None

    def add(ntype, title, props):
        nonlocal x, n, prev
        n += 1
        nid = f"n{n}"
        nodes.append(_node(nid, ntype, title, x, y, props))
        if prev: edges.append(_edge(prev, nid))
        prev = nid; x += 230
        return nid

    # ── Trigger type ──────────────────────────────────────────────────────────
    if any(w in wds for w in ("webhook", "rest", "external", "incoming", "trigger-on")):
        method = "GET" if "get" in wds else "POST"
        add("webhook", "Webhook Trigger", {"method": method, "auth_token": ""})
    else:
        cron = "0 6 * * *"
        if "hourly" in p or "every hour" in p:  cron = "0 * * * *"
        elif "daily"  in p or "every day" in p:  cron = "0 6 * * *"
        elif "weekly" in p or "every week" in p: cron = "0 6 * * 1"
        elif re.search(r'every\s+(\d+)\s+min', p): cron = f"*/{re.search(r'every\\s+(\\d+)\\s+min', p).group(1)} * * * *"
        elif "manual" in p or "on demand" in p:  cron = "manual"
        add("trigger", "Schedule Trigger", {"cron": cron, "timezone": "UTC"})

    # ── Airflow ───────────────────────────────────────────────────────────────
    # Find all DAG IDs mentioned
    dag_matches = re.findall(r'\bdag[_\s]?([a-z0-9_]+)|\b([a-z0-9_]+)[_\s]?dag\b', p)
    dag_ids     = list({m[0] or m[1] for m in dag_matches if m[0] or m[1]})

    if any(w in wds for w in ("dag", "airflow", "pipeline", "etl")) or dag_ids:
        # Default dag id from first match or generic
        dag_id  = dag_ids[0] if dag_ids else "my_dag"
        timeout = 7200 if "long" in p or "hour" in p else 3600

        # Detect log assertions
        log_kw = None
        m = re.search(r'log[s]?\s+(?:contain[s]?|include[s]?|ha[s|ve]+)\s+["\']?([a-z0-9_\s\-]+)["\']?', p)
        if m: log_kw = m.group(1).strip()
        # "check logs for errors" / "no errors in logs"
        if "no error" in p or "zero error" in p: log_kw = None  # assert_no_failed_tasks handles it

        # Task validations
        task_ids = re.findall(r'\btask[_\s]([a-z0-9_]+)\b', p)

        props = {
            "dag_id": dag_id,
            "timeout": timeout,
            "retries": 2,
            "credential": "PROD_AIRFLOW",
            "assert_no_failed_tasks": True,
        }
        if log_kw:
            props["assert_log_contains"] = log_kw
        if task_ids:
            props["validate_tasks"] = task_ids
        if "collect log" in p or "fetch log" in p or "download log" in p:
            # Always true now — just signals intent
            pass

        title = f"Trigger {dag_id.replace('_',' ').title()}"
        add("airflow", title, props)

        # Second DAG if mentioned
        if len(dag_ids) > 1:
            for dag_id2 in dag_ids[1:]:
                add("airflow", f"Trigger {dag_id2.replace('_',' ').title()}", {
                    "dag_id": dag_id2, "timeout": 3600, "retries": 1,
                    "credential": "PROD_AIRFLOW", "assert_no_failed_tasks": True,
                })

    # ── Wait / sleep ──────────────────────────────────────────────────────────
    if any(w in wds for w in ("wait", "sleep", "delay", "pause", "after")):
        dur_m  = re.search(r'wait\s+(\d+)\s*(second|minute|hour|s|m|h)', p)
        dur    = int(dur_m.group(1)) if dur_m else 30
        unit   = "seconds"
        if dur_m:
            u = dur_m.group(2)
            if u.startswith("m"): unit = "minutes"
            elif u.startswith("h"): unit = "hours"
        add("wait", f"Wait {dur} {unit}", {"duration": dur, "unit": unit})

    # ── SQL ───────────────────────────────────────────────────────────────────
    if any(w in wds for w in ("sql", "query", "select", "database", "db", "table", "row", "count", "validate", "verify")):
        # Parse expected counts
        count_m = re.search(r'(?:row\s*count|count)\s*(?:is|==|=|equals?|of)\s*(\d+)', p)
        min_m   = re.search(r'(?:at\s+least|>=|minimum|min)\s*(\d+)\s*rows?', p)
        no_rows = "no rows" in p or "zero rows" in p or "empty" in p

        # Guess query from prompt
        query = "SELECT COUNT(*) FROM table_name"
        if "order" in wds:        query = "SELECT COUNT(*) FROM orders WHERE status='active'"
        elif "error" in wds:      query = "SELECT COUNT(*) FROM error_log WHERE created_at >= GETDATE()-1"
        elif "sale" in wds:       query = "SELECT COUNT(*) FROM sales WHERE DATE = CAST(GETDATE() AS DATE)"
        elif "customer" in wds:   query = "SELECT COUNT(*) FROM customers WHERE is_active = 1"
        elif "user" in wds:       query = "SELECT COUNT(*) FROM users"
        elif "product" in wds:    query = "SELECT COUNT(*) FROM products"
        elif "invoice" in wds:    query = "SELECT COUNT(*) FROM invoices"

        # Check if export needed
        export_fmt = "none"
        if "export" in wds or "csv" in wds:   export_fmt = "csv"
        elif "excel" in wds or "xlsx" in wds: export_fmt = "xlsx"

        props = {
            "query":      query,
            "credential": "PROD_MSSQL",
            "store_rows": True,
            "export_format": export_fmt,
        }
        if no_rows:
            props["assert_no_rows"] = True
        elif count_m:
            props["expected_row_count"] = int(count_m.group(1))
        elif min_m:
            props["min_row_count"] = int(min_m.group(1))

        title = "Validate Row Count" if any(w in wds for w in ("validate","verify","check","assert")) else "SQL Query"
        add("sql", title, props)

        # Second SQL for error check
        if "no error" in p or "zero error" in p or "assert no error" in p:
            add("sql", "Assert No Errors", {
                "query": "SELECT COUNT(*) FROM error_log WHERE run_date = CAST(GETDATE() AS DATE)",
                "credential": "PROD_MSSQL",
                "assert_no_rows": True,
                "store_rows": True,
            })

    # ── IF/ELSE ───────────────────────────────────────────────────────────────
    if any(t in p for t in ("if ", "check if", "branch", "condition", "when ", "only if")):
        # Try to extract comparison
        op = "greater_than"
        if "equal" in p or " == " in p:   op = "equals"
        elif "not equal" in p:            op = "not_equals"
        elif "contain" in p:              op = "contains"
        elif "less than" in p or " < " in p: op = "less_than"
        elif "empty" in p:                op = "is_empty"

        # Common left values using expression references
        left = "{{$node.SQL Query.output.rows_returned}}"
        if "row" in wds and n > 0:
            # reference the previous SQL node if one exists
            sql_nodes = [nd for nd in nodes if nd["type"] == "sql"]
            if sql_nodes:
                left = "{{$node." + sql_nodes[-1]["title"] + ".output.rows_returned}}"

        right_m = re.search(r'\b(\d+)\b.*(?:condition|than|equal|row)', p)
        right   = right_m.group(1) if right_m else "0"

        add("if", "Condition Check", {
            "left_value":  left,
            "operator":    op,
            "right_value": right,
        })

    # ── HTTP / Slack / API notify ─────────────────────────────────────────────
    if any(w in wds for w in ("http", "api", "notify", "slack", "alert", "webhook", "post", "call", "request")):
        method = "GET"
        if any(w in wds for w in ("post", "send", "notify", "slack", "alert", "create", "trigger")): method = "POST"
        elif any(w in wds for w in ("put", "update")): method = "PUT"

        url = "https://api.example.com/endpoint"
        if "slack" in wds: url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
        elif "teams" in wds: url = "https://outlook.office.com/webhook/..."
        elif "pagerduty" in wds: url = "https://events.pagerduty.com/v2/enqueue"

        # Detect JSON field assertions
        json_field = None
        jf_m = re.search(r'(?:assert|check|verify)\s+(?:field|key|property)\s+["\']?([a-z_.]+)["\']?\s*(?:==?|equals?|is)\s*["\']?([^\s"\']+)["\']?', p)
        if jf_m: json_field = jf_m.group(1)

        props = {
            "method":          method,
            "url":             url,
            "expected_status": 200,
            "headers":         '{"Content-Type": "application/json"}',
        }
        if json_field:
            props["assert_json_field"] = json_field

        if method == "POST" and "slack" in wds:
            props["body"] = '{"text": "FlowForge: Workflow completed ✓"}'
        if "assert" in wds and "body" in wds:
            props["assert_body_contains"] = "success"

        title = "Notify Slack" if "slack" in wds else "HTTP Request"
        add("http", title, props)

    # ── S3 ────────────────────────────────────────────────────────────────────
    if any(w in wds for w in ("s3", "bucket", "upload", "download")) and "azure" not in wds:
        op = "upload"
        if "download" in wds: op = "download"
        elif "list" in wds:   op = "list"
        elif "exist" in wds or "check" in wds: op = "exists"
        elif "delete" in wds: op = "delete"
        bucket_m = re.search(r'bucket[_\-\s]+([a-z0-9\-_]+)', p)
        bucket   = bucket_m.group(1) if bucket_m else "my-bucket"
        props = {"bucket": bucket, "key": "data/output.csv", "operation": op, "credential": "AWS_PROD", "assert_exists": True}
        add("s3", f"S3 {op.capitalize()}", props)

    # ── Azure ─────────────────────────────────────────────────────────────────
    if any(w in wds for w in ("azure", "blob", "adls", "adl")):
        op = "upload"
        if "download" in wds: op = "download"
        elif "list" in wds:   op = "list"
        elif "exist" in wds:  op = "exists"
        cont_m = re.search(r'container[_\-\s]+([a-z0-9\-_]+)', p)
        cont   = cont_m.group(1) if cont_m else "my-container"
        add("azure", f"Azure Blob {op.capitalize()}", {
            "container": cont, "blob_name": "data/output.csv",
            "operation": op, "credential": "AZURE_STORAGE", "assert_exists": True,
        })

    # ── SFTP ──────────────────────────────────────────────────────────────────
    if any(w in wds for w in ("sftp", "ftp", "secure", "remote", "file transfer")):
        op = "upload"
        if "download" in wds: op = "download"
        elif "list" in wds:   op = "list"
        elif "exist" in wds:  op = "exists"
        add("sftp", f"SFTP {op.capitalize()}", {
            "remote_path": "/data/output.csv", "local_path": "/tmp/",
            "operation": op, "credential": "SFTP_LEGACY", "assert_exists": True,
        })

    # ── Fallback: if only trigger, add generic airflow ────────────────────────
    if len(nodes) == 1:
        add("airflow", "Run DAG", {
            "dag_id": "my_dag", "timeout": 3600, "retries": 2,
            "credential": "PROD_AIRFLOW", "assert_no_failed_tasks": True,
        })
        add("sql", "Validate Result", {
            "query": "SELECT COUNT(*) FROM result_table",
            "credential": "PROD_MSSQL", "store_rows": True,
        })
        add("http", "Notify", {
            "method": "POST", "url": "https://hooks.slack.com/services/...",
            "expected_status": 200,
        })

    name = prompt[:60].strip() if len(prompt) > 3 else "Generated Workflow"
    desc = f"Auto-generated from prompt: {prompt[:120]}"
    return {"name": name, "description": desc, "nodes": nodes, "edges": edges}


def extract_dsl(text: str):
    """Extract JSON DSL block from AI response."""
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try: return json.loads(m.group(1))
        except Exception: pass
    try:
        s = text.find("{"); e = text.rfind("}") + 1
        if s >= 0 and e > s: return json.loads(text[s:e])
    except Exception: pass
    return None


async def call_anthropic(messages: list):
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp   = client.messages.create(
            model="claude-opus-4-6", max_tokens=3000,
            system=SYSTEM_PROMPT, messages=messages,
        )
        return resp.content[0].text
    except Exception as e:
        logger.error(f"Anthropic API error: {e}")
        return None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/message", response_model=ChatResponse)
async def chat_message(req: ChatRequest, current_user: dict = Depends(get_current_user)):
    session_id = req.session_id or str(uuid.uuid4())[:8]
    messages   = req.history + [{"role": "user", "content": req.message}]

    ai_reply     = None
    workflow_dsl = None
    reply_text   = ""

    if ANTHROPIC_API_KEY:
        ai_reply = await call_anthropic(messages)

    if ai_reply:
        workflow_dsl = extract_dsl(ai_reply)
        reply_text   = re.sub(r"```(?:json)?.*?```", "", ai_reply, flags=re.DOTALL).strip()
    else:
        workflow_dsl = rule_based_dsl(req.message)
        n     = len(workflow_dsl["nodes"])
        kinds = list({nd["type"] for nd in workflow_dsl["nodes"]})
        assertions = []
        for nd in workflow_dsl["nodes"]:
            pr = nd.get("props", {})
            if pr.get("assert_no_failed_tasks"):   assertions.append("Airflow task state check")
            if pr.get("assert_log_contains"):       assertions.append(f"Airflow log contains '{pr['assert_log_contains']}'")
            if pr.get("validate_tasks"):            assertions.append(f"Task validation: {pr['validate_tasks']}")
            if pr.get("expected_row_count") is not None: assertions.append(f"SQL row count == {pr['expected_row_count']}")
            if pr.get("assert_no_rows"):            assertions.append("SQL asserts 0 rows")
            if pr.get("assert_json_field"):         assertions.append(f"HTTP JSON field: {pr['assert_json_field']}")
            if pr.get("assert_body_contains"):      assertions.append(f"HTTP body contains: {pr['assert_body_contains']}")
            if pr.get("assert_exists"):             assertions.append(f"{nd['type'].upper()} existence check")

        reply_text = (
            f'Workflow "{workflow_dsl["name"]}" built — {n} nodes ({", ".join(kinds)}).\n'
            + (f'Validations: {"; ".join(assertions)}.\n' if assertions else "")
            + "All Airflow task logs, SQL results and HTTP responses will be captured in the execution log."
        )

    return ChatResponse(reply=reply_text, workflow_dsl=workflow_dsl, session_id=session_id)


@router.get("/sessions")
async def list_sessions(current_user: dict = Depends(get_current_user)):
    return {"sessions": [
        {"id": "ch1", "title": "ETL Pipeline Validation", "created_at": datetime.utcnow().isoformat()},
        {"id": "ch2", "title": "S3 Export + Slack Notify",  "created_at": datetime.utcnow().isoformat()},
    ]}

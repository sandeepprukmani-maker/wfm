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


SYSTEM_PROMPT = """You are FlowForge AI — a workflow automation assistant with access to live MCP tools
for Airflow, SQL, S3, Azure, SFTP and HTTP. You have two modes and you decide which to use:

━━ MODE 1: WORKFLOW CREATION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
When the user wants to build, create, automate, or set up a workflow/pipeline:

ALWAYS follow this sequence:
  STEP 1 — DISCOVER using MCP tools:
    • For any Airflow DAGs mentioned → call airflow_list_dags to find real dag_ids
    • For any SQL tables mentioned → call sql_execute_query with a LIMIT 0 to verify table/columns
    • For S3/Azure/SFTP resources → call the appropriate list/exists tool to confirm paths
    • For HTTP endpoints → call http_request to verify URL responds
    • If credentials are unclear → use airflow_health_check / sql_execute_query to identify
      which credential name is active (e.g. PROD_AIRFLOW, PROD_MSSQL)

  STEP 2 — PLAN: Decide the best node sequence based on what you discovered.
    Think about: What must run first? What conditions gate the next step?
    What validations make sense given the real resources you found?

  STEP 3 — GENERATE: Output 1-2 sentences explaining the workflow, then the DSL JSON block.

DSL FORMAT RULES:
- Respond with a fenced ```json block containing the full DSL object
- Every node: id (unique, "n1","n2"...), type, title, x (60 + 230*index), y (180 or 220 alt), props
- Every edge: from (node id), to (node id). Branch edges add: branch ("true"/"false")

VALID NODE TYPES AND PROPS:
  trigger      → cron (e.g. "0 6 * * *" or "manual"), timezone
  webhook      → method ("POST"), auth_token
  airflow      → credential, operation (default "trigger")
                 trigger props: dag_id, wait_for_completion (bool), timeout (int),
                   wait_if_running (bool), assert_no_failed_tasks (bool),
                   validate_task_patterns (list of prefix strings), conf (dict)
                 other operations: list_dags, get_dag, pause, unpause, delete_dag,
                   list_runs, get_run, clear_run, delete_run, update_run_state,
                   get_task, update_task_state, clear_task, get_task_log,
                   list_variables, get_variable, set_variable, delete_variable,
                   list_connections, get_connection, create_connection, update_connection, delete_connection,
                   list_pools, get_pool, create_pool, update_pool, delete_pool,
                   get_xcom, list_xcom, health_check, get_version, get_config
  sql          → query (full SQL), credential
                 optional: extract_column, output_as, assert_greater_than,
                   assert_less_than, expected_row_count, min_row_count
  http         → method, url, expected_status (int), headers (JSON), body (JSON)
  s3           → bucket, key, operation ("list"|"exists"|"upload"|"download"|"delete"), credential
  azure        → container, blob_name, operation, credential
  sftp         → remote_path, operation, credential
  wait         → duration (int), unit ("seconds"|"minutes"|"hours")
  if           → left_value, operator, right_value
                 operators: equals, not_equals, greater_than, less_than, contains, is_empty, not_empty
  set_variable → key, value, scope ("workflow"|"global")
  get_variable → key, default
  code         → code (Python; reads input_data dict, writes to output dict), timeout (int)
  call_workflow → workflow_id

EXPRESSION SYNTAX (use in any prop value to wire nodes together):
  {{$node.Node Title.output.field}}   — upstream node output field
  {{$var.key_name}}                   — stored variable

NODE OUTPUT FIELDS:
  airflow  → dag_run_id, final_state, elapsed_seconds, task_count, task_summary
             {prefix}_tasks_all_passed, {prefix}_tasks, {prefix}_failed_tasks
  sql      → rows_returned, columns, rows_sample, + any extract_column output_as key
  http     → status_code, response_time, response
  s3/azure/sftp → exists (bool), count (int), items (list)
  if       → branch ("true"|"false"), condition_result (bool)

TRIGGER CHOICE:
  - No schedule words in prompt → use cron: "manual"
  - "daily", "every day", "at 6am" etc → use cron schedule
  - "webhook", "incoming", "external" → use webhook node

━━ MODE 2: LIVE DATA QUESTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
When the user asks about current state (DAG status, row counts, file existence etc):
Use the appropriate MCP tool immediately and return a direct answer.
Never guess data values — always use tools to get real answers.

━━ CRITICAL RULES — NEVER BREAK THESE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RULE 1 — MISSING RESOURCE = STOP AND ASK, NEVER GUESS:
  If a DAG, table, bucket, file, endpoint or credential mentioned by the user
  is NOT found by your discovery tools:
    → Do NOT generate a workflow
    → Do NOT use a placeholder name
    → STOP and ask the user:
      "I couldn't find [resource] in [system]. Did you mean one of these: [list what you did find]?
       Please confirm the exact name and I'll generate the workflow."

RULE 2 — NEVER INVENT NAMES:
  dag_id, table names, bucket names, blob names, credential names must come from
  either the user's message OR confirmed by a tool call.
  If neither — ask. Never fill in "my_dag", "result_table", "my-bucket" as placeholders.

RULE 3 — TOOLS FAIL = TELL THE USER WHY:
  If a tool returns a connection error, auth failure, or timeout:
    → Report the exact error to the user
    → Ask them to check the credential / connection
    → Do NOT generate a workflow based on unconfirmed resources

RULE 4 — AMBIGUOUS NAME = SHOW OPTIONS:
  If the user says "dag a" and you find ["dag_orders", "dag_analytics", "dag_a_etl"]:
    → List what you found and ask which one they mean
    → Only generate after they confirm

RULE 5 — PARTIAL MATCH IS OK WITH CONFIRMATION:
  If you find exactly one DAG/table that closely matches what the user said,
  you may proceed but state clearly:
  "I found 'dag_orders_etl' — assuming that's what you meant. Generating workflow..."

- Credentials are named strings the user configured: e.g. PROD_AIRFLOW, PROD_MSSQL, AWS_PROD
"""


# ── Rule-based DSL fallback ───────────────────────────────────────────────────

def _node(nid, ntype, title, x, y, props):
    return {"id": nid, "type": ntype, "title": title, "x": x, "y": y, "props": props}

def _edge(s, d):
    return {"from": s, "to": d}


def rule_based_dsl(prompt: str) -> dict:
    """
    Parse a natural-language prompt and produce a FlowForge DSL dict.

    Handles:
      - Multiple DAGs in sequence, with or without conditions between them
      - "completed within N minutes/seconds" → IF node on elapsed_seconds
      - "all tasks starting with PREFIX succeeded" → validate_task_patterns + IF node
      - "if [DAG] [condition] then trigger [DAG B]" → conditional IF gate + branched edge
      - SQL with upstream expression refs: "with [field] of [node] in [column]"
      - "more than N entries/rows", "at least N" → assert_greater_than
      - HTTP/Slack/S3/Azure/SFTP nodes from keyword detection
    """
    import re as _re

    # ── Normalise common voice/OCR transcription errors ─────────────────────
    # "tag a" → "dag a"  (speech recognition mishears "dag" as "tag")
    import re as _re2
    _norm = prompt
    _norm = _re2.sub(r'\btag\s+([a-z0-9])', lambda m: 'dag ' + m.group(1), _norm, flags=_re2.IGNORECASE)
    _norm = _re2.sub(r'\bdag\s+this\b', 'dag b', _norm, flags=_re2.IGNORECASE)   # "dag this run id" → "dag b"
    # "cloud extraction progress" → "cloud_extraction_progress" (multi-word table names)
    # General: consecutive lowercase words before "table" joined with underscore
    _norm = _re2.sub(
        r'\b([a-z][a-z0-9]*)\s+([a-z][a-z0-9]*)\s+([a-z][a-z0-9]*)\s+(table\b)',
        lambda m: f"{m.group(1)}_{m.group(2)}_{m.group(3)} {m.group(4)}",
        _norm, flags=_re2.IGNORECASE
    )
    _norm = _re2.sub(
        r'\b([a-z][a-z0-9]*)\s+([a-z][a-z0-9]*)\s+(table\b)',
        lambda m: f"{m.group(1)}_{m.group(2)} {m.group(3)}",
        _norm, flags=_re2.IGNORECASE
    )
    # "created by column" → keep as-is (regex handles spaces in column name)
    prompt = _norm
    # ─────────────────────────────────────────────────────────────────────────
    p    = prompt.lower()
    wds  = set(_re.findall(r'\b\w+\b', p))
    nodes: list  = []
    edges: list  = []
    x, n, prev_id = 60, 0, None
    y_alt = [180, 220]

    def _nid():
        nonlocal n
        n += 1
        return f"n{n}"

    def add(ntype, title, props, *, from_id=None, branch=None):
        """Add a node and an edge from from_id (default=prev) with optional branch label."""
        nonlocal x, prev_id
        nid = _nid()
        nodes.append({"id": nid, "type": ntype, "title": title,
                       "x": x, "y": y_alt[(len(nodes)) % 2], "props": props})
        src = from_id if from_id is not None else prev_id
        if src:
            e = {"from": src, "to": nid}
            if branch:
                e["branch"] = branch
            edges.append(e)
        prev_id = nid
        x += 230
        return nid

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _minutes_to_seconds(text):
        """Extract a time limit from text like 'within 10 minutes' → 600"""
        m = _re.search(r'within\s+(\d+)\s*(minute|min|second|sec|hour)', text)
        if not m:
            return None
        val  = int(m.group(1))
        unit = m.group(2)
        if unit.startswith('hour'):   return val * 3600
        if unit.startswith('min'):    return val * 60
        return val  # seconds

    def _task_pattern(text):
        """Extract task prefix: 'tasks starts with QA' → 'QA'"""
        m = _re.search(r'tasks?\s+(?:start(?:s|ing)?\s+with|prefix(?:ed)?\s+with|named?|beginning\s+with)\s+["\']?([A-Za-z0-9_\-]+)["\']?', text)
        return m.group(1) if m else None

    def _extract_dags(text):
        """Extract all DAG ids in order from text."""
        matches = list(_re.finditer(
            r'\bdag(?:([_\-\s]+)([a-z0-9][a-z0-9_\-]*)|([\d][a-z0-9_]*))', text
        ))
        _skip = {"id","run","the","for","and","then","wait","complete","after","trigger",
                 "names","name","task","tasks","is","are","at","to","with","when",
                 "using","from","in","of","or","its","has","been"}
        seen = set(); dag_ids = []
        for m in matches:
            d = m.group(2) if m.group(2) else ('dag' + m.group(3) if m.group(3) else None)
            if d and d not in seen and d not in _skip:
                dag_ids.append(d); seen.add(d)
        return dag_ids

    # ── Parse the overall structure ───────────────────────────────────────────
    # Split on "then" / "if [dag] X then" / "after" to understand sequencing
    dag_ids     = _extract_dags(p)
    time_limit  = _minutes_to_seconds(p)
    task_pat    = _task_pattern(prompt)  # use original prompt to preserve case (e.g. "QA" not "qa")
    # "all tasks completed" with no prefix: assert all tasks passed on the relevant DAG
    wants_all_tasks = any(ph in p for ph in (
        "all tasks completed", "all the tasks", "check all tasks",
        "all tasks succeeded", "all tasks passed", "check all the tasks",
        "once it is done", "once done",
    ))
    wants_wait  = any(ph in p for ph in ("wait for","wait until","complete","finish",
                                          "done before","after it","then trigger"))

    # Detect conditional trigger pattern: "if DAG A [condition] then trigger DAG B"
    has_condition = bool(time_limit or task_pat or
                         _re.search(r'\bif\b.*\bthen\b', p) or
                         _re.search(r'\bonly\s+if\b', p))

    # Detect upstream reference in SQL: "with DAG run id of DAG B in created_by column"
    # Pattern: "with [field phrase] of [node ref] in [column] column"
    upstream_col_m  = _re.search(
        r'with\s+(?:the\s+)?(?:dag\s+)?run\s+id\s+of\s+dag\s+(\w+)\s+in\s+(\w+(?:\s+\w+)?)\s+column',
        p
    )
    # Also: "dag run id of DAG B" → column name
    upstream_col_m2 = _re.search(
        r'(?:in|using)\s+(?:the\s+)?(?:created[_\s]by|(\w+(?:_\w+)?))\s+column',
        p
    ) if not upstream_col_m else None

    # ── Node 1: Trigger ───────────────────────────────────────────────────────
    if any(w in wds for w in ("webhook","rest","external","incoming")):
        add("webhook", "Webhook Trigger", {"method": "POST", "auth_token": ""})
    else:
        cron = "0 6 * * *"
        if "hourly" in p:   cron = "0 * * * *"
        elif "weekly" in p: cron = "0 6 * * 1"
        elif "manual" in p: cron = "manual"
        else:
            m2 = _re.search(r'every\s+(\d+)\s*min', p)
            if m2: cron = f"*/{m2.group(1)} * * * *"
        add("trigger", "Schedule Trigger", {"cron": cron, "timezone": "UTC"})

    # ── Airflow nodes + conditions ─────────────────────────────────────────────
    if any(w in wds for w in ("dag","airflow","pipeline","etl")) or dag_ids:
        all_dags = dag_ids or ["my_dag"]

        for i, dag_id in enumerate(all_dags):
            is_first  = (i == 0)
            is_last   = (i == len(all_dags) - 1)
            is_cond   = has_condition and not is_last  # conditions apply between first and subsequent

            dag_label = dag_id.upper()
            dag_props = {
                "dag_id":              dag_id,
                "wait_for_completion": wants_wait or True,
                "wait_if_running":     True,
                "timeout":             3600,
                "retries":             2,
                "credential":          "PROD_AIRFLOW",
                # Don't assert_no_failed_tasks on first dag if we're doing pattern check
                "assert_no_failed_tasks": not (is_first and task_pat),
            }

            # If there's a task pattern to check, add it to the first DAG
            if is_first and task_pat:
                dag_props["validate_task_patterns"] = [task_pat]

            dag_node_id = add("airflow", f"Trigger DAG {dag_label}", dag_props)

            # After first DAG, insert IF conditions before triggering the next one
            if is_first and has_condition and len(all_dags) > 1:
                # ── IF: time limit check ─────────────────────────────────────
                if time_limit:
                    time_if_id = add("if", f"DAG {dag_label} Within Time Limit", {
                        "left_value":  f"{{{{$node.Trigger DAG {dag_label}.output.elapsed_seconds}}}}",
                        "operator":    "less_than",
                        "right_value": str(time_limit),
                        "description": f"Check DAG {dag_label} completed within {time_limit}s"
                    })
                    # Next node only runs on true branch
                    prev_id = time_if_id   # so the next add() connects from here
                    # Mark the edge to the next node as branch=true
                    _prev_cond_id = time_if_id

                # ── IF: task pattern check ────────────────────────────────────
                if task_pat:
                    pat_key    = task_pat.lower().replace(" ", "_")
                    pat_if_id  = add("if", f"{task_pat} Tasks All Passed", {
                        "left_value":  f"{{{{$node.Trigger DAG {dag_label}.output.{pat_key}_tasks_all_passed}}}}",
                        "operator":    "equals",
                        "right_value": "true",
                        "description": f"Check all tasks prefixed '{task_pat}' in DAG {dag_label} succeeded"
                    })

                # The edge from the last IF to the next DAG must be branch=true
                # Patch the *next* edge that add() will create
                # We do this by overriding edge creation via a flag
                # → store that the next edge should be branch=true
                # We handle this by setting a module-level flag via closure trick:
                # Actually, we change approach: manually add the next DAG node with branch

    # ── Re-approach: build the graph in two passes for conditional prompts ───
    # The above approach has a structural problem with branch edge injection.
    # Use a clean declarative plan instead.
    pass

    # ── CLEAN DECLARATIVE REBUILD ─────────────────────────────────────────────
    # Discard the incremental approach above for complex conditional prompts
    # and build the full graph from the parsed intent.

    # Reset
    nodes.clear(); edges.clear()
    x = 60; n = 0; prev_id = None

    # ── 1. Trigger ────────────────────────────────────────────────────────────
    _schedule_words = ("daily","hourly","weekly","every","cron","schedule","scheduled",
                       "morning","night","midnight","monday","friday","weekend","weekday",
                       "at 6","at 8","at midnight","at noon","pm","am")
    _has_schedule = any(w in p for w in _schedule_words)
    if any(w in wds for w in ("webhook", "rest", "external", "incoming")):
        add("webhook", "Webhook Trigger", {"method": "POST", "auth_token": ""})
    elif _has_schedule:
        _cron = "0 6 * * *"
        if "hourly" in p:   _cron = "0 * * * *"
        elif "weekly" in p: _cron = "0 6 * * 1"
        else:
            _m2 = _re.search(r'every\s+(\d+)\s*min', p)
            if _m2: _cron = f"*/{_m2.group(1)} * * * *"
        add("trigger", "Schedule Trigger", {"cron": _cron, "timezone": "UTC"})
    else:
        add("trigger", "Manual Trigger", {"cron": "manual", "timezone": "UTC"})

    all_dags = dag_ids or []

    if all_dags:
        dag_a = all_dags[0]
        dag_b = all_dags[1] if len(all_dags) > 1 else None

        # ── 2. Trigger DAG A ─────────────────────────────────────────────────
        dag_a_props = {
            "dag_id":                  dag_a,
            "wait_for_completion":     True,
            "wait_if_running":         True,
            "timeout":                 3600,
            "retries":                 2,
            "credential":              "PROD_AIRFLOW",
            "assert_no_failed_tasks":  not bool(task_pat),  # False if we check via pattern
        }
        if task_pat:
            dag_a_props["validate_task_patterns"] = [task_pat]

        dag_a_id = add("airflow", f"Trigger DAG {dag_a.upper()}", dag_a_props)

        # ── 2b. Validation IF node (wants_all_tasks and no complex conditions) ─
        # When user says "validate all tasks completed" without a DAG B or time limit,
        # add an explicit IF node that checks final_state == success so it's visible in the graph.
        if wants_all_tasks and not dag_b and not time_limit and not task_pat:
            last_if_id = add("if", "All Tasks Completed", {
                "left_value":  f"{{{{$node.Trigger DAG {dag_a.upper()}.output.final_state}}}}",
                "operator":    "equals",
                "right_value": "success",
                "description": "Verify all tasks in DAG completed successfully",
            })
        else:
            last_if_id = None

        # ── 3. IF: time limit check (if "within N minutes") ──────────────────
        # Determine whether conditions (time/task) apply TO dag B or GATE dag B.
        # "trigger B ... check tasks within 5 min" -> conditions DESCRIBE B (after it runs)
        # "if A done within 10 min then trigger B" -> conditions GATE B (before it runs)
        p_lower   = prompt.lower()
        dag_b_pos = p_lower.find(f"trigger dag {dag_b}") if dag_b else -1
        cond_pos  = max(
            p_lower.find("within"),
            p_lower.find("all tasks"),
            p_lower.find("check all"),
            p_lower.find("all the tasks"),
            p_lower.find("once it"),
        )
        cond_on_b = dag_b and dag_b_pos >= 0 and cond_pos > dag_b_pos

        # last_if_id may already be set by wants_all_tasks check above
        if not (wants_all_tasks and not dag_b and not time_limit and not task_pat):
            last_if_id = None

        # Gate conditions (apply BEFORE triggering B)
        if not cond_on_b:
            if time_limit:
                last_if_id = add("if", f"DAG {dag_a.upper()} Within Time Limit", {
                    "left_value":  f"{{{{$node.Trigger DAG {dag_a.upper()}.output.elapsed_seconds}}}}",
                    "operator":    "less_than",
                    "right_value": str(time_limit),
                })
            if task_pat:
                pat_key   = task_pat.lower().replace(" ","_").rstrip("_")
                qa_from   = last_if_id if last_if_id else dag_a_id
                qa_branch = "true" if last_if_id else None
                last_if_id = add("if", f"All {task_pat} Tasks Passed", {
                    "left_value":  f"{{{{$node.Trigger DAG {dag_a.upper()}.output.{pat_key}_tasks_all_passed}}}}",
                    "operator":    "equals", "right_value": "true",
                }, from_id=qa_from, branch=qa_branch)

        # Trigger DAG B
        if dag_b:
            dag_b_src_id = last_if_id if last_if_id else dag_a_id
            b_props = {
                "dag_id":                 dag_b,
                "wait_for_completion":    True,
                "wait_if_running":        True,
                "timeout":                3600,
                "retries":                2,
                "credential":             "PROD_AIRFLOW",
                "assert_no_failed_tasks": True,
            }
            if cond_on_b and task_pat:
                b_props["validate_task_patterns"] = [task_pat]
            dag_b_id = add("airflow", f"Trigger DAG {dag_b.upper()}", b_props,
                from_id=dag_b_src_id,
                branch="true" if (last_if_id and not cond_on_b) else None)
        else:
            dag_b_id = None

        # Post-B conditions (apply AFTER B completes)
        if cond_on_b and dag_b_id:
            last_if_id = None
            if time_limit:
                last_if_id = add("if", f"DAG {dag_b.upper()} Within Time Limit", {
                    "left_value":  f"{{{{$node.Trigger DAG {dag_b.upper()}.output.elapsed_seconds}}}}",
                    "operator":    "less_than",
                    "right_value": str(time_limit),
                }, from_id=dag_b_id)
            if task_pat:
                pat_key   = task_pat.lower().replace(" ","_").rstrip("_")
                tf_from   = last_if_id if last_if_id else dag_b_id
                tf_branch = "true" if last_if_id else None
                last_if_id = add("if", f"All {task_pat} Tasks Passed", {
                    "left_value":  f"{{{{$node.Trigger DAG {dag_b.upper()}.output.{pat_key}_tasks_all_passed}}}}",
                    "operator":    "equals", "right_value": "true",
                }, from_id=tf_from, branch=tf_branch)


        # ── 6. Additional DAGs beyond B ───────────────────────────────────────
        for dag_id in all_dags[2:]:
            add("airflow", f"Trigger DAG {dag_id.upper()}", {
                "dag_id": dag_id, "wait_for_completion": True, "wait_if_running": True,
                "timeout": 3600, "retries": 2, "credential": "PROD_AIRFLOW",
                "assert_no_failed_tasks": True,
            })

    elif any(w in wds for w in ("airflow","pipeline","etl")):
        add("airflow", "Run DAG", {
            "dag_id": "my_dag", "wait_for_completion": True, "wait_if_running": True,
            "timeout": 3600, "retries": 2, "credential": "PROD_AIRFLOW",
            "assert_no_failed_tasks": True,
        })
        dag_b_id = None
    else:
        dag_b_id = None

    # ── SQL node ──────────────────────────────────────────────────────────────
    sql_triggers = ("sql","query","select","database","db","table","row","count",
                    "entries","having","records","fetch")
    if any(w in wds for w in sql_triggers) or "more than" in p or "greater than" in p:

        # Table name
        # Multi-strategy table name extraction:
        # 1. "from table_name" or "from tablename" (explicit name)
        table = "table_name"
        _stop = {'the','an','a','this','that','my','our','and','or','in','with',
                 'using','use','run','id','dag','by','column','created','records',
                 'number','of','for','search','find','its','all','done','once'}
        # Strategy A: "table <identifier>" — captures full snake_case names like cloud_extraction_progress
        tbl_word_m   = _re.search(r'\btable\s+([a-z][a-z0-9_]+)', p)
        # Strategy B: "<name> table" (name before the word table)
        tbl_before_m = _re.search(r'\bin\s+([a-z][a-z0-9_]+)\s+table', p)
        # Strategy C: "from <name>" (not starting with 'table')
        tbl_from_m   = _re.search(r'\bfrom\s+(?!table\b)([a-z][a-z0-9_]+)', p)
        if tbl_word_m and tbl_word_m.group(1) not in _stop:
            table = tbl_word_m.group(1)
        elif tbl_before_m and tbl_before_m.group(1) not in _stop:
            table = tbl_before_m.group(1)
        elif tbl_from_m and tbl_from_m.group(1) not in _stop:
            table = tbl_from_m.group(1)
        if table in ('table', 'table_name', '') or table in (dag_ids or []):
            table = 'result_table'

        # Detect count intent: "number of records", "how many", etc.
        wants_count = any(ph in p for ph in (
            "number of records", "number of rows", "how many records",
            "how many rows", "count of records", "record count", "row count",
        ))

        # Detect DAG run ID reference — supports:
        #   "dag B run id", "dag run id of dag B", "using the dag run id", "the dag B run ID"
        run_id_ref_m = _re.search(
            r'(?:'
            r'dag\s+(\w+)\s+run\s+id'       # "dag B run id"
            r'|dag\s+this\s+run\s+id'         # "dag this run id" (voice transcription)
            r'|run\s+id\s+of\s+dag\s+(\w+)' # "run id of dag B"
            r'|(?:with|using|use)\s+(?:the\s+)?(?:this\s+)?(?:dag\s+)?run\s+(?:id|ids?)' # generic
            r')',
            p
        )
        col_name_m = _re.search(r'in\s+(?:the\s+)?(\w+(?:[_\s]\w+)?)\s+column', p)

        query = f"SELECT COUNT(*) AS record_count FROM {table}" if wants_count else f"SELECT * FROM {table}"

        if run_id_ref_m:
            # Determine which DAG the run ID belongs to
            ref_dag_raw = run_id_ref_m.group(1) or (run_id_ref_m.group(2) if run_id_ref_m.lastindex and run_id_ref_m.lastindex >= 2 else None)
            if ref_dag_raw and ref_dag_raw not in ('this', 'the', 'a'):
                ref_dag = ref_dag_raw.upper()
            elif dag_b:
                ref_dag = dag_b.upper()
            else:
                ref_dag = (dag_ids[-1] if dag_ids else 'B').upper()
            col_name = col_name_m.group(1).replace(" ", "_") if col_name_m else "created_by"
            where = f"{col_name} = '{{{{$node.Trigger DAG {ref_dag}.output.dag_run_id}}}}'"
            if wants_count:
                query = f"SELECT COUNT(*) AS record_count FROM {table} WHERE {where}"
            else:
                query = f"SELECT * FROM {table} WHERE {where}"
        else:
            gt_m = _re.search(r'(?:more than|greater than|over)\s+(\d+)', p)
            ge_m = _re.search(r'(?:at least|minimum)\s+(\d+)', p)
            if gt_m or ge_m:
                query = f"SELECT COUNT(*) AS cnt FROM {table}"

        sql_props = {
            "query":       query,
            "credential":  "PROD_MSSQL",
            "store_rows":  True,
            "export_format": "none",
        }

        gt_m2 = _re.search(r'(?:more than|greater than|over)\s+(\d+)', p)
        ge_m2 = _re.search(r'(?:at least|minimum)\s+(\d+)', p)
        if gt_m2:
            sql_props["extract_column"]      = "cnt"
            sql_props["output_as"]           = "row_count"
            sql_props["assert_greater_than"] = int(gt_m2.group(1))
        elif ge_m2:
            sql_props["extract_column"]      = "cnt"
            sql_props["output_as"]           = "row_count"
            sql_props["assert_greater_than"] = int(ge_m2.group(1)) - 1

        if wants_count and run_id_ref_m:
            sql_props["extract_column"] = "record_count"
            sql_props["output_as"]      = "record_count"

        title = ("Count Records in " + table) if wants_count else ("Validate Row Count" if (gt_m2 or ge_m2) else f"Fetch from {table}")
        add("sql", title, sql_props,
            from_id=last_if_id if ('last_if_id' in dir() and last_if_id) else None,
            branch="true" if ('last_if_id' in dir() and last_if_id) else None)


    # ── HTTP / Slack ───────────────────────────────────────────────────────────
    if any(w in wds for w in ("slack","notify","alert")) and "call workflow" not in p:
        add("http", "Notify Slack", {
            "method": "POST",
            "url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
            "body": '{"text":"FlowForge: Workflow completed ✓"}',
            "expected_status": 200,
        })

    # ── S3 ────────────────────────────────────────────────────────────────────
    if any(w in wds for w in ("s3","bucket")) and "azure" not in wds:
        op = "download" if "download" in wds else ("list" if "list" in wds else "upload")
        add("s3", f"S3 {op.title()}", {
            "bucket": "my-bucket", "key": "data/output.csv",
            "operation": op, "credential": "AWS_PROD",
        })

    # ── Fallback: if only trigger was added ───────────────────────────────────
    if len(nodes) == 1:
        add("airflow", "Run DAG", {
            "dag_id": "my_dag", "wait_for_completion": True, "wait_if_running": True,
            "timeout": 3600, "retries": 2, "credential": "PROD_AIRFLOW",
            "assert_no_failed_tasks": True,
        })
        add("sql", "Validate Result", {
            "query": "SELECT COUNT(*) FROM result_table",
            "credential": "PROD_MSSQL", "store_rows": True,
        })

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



# ── DSL Validator ─────────────────────────────────────────────────────────────

VALID_NODE_TYPES = {
    "trigger", "webhook", "airflow", "sql", "http", "s3", "azure",
    "sftp", "wait", "if", "set_variable", "get_variable", "code", "call_workflow",
}

VALID_OPERATORS = {
    "equals", "not_equals", "greater_than", "less_than",
    "contains", "is_empty", "not_empty",
}

REQUIRED_PROPS: dict = {
    "trigger":      ["cron"],
    "airflow":      ["dag_id", "credential"],
    "sql":          ["query", "credential"],
    "http":         ["method", "url"],
    "s3":           ["bucket", "operation", "credential"],
    "azure":        ["container", "operation", "credential"],
    "sftp":         ["remote_path", "operation", "credential"],
    "wait":         ["duration", "unit"],
    "if":           ["left_value", "operator", "right_value"],
    "set_variable": ["key", "value"],
    "get_variable": ["key"],
    "code":         ["code"],
    "call_workflow":["workflow_id"],
}


def validate_dsl(dsl: dict) -> list[str]:
    """
    Validate a FlowForge DSL dict. Returns a list of error strings.
    Empty list means valid.

    Checks:
      - Top-level keys (name, nodes, edges)
      - Every node has id, type, title, x, y, props
      - Node types are from the known set
      - Node IDs are unique
      - Required props present per node type
      - IF operator is a known value
      - Expression syntax is well-formed (no unclosed braces)
      - Every edge references existing node IDs
      - Branch values on edges are "true" or "false" only
      - No duplicate edges
      - Graph has at least one non-trigger node
    """
    errors: list[str] = []

    if not isinstance(dsl, dict):
        return ["DSL must be a JSON object"]

    if not dsl.get("name"):
        errors.append("Missing 'name' field")

    nodes_raw = dsl.get("nodes")
    edges_raw = dsl.get("edges")

    if not isinstance(nodes_raw, list) or len(nodes_raw) == 0:
        errors.append("'nodes' must be a non-empty array")
        return errors  # can't proceed

    if not isinstance(edges_raw, list):
        errors.append("'edges' must be an array")
        edges_raw = []

    # ── Node checks ───────────────────────────────────────────────────────────
    ids_seen: set = set()
    node_ids: set = set()

    for i, node in enumerate(nodes_raw):
        prefix = f"Node[{i}]"

        if not isinstance(node, dict):
            errors.append(f"{prefix}: must be an object"); continue

        nid    = node.get("id", "")
        ntype  = node.get("type", "")
        ntitle = node.get("title", "")
        props  = node.get("props", {})

        # Required structural fields
        for field in ("id", "type", "title"):
            if not node.get(field):
                errors.append(f"{prefix}: missing '{field}'")

        for coord in ("x", "y"):
            if not isinstance(node.get(coord), (int, float)):
                errors.append(f"{prefix} '{ntitle or nid}': '{coord}' must be a number")

        if not isinstance(props, dict):
            errors.append(f"{prefix} '{ntitle or nid}': 'props' must be an object")
            props = {}

        # Unique IDs
        if nid:
            if nid in ids_seen:
                errors.append(f"Duplicate node id '{nid}'")
            ids_seen.add(nid)
            node_ids.add(nid)

        # Valid type
        if ntype and ntype not in VALID_NODE_TYPES:
            errors.append(f"Node '{ntitle or nid}': unknown type '{ntype}'. "
                          f"Valid: {sorted(VALID_NODE_TYPES)}")

        # Required props per type
        for req in REQUIRED_PROPS.get(ntype, []):
            if req not in props or props[req] in (None, "", [], {}):
                errors.append(f"Node '{ntitle or nid}' ({ntype}): missing required prop '{req}'")

        # IF: operator must be valid
        if ntype == "if":
            op = props.get("operator", "")
            if op and op not in VALID_OPERATORS:
                errors.append(f"Node '{ntitle or nid}' (if): unknown operator '{op}'. "
                              f"Valid: {sorted(VALID_OPERATORS)}")

        # Expression well-formedness: {{ must be closed with }}
        for key, val in props.items():
            if isinstance(val, str) and "{{" in val:
                opens  = val.count("{{")
                closes = val.count("}}")
                if opens != closes:
                    errors.append(f"Node '{ntitle or nid}' prop '{key}': "
                                  f"unclosed expression ({{ count {opens} != }} count {closes})")
                # Check $node references point to a real title
                # Collect for post-loop validation (node_titles built after all nodes parsed)
                pass  # validated in second pass below

    # ── Edge checks ───────────────────────────────────────────────────────────
    node_titles = {n.get("title", ""): n.get("id", "") for n in nodes_raw}
    seen_edges: set = set()

    for i, edge in enumerate(edges_raw):
        if not isinstance(edge, dict):
            errors.append(f"Edge[{i}]: must be an object"); continue

        src = edge.get("from", "")
        dst = edge.get("to", "")

        if not src:
            errors.append(f"Edge[{i}]: missing 'from'")
        elif src not in node_ids:
            errors.append(f"Edge[{i}]: 'from' id '{src}' does not match any node id")

        if not dst:
            errors.append(f"Edge[{i}]: missing 'to'")
        elif dst not in node_ids:
            errors.append(f"Edge[{i}]: 'to' id '{dst}' does not match any node id")

        branch = edge.get("branch")
        if branch is not None and branch not in ("true", "false"):
            errors.append(f"Edge[{i}] ({src}→{dst}): branch must be 'true' or 'false', got '{branch}'")

        key = (src, dst)
        if key in seen_edges:
            errors.append(f"Duplicate edge {src}→{dst}")
        seen_edges.add(key)

    # ── Expression title cross-check ─────────────────────────────────────────
    # Verify {{$node.Title.output.field}} references match actual node titles
    node_title_set = {n.get("title", "") for n in nodes_raw}
    for node in nodes_raw:
        for key, val in (node.get("props") or {}).items():
            if not isinstance(val, str): continue
            for ref_title in re.findall(r'\{\{\$node\.(.+?)\.output\..+?\}\}', val):
                if ref_title not in node_title_set:
                    errors.append(
                        f"Node '{node.get('title',node.get('id','?'))}' prop '{key}': "
                        f"expression references unknown node title '{ref_title}'. "
                        f"Available titles: {sorted(node_title_set)}"
                    )

    # ── Graph-level checks ────────────────────────────────────────────────────
    non_trigger = [n for n in nodes_raw if n.get("type") != "trigger"]
    if not non_trigger:
        errors.append("Workflow must have at least one non-trigger node")

    # Cycle detection — topo sort drops cycle nodes silently; catch it here
    from collections import defaultdict as _dd, deque as _dq
    _ids   = {n.get("id","") for n in nodes_raw}
    _indeg = {i: 0 for i in _ids}
    _adj   = _dd(list)
    for e in edges_raw:
        s = e.get("from",""); d = e.get("to","")
        if s in _ids and d in _ids:
            _adj[s].append(d); _indeg[d] += 1
    _q = _dq(i for i in _indeg if _indeg[i] == 0)
    _visited = 0
    while _q:
        _n = _q.popleft(); _visited += 1
        for _nb in _adj[_n]:
            _indeg[_nb] -= 1
            if _indeg[_nb] == 0: _q.append(_nb)
    if _visited < len(_ids):
        errors.append(
            f"Workflow contains a cycle — {len(_ids) - _visited} node(s) are part of a loop "
            f"and would never execute. Remove circular edges."
        )

    # Warn if an IF node exists but has no branch-labelled outgoing edges
    if_nodes = {n["id"] for n in nodes_raw if n.get("type") == "if"}
    for if_id in if_nodes:
        out_edges = [e for e in edges_raw if e.get("from") == if_id]
        branched  = [e for e in out_edges if e.get("branch") in ("true", "false")]
        if out_edges and not branched:
            title = next((n.get("title","?") for n in nodes_raw if n.get("id")==if_id), if_id)
            errors.append(f"IF node '{title}': has outgoing edges but none have branch labels "
                          f"(add branch='true'/'false' to control which path runs)")

    return errors


# ── Agentic DSL Generator with validation + retry ────────────────────────────


def extract_dsl(text: str):
    # Strategy 1: fenced JSON block  ```json { ... } ```
    for m in re.finditer(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL):
        try:
            parsed = json.loads(m.group(1))
            if isinstance(parsed, dict) and "nodes" in parsed:
                return parsed
        except Exception:
            pass
    # Strategy 2: scan for all top-level { } pairs and pick the one with "nodes"
    depth = 0; start = -1
    candidates = []
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                candidates.append(text[start:i+1])
    for blob in sorted(candidates, key=len, reverse=True):
        try:
            parsed = json.loads(blob)
            if isinstance(parsed, dict) and "nodes" in parsed:
                return parsed
        except Exception:
            pass
    return None


# ── Agentic tool-use loop ─────────────────────────────────────────────────────

async def _run_tool(name, args, cred_mgr, owner_id):
    import asyncio
    from mcp_servers.tool_registry import execute_tool
    loop = asyncio.get_running_loop()
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

    client    = anthropic.Anthropic(
        api_key=ANTHROPIC_API_KEY,
        # Suppress SDK telemetry headers (X-Stainless-*) for corporate networks
        default_headers={
            "X-Stainless-Lang": "",
            "X-Stainless-Package-Version": "",
            "X-Stainless-OS": "",
            "X-Stainless-Arch": "",
            "X-Stainless-Runtime": "",
            "X-Stainless-Runtime-Version": "",
        },
    )
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
        client = anthropic.Anthropic(
            api_key=ANTHROPIC_API_KEY,
            default_headers={
                "X-Stainless-Lang": "",
                "X-Stainless-Package-Version": "",
                "X-Stainless-OS": "",
                "X-Stainless-Arch": "",
                "X-Stainless-Runtime": "",
                "X-Stainless-Runtime-Version": "",
            },
        )
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
    dsl_source:   str  = "ai" # "ai" always


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

    if not ANTHROPIC_API_KEY:
        return ChatResponse(
            reply="ANTHROPIC_API_KEY is not set. Please set it in your environment to use FlowForge AI.",
            session_id=session_id, tool_calls=[], dsl_source="",
        )

    try:
        from credential_manager import CredentialManager
        cred_mgr = CredentialManager(db)
    except Exception:
        cred_mgr = None

    # ── Single AI path: Claude uses MCP tools then generates DSL ──────────────
    # Claude decides what tools to call (discover real DAGs/tables/buckets),
    # plans the workflow, and emits DSL JSON. No rule-based fallback.
    try:
        reply_text, tool_calls = await call_with_tools(
            messages, cred_mgr=cred_mgr, owner_id=current_user["sub"]
        )
        if not reply_text:
            reply_text = "AI did not return a response. Please try again."
    except Exception as e:
        logger.error(f"[AI] call_with_tools failed: {e}")
        reply_text = f"AI request failed: {e}. Please check your API key and try again."

    # Extract DSL if Claude included a workflow JSON block
    workflow_dsl = extract_dsl(reply_text)
    if workflow_dsl:
        reply_text = re.sub(r"```(?:json)?.*?```", "", reply_text, flags=re.DOTALL).strip()

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

    return ChatResponse(
        reply=reply_text, workflow_dsl=workflow_dsl,
        session_id=session_id, tool_calls=tool_calls,
        dsl_source="ai",
    )


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

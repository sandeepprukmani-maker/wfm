"""
FlowForge — Workflow Templates Router (Sprint 3)

10 production-ready templates covering the most common enterprise patterns.
Templates are read-only built-ins; cloning creates a real Workflow record.

Endpoints:
  GET  /api/templates/              — list all templates
  GET  /api/templates/{id}          — get template detail + full DSL
  POST /api/templates/{id}/clone    — clone template → new Workflow, return workflow_id
"""

import logging
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db, Workflow, WorkflowVersion

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Template definitions ──────────────────────────────────────────────────────

def _n(nid, ntype, title, x, y, props):
    return {"id": nid, "type": ntype, "title": title, "x": x, "y": y, "props": props}

def _e(src, dst):
    return {"from": src, "to": dst}


TEMPLATES = [

    {
        "id":          "tpl-etl-validate",
        "name":        "ETL Pipeline Validation",
        "description": "Trigger an Airflow ETL DAG, wait for completion, validate row count in MSSQL, then notify Slack.",
        "category":    "data-engineering",
        "tags":        ["airflow", "sql", "slack", "etl"],
        "complexity":  "medium",
        "dsl": {
            "name": "ETL Pipeline Validation",
            "description": "Triggers ETL DAG, validates output row count, sends Slack notification",
            "nodes": [
                _n("n1","trigger","Daily Schedule",60,180,{"cron":"0 6 * * *","timezone":"UTC"}),
                _n("n2","airflow","Run ETL DAG",290,180,{"dag_id":"etl_pipeline","timeout":7200,"retries":2,"credential":"PROD_AIRFLOW","assert_no_failed_tasks":True}),
                _n("n3","sql","Validate Row Count",520,180,{"query":"SELECT COUNT(*) FROM staging.etl_results WHERE run_date = CAST(GETDATE() AS DATE)","credential":"PROD_MSSQL","min_row_count":1,"store_rows":True}),
                _n("n4","http","Notify Slack",750,180,{"method":"POST","url":"https://hooks.slack.com/services/YOUR/WEBHOOK","expected_status":200,"body":'{"text":"✅ ETL complete — {{$node.Validate Row Count.output.rows_returned}} rows loaded"}'}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4")],
        },
    },

    {
        "id":          "tpl-data-quality",
        "name":        "Data Quality Gate",
        "description": "Run two SQL quality checks (no errors, min row count), branch on result, alert on failure.",
        "category":    "data-quality",
        "tags":        ["sql", "if", "slack", "quality"],
        "complexity":  "medium",
        "dsl": {
            "name": "Data Quality Gate",
            "description": "SQL quality checks with IF/ELSE branching and Slack alerting on failure",
            "nodes": [
                _n("n1","trigger","Daily Schedule",60,180,{"cron":"0 7 * * *","timezone":"UTC"}),
                _n("n2","sql","Check Error Log",290,180,{"query":"SELECT COUNT(*) FROM dbo.error_log WHERE run_date = CAST(GETDATE() AS DATE)","credential":"PROD_MSSQL","assert_no_rows":True}),
                _n("n3","sql","Check Row Count",520,180,{"query":"SELECT COUNT(*) FROM dbo.daily_summary WHERE summary_date = CAST(GETDATE() AS DATE)","credential":"PROD_MSSQL","min_row_count":100}),
                _n("n4","if","Row Count OK?",750,180,{"left_value":"{{$node.Check Row Count.output.rows_returned}}","operator":"greater_than","right_value":"99","assert_true":True}),
                _n("n5","http","Slack: Quality Passed",980,140,{"method":"POST","url":"https://hooks.slack.com/services/YOUR/WEBHOOK","expected_status":200,"body":'{"text":"✅ Data quality passed"}'}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4"),_e("n4","n5")],
        },
    },

    {
        "id":          "tpl-s3-backup",
        "name":        "S3 Backup & Verify",
        "description": "Upload a file to S3, verify it exists, then log success.",
        "category":    "storage",
        "tags":        ["s3", "http"],
        "complexity":  "simple",
        "dsl": {
            "name": "S3 Backup & Verify",
            "description": "Upload to S3, verify object exists, notify on completion",
            "nodes": [
                _n("n1","trigger","Manual / Scheduled",60,180,{"cron":"0 2 * * *","timezone":"UTC"}),
                _n("n2","s3","Upload to S3",290,180,{"bucket":"my-backup-bucket","key":"daily/backup_{{$execution.id}}.csv","operation":"upload","local_path":"/data/export.csv","credential":"AWS_PROD"}),
                _n("n3","s3","Verify Object Exists",520,180,{"bucket":"my-backup-bucket","key":"daily/backup_{{$execution.id}}.csv","operation":"exists","credential":"AWS_PROD","assert_exists":True}),
                _n("n4","http","Log Completion",750,180,{"method":"POST","url":"https://hooks.slack.com/services/YOUR/WEBHOOK","expected_status":200,"body":'{"text":"✅ S3 backup verified"}'}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4")],
        },
    },

    {
        "id":          "tpl-sftp-ingest",
        "name":        "SFTP Ingest → SQL Load",
        "description": "Check SFTP for a new file, download it, trigger an Airflow load DAG, validate the loaded rows.",
        "category":    "data-engineering",
        "tags":        ["sftp", "airflow", "sql"],
        "complexity":  "complex",
        "dsl": {
            "name": "SFTP Ingest → SQL Load",
            "description": "SFTP file check, download, Airflow load, SQL validation",
            "nodes": [
                _n("n1","trigger","Hourly Check",60,180,{"cron":"0 * * * *","timezone":"UTC"}),
                _n("n2","sftp","Check File Exists",290,180,{"remote_path":"/inbound/daily_feed.csv","operation":"exists","credential":"SFTP_LEGACY","assert_exists":True}),
                _n("n3","sftp","Download File",520,180,{"remote_path":"/inbound/daily_feed.csv","local_path":"/tmp/daily_feed.csv","operation":"download","credential":"SFTP_LEGACY"}),
                _n("n4","airflow","Load to SQL",750,180,{"dag_id":"ingest_daily_feed","timeout":3600,"retries":2,"credential":"PROD_AIRFLOW","assert_no_failed_tasks":True,"conf":{"file_path":"/tmp/daily_feed.csv"}}),
                _n("n5","sql","Validate Load",980,180,{"query":"SELECT COUNT(*) FROM staging.daily_feed WHERE load_date = CAST(GETDATE() AS DATE)","credential":"PROD_MSSQL","min_row_count":1}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4"),_e("n4","n5")],
        },
    },

    {
        "id":          "tpl-api-health",
        "name":        "API Health Monitor",
        "description": "Periodically check multiple API endpoints for liveness and correct response structure.",
        "category":    "monitoring",
        "tags":        ["http", "if", "slack"],
        "complexity":  "simple",
        "dsl": {
            "name": "API Health Monitor",
            "description": "Check multiple HTTP endpoints and alert on failure",
            "nodes": [
                _n("n1","trigger","Every 15 min",60,180,{"cron":"*/15 * * * *","timezone":"UTC"}),
                _n("n2","http","Check /health",290,180,{"method":"GET","url":"https://api.example.com/health","expected_status":200,"assert_json_field":"status","assert_json_value":"ok"}),
                _n("n3","http","Check /api/v1/ping",520,180,{"method":"GET","url":"https://api.example.com/api/v1/ping","expected_status":200,"assert_body_contains":"pong"}),
                _n("n4","http","Slack: All Healthy",750,180,{"method":"POST","url":"https://hooks.slack.com/services/YOUR/WEBHOOK","expected_status":200,"body":'{"text":"✅ All API endpoints healthy"}'}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4")],
        },
    },

    {
        "id":          "tpl-webhook-process",
        "name":        "Webhook → Process → Store",
        "description": "Receive a webhook event, run a code transform, store a variable, write result to S3.",
        "category":    "integration",
        "tags":        ["webhook", "code", "set_variable", "s3"],
        "complexity":  "medium",
        "dsl": {
            "name": "Webhook → Process → Store",
            "description": "Receive webhook, transform data with code node, persist to S3",
            "nodes": [
                _n("n1","webhook","Receive Event",60,180,{"method":"POST","auth_token":""}),
                _n("n2","code","Transform Payload",290,180,{
                    "code": (
                        "payload = input_data.get('__trigger_data', {})\n"
                        "output['event_type'] = payload.get('type', 'unknown')\n"
                        "output['record_count'] = len(payload.get('records', []))\n"
                        "output['processed_at'] = str(datetime.utcnow())\n"
                        "log(f\"Processed {output['record_count']} records\")"
                    ),
                    "timeout": 10,
                    "language": "python",
                }),
                _n("n3","set_variable","Store Record Count",520,180,{"key":"last_webhook_count","value":"{{$node.Transform Payload.output.record_count}}","scope":"workflow","cast":"number"}),
                _n("n4","s3","Archive to S3",750,180,{"bucket":"events-archive","key":"events/{{$node.Transform Payload.output.event_type}}/{{$execution.id}}.json","operation":"upload","credential":"AWS_PROD","local_path":"/tmp/event.json"}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4")],
        },
    },

    {
        "id":          "tpl-multi-dag",
        "name":        "Multi-DAG Orchestration",
        "description": "Run three Airflow DAGs in sequence with SQL validation between each stage.",
        "category":    "data-engineering",
        "tags":        ["airflow", "sql", "multi-stage"],
        "complexity":  "complex",
        "dsl": {
            "name": "Multi-DAG Orchestration",
            "description": "Three-stage Airflow pipeline with SQL validation gates",
            "nodes": [
                _n("n1","trigger","Weekly Monday",60,180,{"cron":"0 5 * * 1","timezone":"UTC"}),
                _n("n2","airflow","Stage 1: Extract",290,140,{"dag_id":"extract_raw","timeout":3600,"retries":2,"credential":"PROD_AIRFLOW","assert_no_failed_tasks":True}),
                _n("n3","sql","Validate Stage 1",520,140,{"query":"SELECT COUNT(*) FROM staging.raw WHERE batch_date = CAST(GETDATE() AS DATE)","credential":"PROD_MSSQL","min_row_count":1}),
                _n("n4","airflow","Stage 2: Transform",750,180,{"dag_id":"transform_data","timeout":5400,"retries":1,"credential":"PROD_AIRFLOW","assert_no_failed_tasks":True}),
                _n("n5","sql","Validate Stage 2",980,180,{"query":"SELECT COUNT(*) FROM dw.fact_table WHERE load_date = CAST(GETDATE() AS DATE)","credential":"PROD_MSSQL","min_row_count":1}),
                _n("n6","airflow","Stage 3: Publish",1210,220,{"dag_id":"publish_reports","timeout":1800,"retries":1,"credential":"PROD_AIRFLOW","assert_no_failed_tasks":True}),
                _n("n7","http","Notify Completion",1440,220,{"method":"POST","url":"https://hooks.slack.com/services/YOUR/WEBHOOK","expected_status":200,"body":'{"text":"✅ Weekly pipeline complete (3 stages)"}'}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4"),_e("n4","n5"),_e("n5","n6"),_e("n6","n7")],
        },
    },

    {
        "id":          "tpl-azure-export",
        "name":        "SQL Export → Azure Blob",
        "description": "Run a SQL query, export results to Excel, upload to Azure Blob Storage, notify team.",
        "category":    "reporting",
        "tags":        ["sql", "azure", "excel", "slack"],
        "complexity":  "simple",
        "dsl": {
            "name": "SQL Export → Azure Blob",
            "description": "SQL to Excel, upload to Azure Blob, Slack notification",
            "nodes": [
                _n("n1","trigger","Monthly Report",60,180,{"cron":"0 8 1 * *","timezone":"UTC"}),
                _n("n2","sql","Export Monthly Data",290,180,{"query":"SELECT * FROM reporting.monthly_summary WHERE report_month = FORMAT(GETDATE()-1, 'yyyy-MM')","credential":"PROD_MSSQL","export_format":"xlsx","max_sample_rows":1000}),
                _n("n3","azure","Upload Report",520,180,{"container":"reports","blob_name":"monthly/summary_{{$execution.id}}.xlsx","operation":"upload","local_path":"/tmp/sql_export.xlsx","credential":"AZURE_STORAGE"}),
                _n("n4","http","Notify Team",750,180,{"method":"POST","url":"https://hooks.slack.com/services/YOUR/WEBHOOK","expected_status":200,"body":'{"text":"📊 Monthly report uploaded to Azure: monthly/summary_{{$execution.id}}.xlsx"}'}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4")],
        },
    },

    {
        "id":          "tpl-retry-pattern",
        "name":        "Resilient DAG with Retry Logic",
        "description": "Airflow DAG with per-node retries, variable tracking of attempt count, and error workflow routing.",
        "category":    "resilience",
        "tags":        ["airflow", "set_variable", "get_variable", "error-handling"],
        "complexity":  "complex",
        "dsl": {
            "name": "Resilient DAG with Retry Logic",
            "description": "Airflow execution with retry tracking and error workflow routing",
            "settings": {"error_workflow_id": ""},
            "nodes": [
                _n("n1","trigger","On Demand",60,180,{"cron":"manual"}),
                _n("n2","set_variable","Init Attempt Counter",290,180,{"key":"dag_attempt","value":"1","scope":"workflow","cast":"number"}),
                _n("n3","airflow","Run Critical DAG",520,180,{"dag_id":"critical_pipeline","timeout":7200,"retries":3,"credential":"PROD_AIRFLOW","assert_no_failed_tasks":True}),
                _n("n4","set_variable","Record Run ID",750,180,{"key":"last_dag_run_id","value":"{{$node.Run Critical DAG.output.dag_run_id}}","scope":"workflow","cast":"string"}),
                _n("n5","sql","Post-Run Validation",980,180,{"query":"SELECT COUNT(*) FROM results WHERE run_date = CAST(GETDATE() AS DATE)","credential":"PROD_MSSQL","min_row_count":1}),
                _n("n6","http","Notify Success",1210,180,{"method":"POST","url":"https://hooks.slack.com/services/YOUR/WEBHOOK","expected_status":200,"body":'{"text":"✅ Critical pipeline succeeded. Run ID: {{$var.last_dag_run_id}}"}'}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4"),_e("n4","n5"),_e("n5","n6")],
        },
    },

    {
        "id":          "tpl-data-enrichment",
        "name":        "Data Enrichment Pipeline",
        "description": "Fetch data from an external API, transform with a code node, load to SQL, validate enriched count.",
        "category":    "data-engineering",
        "tags":        ["http", "code", "sql", "set_variable"],
        "complexity":  "complex",
        "dsl": {
            "name": "Data Enrichment Pipeline",
            "description": "HTTP fetch → code transform → SQL load → validate",
            "nodes": [
                _n("n1","trigger","Nightly Enrichment",60,180,{"cron":"0 1 * * *","timezone":"UTC"}),
                _n("n2","http","Fetch External Data",290,180,{"method":"GET","url":"https://api.example.com/v1/entities?updated_since=yesterday","expected_status":200,"assert_json_field":"data"}),
                _n("n3","code","Transform Records",520,180,{
                    "code": (
                        "resp = input_data.get('Fetch External Data', {})\n"
                        "records = resp.get('response', {}).get('data', []) if isinstance(resp.get('response'), dict) else []\n"
                        "output['record_count'] = len(records)\n"
                        "output['ids'] = [str(r.get('id','')) for r in records[:100]]\n"
                        "output['summary'] = f'{len(records)} records fetched'\n"
                        "log(output['summary'])"
                    ),
                    "timeout": 15,
                    "language": "python",
                }),
                _n("n4","set_variable","Store Count",750,180,{"key":"enrichment_count","value":"{{$node.Transform Records.output.record_count}}","scope":"workflow","cast":"number"}),
                _n("n5","airflow","Load to DW",980,180,{"dag_id":"load_enriched","timeout":3600,"retries":2,"credential":"PROD_AIRFLOW","assert_no_failed_tasks":True}),
                _n("n6","sql","Validate Enrichment",1210,180,{"query":"SELECT COUNT(*) FROM dw.enriched WHERE enrich_date = CAST(GETDATE() AS DATE)","credential":"PROD_MSSQL","min_row_count":1}),
            ],
            "edges": [_e("n1","n2"),_e("n2","n3"),_e("n3","n4"),_e("n4","n5"),_e("n5","n6")],
        },
    },
]

# Index by id
_BY_ID = {t["id"]: t for t in TEMPLATES}


# ── Schemas ───────────────────────────────────────────────────────────────────

class CloneRequest(BaseModel):
    name: Optional[str] = None    # override name, or use template name


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/")
async def list_templates(
    category: Optional[str] = None,
    complexity: Optional[str] = None,
):
    """List all templates (no auth required — templates are public)."""
    result = TEMPLATES
    if category:
        result = [t for t in result if t["category"] == category]
    if complexity:
        result = [t for t in result if t["complexity"] == complexity]

    # Return metadata only (no full DSL in list)
    return {
        "templates": [
            {
                "id":          t["id"],
                "name":        t["name"],
                "description": t["description"],
                "category":    t["category"],
                "tags":        t["tags"],
                "complexity":  t["complexity"],
                "node_count":  len(t["dsl"]["nodes"]),
                "node_types":  sorted({n["type"] for n in t["dsl"]["nodes"]}),
            }
            for t in result
        ],
        "total": len(result),
        "categories": sorted({t["category"] for t in TEMPLATES}),
    }


@router.get("/{template_id}")
async def get_template(template_id: str):
    """Get full template detail including DSL."""
    tpl = _BY_ID.get(template_id)
    if not tpl:
        raise HTTPException(404, f"Template {template_id!r} not found")
    return tpl


@router.post("/{template_id}/clone", status_code=201)
async def clone_template(
    template_id:  str,
    body:         CloneRequest,
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    """
    Clone a template into the user's workflow library.
    Returns the new workflow_id so the frontend can open it in the editor.
    """
    tpl = _BY_ID.get(template_id)
    if not tpl:
        raise HTTPException(404, f"Template {template_id!r} not found")

    name = body.name or tpl["name"]
    dsl  = dict(tpl["dsl"])
    dsl["name"] = name

    wf = Workflow(
        owner_id=current_user["sub"],
        name=name,
        description=f"From template: {tpl['description']}",
        dsl=dsl,
        execution_mode="manual",
        tags=tpl["tags"],
        generated_by_ai=False,
    )
    db.add(wf)
    db.commit()
    db.refresh(wf)

    ver = WorkflowVersion(
        workflow_id=wf.id,
        version=1,
        dsl=dsl,
        change_note=f"Cloned from template: {tpl['name']}",
        created_by=current_user["sub"],
    )
    db.add(ver)
    db.commit()

    logger.info(f"Template {template_id!r} cloned → workflow {wf.id!r} by {current_user['sub']!r}")

    return {
        "workflow_id":   wf.id,
        "workflow_name": wf.name,
        "template_id":   template_id,
        "template_name": tpl["name"],
        "node_count":    len(dsl["nodes"]),
        "message":       f"Template '{tpl['name']}' cloned. Open in editor to configure credentials.",
    }

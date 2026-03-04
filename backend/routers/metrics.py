"""
FlowForge — Metrics Router (Sprint 3)

  GET /api/metrics/summary          — totals, success rate, avg/p95 duration, 7-day sparkline
  GET /api/metrics/trend            — daily run counts (success/failed) for last N days
  GET /api/metrics/workflows        — per-workflow breakdown sorted by run count
  GET /api/metrics/nodes            — bottleneck analysis: slowest + most-failed nodes
  GET /api/metrics/workflows/{id}   — single-workflow detail with node breakdown
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db, Workflow, WorkflowRun, NodeRun

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Summary ───────────────────────────────────────────────────────────────────

@router.get("/summary")
async def get_summary(
    days:         int     = Query(30, ge=1, le=365),
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    cutoff = datetime.utcnow() - timedelta(days=days)
    wf_ids = db.query(Workflow.id).filter_by(owner_id=current_user["sub"], is_active=True).subquery()
    runs   = db.query(WorkflowRun).filter(
        WorkflowRun.workflow_id.in_(wf_ids),
        WorkflowRun.started_at >= cutoff,
    ).all()

    total     = len(runs)
    succeeded = sum(1 for r in runs if r.status == "success")
    failed    = sum(1 for r in runs if r.status == "failed")
    cancelled = sum(1 for r in runs if r.status == "cancelled")
    durs      = sorted([r.duration_seconds for r in runs if r.duration_seconds is not None])
    avg_dur   = round(sum(durs) / len(durs), 2) if durs else None
    p95_dur   = round(durs[int(len(durs) * 0.95)], 2) if durs else None

    sparkline = []
    for i in range(6, -1, -1):
        day = datetime.utcnow().date() - timedelta(days=i)
        dr  = [r for r in runs if r.started_at and r.started_at.date() == day]
        sparkline.append({
            "date":    day.isoformat(),
            "total":   len(dr),
            "success": sum(1 for r in dr if r.status == "success"),
            "failed":  sum(1 for r in dr if r.status == "failed"),
        })

    return {
        "period_days":      days,
        "total_runs":       total,
        "success_runs":     succeeded,
        "failed_runs":      failed,
        "cancelled_runs":   cancelled,
        "success_rate_pct": round(succeeded / total * 100, 1) if total else None,
        "avg_duration_s":   avg_dur,
        "p95_duration_s":   p95_dur,
        "total_workflows":  db.query(Workflow).filter_by(owner_id=current_user["sub"], is_active=True).count(),
        "manual_runs":      sum(1 for r in runs if r.triggered_by == "manual"),
        "scheduled_runs":   sum(1 for r in runs if r.triggered_by == "cron"),
        "webhook_runs":     sum(1 for r in runs if r.triggered_by == "webhook"),
        "error_runs":       sum(1 for r in runs if r.triggered_by == "error_handler"),
        "sparkline_7d":     sparkline,
    }


# ── Trend ─────────────────────────────────────────────────────────────────────

@router.get("/trend")
async def get_trend(
    days:         int           = Query(30, ge=7, le=90),
    workflow_id:  Optional[str] = None,
    db:           Session       = Depends(get_db),
    current_user: dict          = Depends(get_current_user),
):
    cutoff = datetime.utcnow() - timedelta(days=days)
    wf_ids = db.query(Workflow.id).filter_by(owner_id=current_user["sub"], is_active=True).subquery()
    q = db.query(WorkflowRun).filter(
        WorkflowRun.workflow_id.in_(wf_ids),
        WorkflowRun.started_at >= cutoff,
    )
    if workflow_id:
        q = q.filter(WorkflowRun.workflow_id == workflow_id)
    runs = q.all()

    by_day: dict = {}
    for r in runs:
        if not r.started_at:
            continue
        day = r.started_at.date().isoformat()
        if day not in by_day:
            by_day[day] = {"date": day, "total": 0, "success": 0, "failed": 0, "_d": []}
        by_day[day]["total"] += 1
        if r.status == "success": by_day[day]["success"] += 1
        elif r.status == "failed": by_day[day]["failed"] += 1
        if r.duration_seconds: by_day[day]["_d"].append(r.duration_seconds)

    result = []
    for i in range(days - 1, -1, -1):
        day   = (datetime.utcnow().date() - timedelta(days=i)).isoformat()
        entry = by_day.get(day, {"date": day, "total": 0, "success": 0, "failed": 0, "_d": []})
        durs  = entry.pop("_d", [])
        entry["avg_duration_s"] = round(sum(durs) / len(durs), 2) if durs else None
        result.append(entry)

    return {"days": days, "trend": result, "workflow_id": workflow_id}


# ── Per-workflow breakdown ────────────────────────────────────────────────────

@router.get("/workflows")
async def get_workflow_metrics(
    days:         int     = Query(30, ge=1, le=365),
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    cutoff = datetime.utcnow() - timedelta(days=days)
    wfs    = db.query(Workflow).filter_by(owner_id=current_user["sub"], is_active=True).all()

    result = []
    for wf in wfs:
        runs = db.query(WorkflowRun).filter(
            WorkflowRun.workflow_id == wf.id,
            WorkflowRun.started_at >= cutoff,
        ).order_by(WorkflowRun.started_at.desc()).all()

        if not runs:
            result.append({
                "workflow_id": wf.id, "workflow_name": wf.name,
                "execution_mode": wf.execution_mode,
                "total_runs": 0, "success_runs": 0, "failed_runs": 0,
                "success_rate_pct": None, "avg_duration_s": None,
                "last_run_at": None, "last_run_status": None,
                "trend_7d": [0] * 7,
            })
            continue

        total     = len(runs)
        succeeded = sum(1 for r in runs if r.status == "success")
        failed    = sum(1 for r in runs if r.status == "failed")
        durs      = [r.duration_seconds for r in runs if r.duration_seconds is not None]
        last      = runs[0]

        trend = []
        for i in range(6, -1, -1):
            day = datetime.utcnow().date() - timedelta(days=i)
            trend.append(sum(1 for r in runs if r.started_at and r.started_at.date() == day))

        result.append({
            "workflow_id":      wf.id,
            "workflow_name":    wf.name,
            "execution_mode":   wf.execution_mode,
            "total_runs":       total,
            "success_runs":     succeeded,
            "failed_runs":      failed,
            "success_rate_pct": round(succeeded / total * 100, 1) if total else None,
            "avg_duration_s":   round(sum(durs) / len(durs), 2) if durs else None,
            "last_run_at":      last.started_at.isoformat() if last.started_at else None,
            "last_run_status":  last.status,
            "trend_7d":         trend,
        })

    result.sort(key=lambda r: r["total_runs"], reverse=True)
    return {"period_days": days, "workflows": result, "total": len(result)}


# ── Node bottleneck analysis ──────────────────────────────────────────────────

@router.get("/nodes")
async def get_node_metrics(
    days:         int     = Query(30, ge=1, le=365),
    limit:        int     = Query(20, ge=5, le=100),
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    cutoff  = datetime.utcnow() - timedelta(days=days)
    wf_ids  = db.query(Workflow.id).filter_by(owner_id=current_user["sub"], is_active=True).subquery()
    run_ids = db.query(WorkflowRun.id).filter(
        WorkflowRun.workflow_id.in_(wf_ids),
        WorkflowRun.started_at >= cutoff,
    ).subquery()
    node_runs = db.query(NodeRun).filter(NodeRun.workflow_run_id.in_(run_ids)).all()

    if not node_runs:
        return {"period_days": days, "slowest": [], "most_failed": [], "by_type": []}

    agg: dict = {}
    for nr in node_runs:
        key = f"{nr.node_type}:{nr.node_title}"
        if key not in agg:
            agg[key] = {"node_type": nr.node_type, "node_title": nr.node_title,
                        "executions": 0, "success": 0, "failed": 0, "durations": []}
        agg[key]["executions"] += 1
        if nr.status == "success":  agg[key]["success"] += 1
        elif nr.status == "failed": agg[key]["failed"] += 1
        if nr.duration_seconds is not None:
            agg[key]["durations"].append(nr.duration_seconds)

    rows = []
    for d in agg.values():
        durs = sorted(d["durations"])
        rows.append({
            "node_key":         f"{d['node_type']}:{d['node_title']}",
            "node_type":        d["node_type"],
            "node_title":       d["node_title"],
            "executions":       d["executions"],
            "success":          d["success"],
            "failed":           d["failed"],
            "failure_rate_pct": round(d["failed"] / d["executions"] * 100, 1) if d["executions"] else 0,
            "avg_duration_s":   round(sum(durs) / len(durs), 2) if durs else None,
            "p95_duration_s":   round(durs[int(len(durs) * 0.95)], 2) if durs else None,
            "max_duration_s":   round(max(durs), 2) if durs else None,
        })

    slowest     = sorted(rows, key=lambda r: r["avg_duration_s"] or 0, reverse=True)[:limit]
    most_failed = [r for r in sorted(rows, key=lambda r: r["failed"], reverse=True)[:limit] if r["failed"] > 0]

    by_type: dict = {}
    for nr in node_runs:
        t = nr.node_type
        if t not in by_type:
            by_type[t] = {"node_type": t, "executions": 0, "failed": 0, "total_duration_s": 0.0}
        by_type[t]["executions"] += 1
        if nr.status == "failed": by_type[t]["failed"] += 1
        by_type[t]["total_duration_s"] += nr.duration_seconds or 0

    return {
        "period_days": days,
        "slowest":     slowest,
        "most_failed": most_failed,
        "by_type":     sorted(by_type.values(), key=lambda r: r["executions"], reverse=True),
    }


# ── Single-workflow detail ────────────────────────────────────────────────────

@router.get("/workflows/{workflow_id}")
async def get_workflow_detail_metrics(
    workflow_id:  str,
    days:         int     = Query(30, ge=1, le=365),
    db:           Session = Depends(get_db),
    current_user: dict    = Depends(get_current_user),
):
    wf = db.query(Workflow).filter_by(id=workflow_id, owner_id=current_user["sub"]).first()
    if not wf:
        raise HTTPException(404, "Workflow not found")

    cutoff = datetime.utcnow() - timedelta(days=days)
    runs   = db.query(WorkflowRun).filter(
        WorkflowRun.workflow_id == workflow_id,
        WorkflowRun.started_at >= cutoff,
    ).order_by(WorkflowRun.started_at.desc()).all()

    trend = []
    for i in range(days - 1, -1, -1):
        day  = datetime.utcnow().date() - timedelta(days=i)
        dr   = [r for r in runs if r.started_at and r.started_at.date() == day]
        durs = [r.duration_seconds for r in dr if r.duration_seconds]
        trend.append({
            "date":           day.isoformat(),
            "total":          len(dr),
            "success":        sum(1 for r in dr if r.status == "success"),
            "failed":         sum(1 for r in dr if r.status == "failed"),
            "avg_duration_s": round(sum(durs) / len(durs), 2) if durs else None,
        })

    node_agg: dict = {}
    for run in runs:
        for nr in db.query(NodeRun).filter_by(workflow_run_id=run.id).all():
            k = nr.node_title
            if k not in node_agg:
                node_agg[k] = {"title": k, "type": nr.node_type, "runs": 0, "failed": 0, "durs": []}
            node_agg[k]["runs"] += 1
            if nr.status == "failed":  node_agg[k]["failed"] += 1
            if nr.duration_seconds:    node_agg[k]["durs"].append(nr.duration_seconds)

    node_stats = []
    for d in sorted(node_agg.values(), key=lambda x: x["runs"], reverse=True):
        durs = sorted(d["durs"])
        node_stats.append({
            "title":          d["title"],
            "type":           d["type"],
            "runs":           d["runs"],
            "failed":         d["failed"],
            "failure_pct":    round(d["failed"] / d["runs"] * 100, 1) if d["runs"] else 0,
            "avg_duration_s": round(sum(durs) / len(durs), 2) if durs else None,
            "p95_duration_s": round(durs[int(len(durs) * 0.95)], 2) if durs else None,
        })

    total     = len(runs)
    succeeded = sum(1 for r in runs if r.status == "success")
    durs_all  = [r.duration_seconds for r in runs if r.duration_seconds]

    return {
        "workflow_id":      workflow_id,
        "workflow_name":    wf.name,
        "period_days":      days,
        "total_runs":       total,
        "success_rate_pct": round(succeeded / total * 100, 1) if total else None,
        "avg_duration_s":   round(sum(durs_all) / len(durs_all), 2) if durs_all else None,
        "trend":            trend,
        "node_stats":       node_stats,
        "recent_runs":      [
            {"id": r.id, "status": r.status, "triggered_by": r.triggered_by,
             "started_at": r.started_at.isoformat() if r.started_at else None,
             "duration_s": r.duration_seconds}
            for r in runs[:10]
        ],
    }

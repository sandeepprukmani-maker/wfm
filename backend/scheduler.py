"""
FlowForge — Cron Scheduler (Sprint 1)

Uses APScheduler AsyncIOScheduler to fire workflows on their cron schedule.
Reads Workflow.execution_mode == "scheduled" and Workflow.dsl.nodes[0].props.cron
at startup, then refreshes when workflows are saved/updated.

Usage:
    from scheduler import FlowScheduler
    scheduler = FlowScheduler()
    await scheduler.start()        # call in lifespan startup
    await scheduler.stop()         # call in lifespan shutdown
    await scheduler.refresh()      # call after workflow save/update
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False
    logger.warning("apscheduler not installed — cron scheduling disabled. Run: pip install apscheduler")


class FlowScheduler:
    """
    Manages cron-triggered workflow execution.
    One APScheduler job per scheduled workflow.
    """

    def __init__(self):
        self._scheduler: Optional[Any] = None
        self._job_ids: dict = {}   # workflow_id → APScheduler job id

    async def start(self):
        if not APSCHEDULER_AVAILABLE:
            logger.warning("APScheduler not available — skipping cron scheduler startup")
            return
        self._scheduler = AsyncIOScheduler(timezone="UTC")
        self._scheduler.start()
        logger.info("FlowForge scheduler started")
        await self.refresh()

    async def stop(self):
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            logger.info("FlowForge scheduler stopped")

    async def refresh(self):
        """Re-read all scheduled workflows from DB and sync APScheduler jobs."""
        if not APSCHEDULER_AVAILABLE or not self._scheduler:
            return
        try:
            from database import SessionLocal, Workflow
            db = SessionLocal()
            try:
                workflows = db.query(Workflow).filter_by(
                    execution_mode="scheduled", is_active=True
                ).all()

                current_ids = set()
                for wf in workflows:
                    cron = _extract_cron(wf.dsl)
                    if not cron or cron == "manual":
                        continue
                    current_ids.add(wf.id)
                    self._upsert_job(wf.id, wf.owner_id, cron)

                # Remove jobs for workflows no longer scheduled
                for wf_id in list(self._job_ids.keys()):
                    if wf_id not in current_ids:
                        self._remove_job(wf_id)

                logger.info(f"Scheduler refresh: {len(current_ids)} scheduled workflow(s)")
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Scheduler refresh failed: {e}", exc_info=True)

    def _upsert_job(self, workflow_id: str, owner_id: str, cron: str):
        """Add or replace an APScheduler job for a workflow."""
        job_id = f"wf_{workflow_id}"
        try:
            trigger = CronTrigger.from_crontab(cron, timezone="UTC")
        except Exception as e:
            logger.warning(f"Invalid cron {cron!r} for workflow {workflow_id}: {e}")
            return

        # Remove existing job if present
        if job_id in self._job_ids.values():
            try:
                self._scheduler.remove_job(job_id)
            except Exception:
                pass

        self._scheduler.add_job(
            _fire_workflow,
            trigger=trigger,
            args=[workflow_id, owner_id],
            id=job_id,
            replace_existing=True,
            misfire_grace_time=300,    # fire up to 5 min late
        )
        self._job_ids[workflow_id] = job_id
        logger.info(f"Scheduled workflow {workflow_id!r} with cron={cron!r}")

    def _remove_job(self, workflow_id: str):
        job_id = self._job_ids.pop(workflow_id, None)
        if job_id:
            try:
                self._scheduler.remove_job(job_id)
                logger.info(f"Removed schedule for workflow {workflow_id!r}")
            except Exception:
                pass

    def get_scheduled_workflows(self) -> list:
        """Return list of currently scheduled workflow IDs and their next run times."""
        if not self._scheduler:
            return []
        result = []
        for wf_id, job_id in self._job_ids.items():
            job = self._scheduler.get_job(job_id)
            result.append({
                "workflow_id": wf_id,
                "next_run":    job.next_run_time.isoformat() if job and job.next_run_time else None,
            })
        return result


# ── Job function — runs in the event loop ────────────────────────────────────

async def _fire_workflow(workflow_id: str, owner_id: str):
    """Called by APScheduler to execute a scheduled workflow."""
    from database import SessionLocal, WorkflowRun
    from credential_manager import CredentialManager
    from workflow_engine import WorkflowEngine

    run_id = f"run-{str(uuid.uuid4())[:8]}"
    logger.info(f"Cron firing workflow {workflow_id!r} → run {run_id}")

    db = SessionLocal()
    try:
        run = WorkflowRun(
            id=run_id,
            workflow_id=workflow_id,
            triggered_by="cron",
            trigger_data={"fired_at": datetime.utcnow().isoformat()},
            status="pending",
            started_at=datetime.utcnow(),
        )
        db.add(run)
        db.commit()

        mgr    = CredentialManager(db)
        engine = WorkflowEngine(db_session=db, credential_manager=mgr)
        result = await engine.execute(workflow_id, owner_id, run_id)
        logger.info(f"Cron run {run_id} finished: {result['status']} in {result['duration']:.1f}s")
    except Exception as e:
        logger.error(f"Cron run {run_id} crashed: {e}", exc_info=True)
        try:
            run = db.query(WorkflowRun).filter_by(id=run_id).first()
            if run:
                run.status        = "failed"
                run.error_message = str(e)
                run.completed_at  = datetime.utcnow()
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


def _extract_cron(dsl: dict) -> Optional[str]:
    """Pull cron string from first trigger node in workflow DSL."""
    if not dsl:
        return None
    for node in dsl.get("nodes", []):
        if node.get("type") == "trigger":
            return node.get("props", {}).get("cron")
    return None


# Singleton
_scheduler_instance: Optional[FlowScheduler] = None

def get_scheduler() -> FlowScheduler:
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = FlowScheduler()
    return _scheduler_instance


# Type alias for Optional[Any] used above
from typing import Any

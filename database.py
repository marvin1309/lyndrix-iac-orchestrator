import json
from datetime import datetime
from core.logger import get_logger
from .models import IaCJob
from sqlalchemy import or_

# Import your central database instance
from core.components.database.logic.db_service import db_instance

log = get_logger("IaC:Database")

class JobDatabase:
    def _get_session(self):
        """Safely retrieves a database session if the central engine is connected."""
        if not db_instance.is_connected or not db_instance.SessionLocal:
            log.error("JobDatabase: Cannot get session, Core Database is disconnected.")
            return None
        return db_instance.SessionLocal()

    def create_job(self, pipeline_type: str) -> int:
        """Creates a new job record and returns its ID."""
        session = self._get_session()
        if not session:
            return -1

        try:
            new_job = IaCJob(
                pipeline_type=pipeline_type,
                status="RUNNING",
                progress=0,                      # NEW
                current_step="Pending Start",    # NEW
                logs="[]",
                pending_tasks="[]" 
            )
            session.add(new_job)
            session.commit()
            session.refresh(new_job)
            return new_job.id
        except Exception as e:
            log.error(f"Failed to create job in DB: {e}")
            session.rollback()
            return -1
        finally:
            if session:
                session.close()

    # Changed signature to remove logs_list
    def update_job(self, job_id: int, status: str):
        """Saves the final status to the database."""
        if job_id == -1:
            return

        session = self._get_session()
        if not session:
            return

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job:
                job.status = status
                if status in ["SUCCESS", "FAILED", "ERROR", "ABORTED"]: # Added ABORTED for the Kill Switch
                    job.end_time = datetime.now()
                    job.progress = 100 if status == "SUCCESS" else job.progress # Snap to 100% on success
                session.commit()
        except Exception as e:
            log.error(f"Failed to update job {job_id} in DB: {e}")
            session.rollback()
        finally:
            if session:
                session.close()

    def get_recent_jobs(self, limit: int = 20) -> list:
        """Fetches metadata for the UI table."""
        session = self._get_session()
        if not session:
            return []

        try:
            jobs = session.query(
                IaCJob.id,
                IaCJob.pipeline_type,
                IaCJob.start_time,
                IaCJob.end_time,
                IaCJob.status,
                IaCJob.progress
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [
                {
                    "id": job.id,
                    "pipeline_type": job.pipeline_type,
                    "status": job.status,
                    "progress": job.progress or 0,
                    "start_time": job.start_time.strftime("%Y-%m-%d %H:%M:%S") if job.start_time else "N/A",
                    "end_time": job.end_time.strftime("%H:%M:%S") if job.end_time else "Running"
                }
                for job in jobs
            ]
        finally:
            if session:
                session.close()

    def get_job_logs(self, job_id: int) -> list:
        """Fetches the raw log array for the popup window."""
        session = self._get_session()
        if not session:
            return ["Database connection lost."]

        try:
            job = session.query(IaCJob.logs).filter(IaCJob.id == job_id).first()
            if job and job.logs:
                return json.loads(job.logs)
            return ["No logs found."]
        finally:
            if session:
                session.close()

    def update_pending_tasks(self, job_id: int, pending_list: list):
        """Updates the queue of services yet to be deployed."""
        session = self._get_session()
        if not session:
            return

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job:
                job.pending_tasks = json.dumps(pending_list)
                session.commit()
        except Exception as e:
            log.error(f"Failed to update pending tasks for {job_id}: {e}")
            session.rollback()
        finally:
            if session:
                session.close()

    def get_pending_tasks(self, job_id: int) -> list:
        """Retrieves the surviving queue list for a specific job."""
        session = self._get_session()
        if not session:
            return []

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job and job.pending_tasks:
                return json.loads(job.pending_tasks)
            return []
        finally:
            if session:
                session.close()

    # ==========================================
    # THE MISSING METHOD THAT CAUSED THE CRASH
    # ==========================================
    def get_jobs_by_status(self, status: str) -> list:
        """Finds all jobs currently in a specific state (e.g., RUNNING)."""
        session = self._get_session()
        if not session:
            return []

        try:
            return session.query(IaCJob).filter(IaCJob.status == status).all()
        except Exception as e:
            log.error(f"Failed to fetch jobs by status '{status}': {e}")
            return []
        finally:
            if session:
                session.close()
                
                
    def update_progress(self, job_id: int, progress: int = None, current_step: str = None):
        """Live updates the progress bar and current action text."""
        session = self._get_session()
        if not session or job_id == -1:
            return

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job:
                if progress is not None: 
                    job.progress = progress
                if current_step:
                    job.current_step = str(current_step)[:250] 
                session.commit()
        except Exception as e:
            log.error(f"Failed to update progress for job {job_id}: {e}")
            session.rollback()
        finally:
            if session:
                session.close()
                
    def get_service_history(self, service_name: str, limit: int = 15) -> list:
        """Fetches recent jobs involving a specific service with strict filtering."""
        session = self._get_session()
        if not session: return []
        try:
            # Search for the service name in the type string OR inside the pending_tasks JSON blob
            search = f"%{service_name}%"
            jobs = session.query(IaCJob).filter(
                or_(IaCJob.pipeline_type.like(search), IaCJob.pending_tasks.like(search))
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [{
                "id": j.id,
                "pipeline_type": j.pipeline_type,
                "status": j.status,
                "progress": j.progress or 0,
                "start_time": j.start_time.strftime("%Y-%m-%d %H:%M:%S") if j.start_time else "N/A"
            } for j in jobs]
        finally:
            if session: session.close()
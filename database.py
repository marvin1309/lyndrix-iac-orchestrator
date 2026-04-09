import json
from datetime import datetime
from core.logger import get_logger
from .models import IaCJob

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
                logs="[]"
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

    def update_job(self, job_id: int, status: str, logs_list: list):
        """Saves the final status and full log array to the database."""
        if job_id == -1:
            return

        session = self._get_session()
        if not session:
            return

        try:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job:
                job.status = status
                job.end_time = datetime.now()
                job.logs = json.dumps(logs_list)
                session.commit()
        except Exception as e:
            log.error(f"Failed to update job {job_id} in DB: {e}")
            session.rollback()
        finally:
            if session:
                session.close()

    def get_recent_jobs(self, limit: int = 20) -> list:
        """Fetches metadata for the UI table (excluding the heavy logs text)."""
        session = self._get_session()
        if not session:
            return []

        try:
            jobs = session.query(
                IaCJob.id,
                IaCJob.pipeline_type,
                IaCJob.start_time,
                IaCJob.end_time,
                IaCJob.status
            ).order_by(IaCJob.id.desc()).limit(limit).all()

            return [
                {
                    "id": job.id,
                    "pipeline_type": job.pipeline_type,
                    "status": job.status,
                    "start_time": job.start_time.strftime("%Y-%m-%d %H:%M:%S") if job.start_time else "N/A",
                    "end_time": job.end_time.strftime("%Y-%m-%d %H:%M:%S") if job.end_time else "Running"
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
        if not self.SessionLocal: return
        with self.SessionLocal() as session:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job:
                import json
                job.pending_tasks = json.dumps(pending_list)
                session.commit()

    def get_pending_tasks(self, job_id: int) -> list:
        if not self.SessionLocal: return []
        with self.SessionLocal() as session:
            job = session.query(IaCJob).filter(IaCJob.id == job_id).first()
            if job and job.pending_tasks:
                import json
                try:
                    return json.loads(job.pending_tasks)
                except Exception:
                    pass
            return []
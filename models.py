from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mysql import LONGTEXT 

Base = declarative_base()

class IaCJob(Base):
    __tablename__ = "iac_orchestrator_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    pipeline_type = Column(String(50), nullable=False)
    status = Column(String(20), default="RUNNING")
    start_time = Column(DateTime(timezone=True), server_default=func.now())
    end_time = Column(DateTime(timezone=True), nullable=True)
    
    logs = Column(LONGTEXT, default="[]")
    pending_tasks = Column(LONGTEXT, default="[]") # <--- ADD THIS LINE
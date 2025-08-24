from sqlalchemy import Column, String, DateTime, Integer, Text
from sqlalchemy.sql import func
from app.config.database import Base
from enum import Enum

class ReportStatus(str, Enum):
    RUNNING = "Running"
    COMPLETE = "Complete"
    FAILED = "Failed"

class Report(Base):
    __tablename__ = "reports"

    id = Column(Integer, primary_key=True, index=True)
    report_id = Column(String, nullable=False, unique=True, index=True)
    status = Column(String, nullable=False, default=ReportStatus.RUNNING)
    file_path = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<Report(report_id={self.report_id}, status={self.status})>"

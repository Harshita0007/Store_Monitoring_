from sqlalchemy import Column, String, Time, Integer, DateTime
from sqlalchemy.sql import func
from app.config.database import Base
from datetime import datetime, time

class BusinessHours(Base):
    __tablename__ = "business_hours"

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, nullable=False, index=True)
    day_of_week = Column(Integer, nullable=False)  
    start_time_local = Column(Time, nullable=False)
    end_time_local = Column(Time, nullable=False)
    created_at = Column(DateTime, default=func.now())

    def __repr__(self):
        return f"<BusinessHours(store_id={self.store_id}, day={self.day_of_week}, start={self.start_time_local}, end={self.end_time_local})>"

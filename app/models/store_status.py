from sqlalchemy import Column, String, DateTime, Integer
from sqlalchemy.sql import func
from app.config.database import Base
from datetime import datetime

class StoreStatus(Base):
    __tablename__ = "store_status"

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, nullable=False, index=True)
    timestamp_utc = Column(DateTime, nullable=False, index=True)
    status = Column(String, nullable=False)  # 'active' or 'inactive'
    created_at = Column(DateTime, default=func.now())

    def __repr__(self):
        return f"<StoreStatus(store_id={self.store_id}, timestamp={self.timestamp_utc}, status={self.status})>"

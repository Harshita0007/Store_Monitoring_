from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.sql import func
from app.config.database import Base

class StoreTimezone(Base):
    __tablename__ = "store_timezone"

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, nullable=False, unique=True, index=True)
    timezone_str = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now())

    def __repr__(self):
        return f"<StoreTimezone(store_id={self.store_id}, timezone={self.timezone_str})>"

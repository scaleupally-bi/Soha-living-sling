from datetime import datetime
from sqlalchemy import Column , DateTime
from sqlalchemy.orm import declarative_base
Base = declarative_base()
"""
Table Datamodel which will be migrated to the destination SQL Server
Description: This script contains the schema for tables.
"""
class TimeStampedModel(Base):
    __tablename__ = "timeStamped"
    __abstract__ = True
    created_at = Column(DateTime, default=datetime.utcnow())
    updated_at = Column(DateTime, onupdate=datetime.utcnow())
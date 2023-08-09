from sqlalchemy import create_engine, Column, Integer, String, DateTime, Time
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func


Base = declarative_base()

class Timezone(Base):
    __tablename__ = 'time_zones'

    store_id = Column(String, primary_key=True)
    timezone_str = Column(String, default="America/Chicago")

class BusinessHours(Base):
    __tablename__ = 'business_hours'

    store_id = Column(Integer, primary_key=True)
    day = Column(Integer, primary_key=True)
    start_time_local = Column(String, primary_key=True)
    end_time_local = Column(String, primary_key=True)

class Activity(Base):
    __tablename__ = 'activity'
    store_id = Column(Integer, primary_key=True)
    status = Column(String, nullable=False)
    timestamp_utc = Column(String,primary_key=True)

# class Report(Base):
#     __tablename__ = 'reports'

#     id = Column(Integer, primary_key=True)
#     store_id = Column(String, nullable=False)
#     uptime_last_hour = Column(Integer)
#     uptime_last_day = Column(Integer)
#     uptime_last_week = Column(Integer)
#     downtime_last_hour = Column(Integer)
#     downtime_last_day = Column(Integer)
#     downtime_last_week = Column(Integer)

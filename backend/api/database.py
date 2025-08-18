# api/database.py
from sqlalchemy import create_engine, event, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
import os
import time
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

# Use StaticPool for better connection reuse
engine = create_engine(
    DATABASE_URL,
    poolclass=pool.QueuePool,
    pool_size=5,  # Keep this small for basic plan
    max_overflow=10,  # Total = 15 connections max
    pool_timeout=10,  # Fail fast if no connections available
    pool_pre_ping=True,  # Test connections before using
    pool_recycle=300,  # Recycle every 5 minutes
    connect_args={
        "connect_timeout": 5,
        "options": "-c statement_timeout=10000",  # 10 second timeout
        "keepalives": 1,
        "keepalives_idle": 10,
        "keepalives_interval": 2,
        "keepalives_count": 3,
    }
)

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False
)

Base = declarative_base()

# Connection limiter
class ConnectionLimiter:
    def __init__(self, max_wait=5):
        self.max_wait = max_wait
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time
        if elapsed > self.max_wait:
            logger.warning(f"Database operation took {elapsed:.2f}s")

def get_db():
    """Get database session with timeout protection"""
    with ConnectionLimiter():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

# Quick read-only sessions for simple queries
@contextmanager
def get_quick_db():
    """Get a quick database session for read-only operations"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.rollback()  # Always rollback read-only
        db.close()
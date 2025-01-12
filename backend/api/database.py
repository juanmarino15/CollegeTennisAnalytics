from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
import os

# Get DATABASE_URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    # Fallback for local development
    DATABASE_URL = "postgresql://juanmarino@localhost:5432/college_tennis_db"
    # DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
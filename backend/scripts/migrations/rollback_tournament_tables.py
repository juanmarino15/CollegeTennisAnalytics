# migrations/rollback_tournament_tables.py
"""
Rollback script to remove tournament tables if needed
"""

import os
import logging
from sqlalchemy import create_engine, text

def rollback_tournament_tables():
    """Remove tournament tables (use with caution!)"""
    try:
        database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        engine = create_engine(database_url)
        
        logging.warning("Rolling back tournament tables...")
        
        with engine.connect() as conn:
            # Drop tables in reverse order of dependencies
            rollback_sql = [
                "DROP TABLE IF EXISTS tournament_events CASCADE;",
                "DROP TABLE IF EXISTS tournaments CASCADE;"
            ]
            
            for sql in rollback_sql:
                conn.execute(text(sql))
                conn.commit()
        
        logging.info("Tournament tables rolled back successfully")
        
    except Exception as e:
        logging.error(f"Error during rollback: {str(e)}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    rollback_tournament_tables()
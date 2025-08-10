# backend/migrations/add_tournament_tables.py
"""
Database migration to add tournament tables and populate with existing data
"""

import os
import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta

# Add the backend directory to Python path
backend_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_root))

# Now import from your models
from models.models import Base, Match, Team
from collector.tournament_collector import TournamentCollector

def setup_logging():
    """Set up logging for migration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('tournament_migration.log'),
            logging.StreamHandler()
        ]
    )

def create_tournament_tables(database_url: str):
    """Create the tournament tables using raw SQL since we haven't created the models yet"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Creating tournament tables...")
        
        with engine.connect() as conn:
            # Create tournaments table
            tournaments_sql = text("""
                CREATE TABLE IF NOT EXISTS tournaments (
                    tournament_id VARCHAR PRIMARY KEY,
                    identification_code VARCHAR,
                    name VARCHAR,
                    image VARCHAR,
                    is_cancelled BOOLEAN DEFAULT FALSE,
                    cancelled_at TIMESTAMP,
                    start_date_time TIMESTAMP,
                    end_date_time TIMESTAMP,
                    time_zone VARCHAR,
                    time_zone_start_date_time TIMESTAMP,
                    time_zone_end_date_time TIMESTAMP,
                    url VARCHAR,
                    root_provider_id VARCHAR,
                                   
                    -- Location information
                    location_id VARCHAR,
                    location_name VARCHAR,
                    primary_location_town VARCHAR,
                    primary_location_county VARCHAR,
                    primary_location_address1 VARCHAR,
                    primary_location_address2 VARCHAR,
                    primary_location_address3 VARCHAR,
                    primary_location_postcode VARCHAR,
                    geo_latitude FLOAT DEFAULT 0,
                    geo_longitude FLOAT DEFAULT 0,
                    
                    -- Level information
                    level_id VARCHAR,
                    level_name VARCHAR,
                    level_branding VARCHAR,
                    
                    -- Organization information
                    organization_id VARCHAR,
                    organization_name VARCHAR,
                    organization_conference VARCHAR,
                    organization_division VARCHAR,
                    organization_url_segment VARCHAR,
                    organization_parent_region_id VARCHAR,
                    organization_region_id VARCHAR,
                    
                    -- Registration information
                    entries_open_date_time TIMESTAMP,
                    entries_close_date_time TIMESTAMP,
                    seconds_until_entries_close FLOAT,
                    seconds_until_entries_open FLOAT,
                    registration_time_zone VARCHAR,
                    
                    -- Classification
                    is_dual_match BOOLEAN DEFAULT FALSE,
                    tournament_type VARCHAR DEFAULT 'TOURNAMENT',
                                   
                     -- Additional tournament info
                    gender VARCHAR,
                    event_types VARCHAR,
                    level_category VARCHAR,
                    registration_status VARCHAR,
                    
                    -- Timestamps
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(tournaments_sql)
            conn.commit()
            
            # Create simplified tournament_events table
            tournament_events_sql = text("""
                CREATE TABLE IF NOT EXISTS tournament_events (
                    event_id VARCHAR PRIMARY KEY,
                    tournament_id VARCHAR REFERENCES tournaments(tournament_id) ON DELETE CASCADE,
                    gender VARCHAR,
                    event_type VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(tournament_events_sql)
            conn.commit()
        
        logging.info("Tournament tables created successfully")
        return engine
        
    except Exception as e:
        logging.error(f"Error creating tournament tables: {str(e)}")
        raise

def add_indexes(database_url: str):
    """Add indexes for better query performance"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Adding database indexes...")
        
        with engine.connect() as conn:
            # Add indexes for common queries
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_tournaments_start_date ON tournaments(start_date_time);",
                "CREATE INDEX IF NOT EXISTS idx_tournaments_is_dual_match ON tournaments(is_dual_match);",
                "CREATE INDEX IF NOT EXISTS idx_tournaments_tournament_type ON tournaments(tournament_type);",
                "CREATE INDEX IF NOT EXISTS idx_tournaments_organization_id ON tournaments(organization_id);",
                
                # Tournament events indexes
                "CREATE INDEX IF NOT EXISTS idx_tournament_events_tournament_id ON tournament_events(tournament_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_events_gender_type ON tournament_events(gender, event_type);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_events_gender ON tournament_events(gender);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_events_event_type ON tournament_events(event_type);"
            ]
            
            for index_sql in indexes:
                try:
                    conn.execute(text(index_sql))
                    conn.commit()
                    logging.info(f"Created index: {index_sql}")
                except Exception as e:
                    logging.warning(f"Index may already exist or failed to create: {index_sql} - {str(e)}")
                    continue
        
        logging.info("Database indexes added successfully")
        
    except Exception as e:
        logging.error(f"Error adding indexes: {str(e)}")
        raise

def run_initial_tournament_collection(database_url: str):
    """Run initial collection of tournament data from API"""
    try:
        logging.info("Starting initial tournament data collection...")
        
        collector = TournamentCollector(database_url)
        
        # Collect tournaments from today onwards for the next 6 months
        today = datetime.now()
        six_months_later = today + timedelta(days=400)
        
        # start_date = today.strftime('%Y-%m-%dT00:00:00.000Z')
        start_date = "2024-07-01T00:00:00.000Z"  # Changed from datetime.now()

        end_date = six_months_later.strftime('%Y-%m-%dT23:59:59.000Z')
        
        collector.collect_tournaments_range(
            start_date=start_date,
            end_date=end_date,
            batch_size=100
        )
        
        logging.info("Initial tournament collection completed")
        
    except Exception as e:
        logging.error(f"Error in initial tournament collection: {str(e)}")
        # Don't raise here as this is optional - the tables and dual matches are more important
        logging.warning("Continuing with migration despite tournament collection error")

def run_migration():
    """Run the complete migration"""
    try:
        # Get database URL from environment variable
        database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        logging.info("Starting tournament tables migration...")
        
        # Step 1: Create tournament tables
        engine = create_tournament_tables(database_url)
        
        # Step 3: Add database indexes
        add_indexes(database_url)
        
        # Step 4: Run initial tournament collection (optional)
        try:
            run_initial_tournament_collection(database_url)
        except Exception as e:
            logging.warning(f"Tournament collection failed, but migration can continue: {str(e)}")
        
        logging.info("Tournament tables migration completed successfully!")
        
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    setup_logging()
    run_migration()
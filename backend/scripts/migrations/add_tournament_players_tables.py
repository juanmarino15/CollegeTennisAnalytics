# migrations/add_tournament_players_table.py
"""
Database migration to add tournament players/registrations table
Run this from the backend directory: python migrations/add_tournament_players_table.py
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
from models.models import Base

def setup_logging():
    """Set up logging for migration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('tournament_players_migration.log'),
            logging.StreamHandler()
        ]
    )

def create_tournament_players_table(database_url: str):
    """Create the tournament_players table"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Creating tournament_players table...")
        
        with engine.connect() as conn:
            # Create tournament_players table
            players_sql = text("""
                CREATE TABLE IF NOT EXISTS tournament_players (
                    id VARCHAR PRIMARY KEY,
                    tournament_id VARCHAR REFERENCES tournaments(tournament_id),
                    
                    -- Player information
                    player_id VARCHAR NOT NULL,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    player_name VARCHAR,
                    gender VARCHAR,
                    city VARCHAR,
                    state VARCHAR,
                    
                    -- Registration details
                    registration_date TIMESTAMP,
                    selection_status VARCHAR,
                    selection_index INTEGER,
                    
                    -- Event participation
                    events_participating VARCHAR,
                    singles_event_id VARCHAR,
                    doubles_event_id VARCHAR,
                    
                    -- Player 2 information (for doubles - current player is player 1)
                    player2_id VARCHAR,
                    player2_first_name VARCHAR,
                    player2_last_name VARCHAR,
                    
                    -- Timestamps
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.execute(players_sql)
            conn.commit()
            logging.info("Tournament players table created successfully")
        
        return engine
        
    except Exception as e:
        logging.error(f"Error creating tournament registrations table: {str(e)}")
        raise

def add_indexes(database_url: str):
    """Add database indexes for better query performance"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Adding database indexes...")
        
        with engine.connect() as conn:
            indexes = [
                # Tournament players indexes
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_tournament_id ON tournament_players(tournament_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_player_id ON tournament_players(player_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_gender ON tournament_players(gender);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_state ON tournament_players(state);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_events_participating ON tournament_players(events_participating);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_player2_id ON tournament_players(player2_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_player_name ON tournament_players(player_name);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_created_at ON tournament_players(created_at);",
                
                # Composite indexes for common queries
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_tournament_gender ON tournament_players(tournament_id, gender);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_tournament_events ON tournament_players(tournament_id, events_participating);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_players_player_tournament ON tournament_players(player_id, tournament_id);",
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

def run_initial_data_collection(database_url: str):
    """Run initial collection of tournament players data from API (limited scope)"""
    try:
        logging.info("Starting limited initial tournament players data collection...")
        
        from collector.tournament_players_collector import TournamentPlayersCollector
        
        collector = TournamentPlayersCollector(database_url)
        
        # Only collect for a specific test tournament or recent tournaments
        # This prevents making hundreds of API calls during migration
        
        # Option 1: Test with the provided tournament ID
        test_tournament_id = "90B70C67-DA64-4F5F-A896-6626779327B7"  # From your example
        try:
            logging.info(f"Testing player collection with tournament: {test_tournament_id}")
            collector.collect_players_for_tournament(test_tournament_id)
            logging.info("Test tournament player collection completed successfully")
        except Exception as e:
            logging.warning(f"Test tournament collection failed: {str(e)}")
        
        # Option 2: Collect for tournaments from the last 360 days
        if not collector.Session:
            return
            
        session = collector.Session()
        try:
            from models.models import Tournament
            
            # Get tournaments from the last 360 days that haven't been cancelled
            cutoff_date = datetime.now() - timedelta(days=360)
            
            tournaments_in_range = session.query(Tournament.tournament_id, Tournament.name, Tournament.start_date_time).filter(
                Tournament.is_cancelled == False,
                Tournament.start_date_time >= cutoff_date
            ).order_by(
                Tournament.start_date_time.desc()
            ).all()
            
            logging.info(f"Found {len(tournaments_in_range)} tournaments from last 360 days for player collection")
            
            success_count = 0
            error_count = 0
            
            for tournament_id, tournament_name, start_date in tournaments_in_range:
                try:
                    logging.info(f"Collecting players for: {tournament_name} ({tournament_id}) - {start_date}")
                    collector.collect_players_for_tournament(tournament_id)
                    success_count += 1
                    # Add a small delay to be respectful to the API
                    import time
                    time.sleep(1)  # 1 second delay between requests
                except Exception as e:
                    logging.warning(f"Failed to collect players for {tournament_id}: {str(e)}")
                    error_count += 1
                    continue
            
            logging.info(f"Player collection completed: {success_count} successful, {error_count} failed")
                    
        except Exception as e:
            logging.warning(f"Error querying tournaments from last 360 days: {str(e)}")
        finally:
            session.close()
        
        logging.info("Limited initial tournament players collection completed")
        
    except Exception as e:
        logging.error(f"Error in initial tournament players collection: {str(e)}")
        # Don't raise here as this is optional - the table creation is more important
        logging.warning("Continuing with migration despite collection error")

def run_migration():
    """Run the complete migration"""
    try:
        # Get database URL from environment variable or use default
        database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        logging.info("Starting tournament players table migration...")
        
        # Step 1: Create tournament registrations table
        engine = create_tournament_players_table(database_url)
        
        # Step 2: Add database indexes
        add_indexes(database_url)
        
        # Step 3: Run initial data collection (optional)
        try:
            run_initial_data_collection(database_url)
        except Exception as e:
            logging.warning(f"Tournament players collection failed, but migration can continue: {str(e)}")
        
        logging.info("Tournament players table migration completed successfully!")
        
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    setup_logging()
    run_migration()
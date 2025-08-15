# migrations/add_tournament_draw_visualization_tables.py
"""
Database migration to add tournament draw visualization tables (hybrid approach)
Run this from the migrations directory: python add_tournament_draw_visualization_tables.py
"""

import os
import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta

# Simple and robust path resolution when running from migrations folder
current_file = Path(__file__).resolve()
migrations_dir = current_file.parent  # migrations/
scripts_dir = migrations_dir.parent   # scripts/
backend_dir = scripts_dir.parent      # backend/

print(f"Script: {current_file}")
print(f"Migrations: {migrations_dir}")
print(f"Scripts: {scripts_dir}")
print(f"Backend: {backend_dir}")
print(f"Models should be at: {backend_dir / 'models' / 'models.py'}")

# Add backend directory to Python path
sys.path.insert(0, str(backend_dir))

# Verify models exist and import
models_file = backend_dir / 'models' / 'models.py'
if not models_file.exists():
    print(f"❌ ERROR: models.py not found at {models_file}")
    print(f"Contents of backend directory: {list(backend_dir.iterdir())}")
    sys.exit(1)

try:
    from models.models import Base, Tournament, TournamentEvent
    print("✅ Successfully imported models")
except ImportError as e:
    print(f"❌ Failed to import models: {e}")
    sys.exit(1)

def setup_logging():
    """Set up logging for migration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('tournament_draw_visualization_migration.log'),
            logging.StreamHandler()
        ]
    )

def create_tournament_draw_visualization_tables(database_url: str):
    """Create the tournament draw visualization tables"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Creating tournament draw visualization tables...")
        
        with engine.connect() as conn:
            # Create tournament_draws table
            draws_sql = text("""
                CREATE TABLE IF NOT EXISTS tournament_draws (
                    draw_id VARCHAR PRIMARY KEY,
                    tournament_id VARCHAR REFERENCES tournaments(tournament_id),
                    event_id VARCHAR,
                    
                    -- Draw information for visualization
                    draw_name VARCHAR,
                    draw_type VARCHAR,
                    draw_size INTEGER,
                    event_type VARCHAR,
                    gender VARCHAR,
                    
                    -- Draw status
                    draw_completed BOOLEAN DEFAULT FALSE,
                    draw_active BOOLEAN DEFAULT FALSE,
                    
                    -- Timestamps from API
                    updated_at_api TIMESTAMP,
                    
                    -- Timestamps
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.execute(draws_sql)
            conn.commit()
            
            # Create tournament_bracket_positions table
            positions_sql = text("""
                CREATE TABLE IF NOT EXISTS tournament_bracket_positions (
                    id SERIAL PRIMARY KEY,
                    draw_id VARCHAR REFERENCES tournament_draws(draw_id),
                    
                    -- Position in the bracket
                    draw_position INTEGER,
                    round_number INTEGER,
                    
                    -- Player/Team information (all IDs stored in lowercase)
                    participant_id VARCHAR,
                    participant_name VARCHAR,
                    participant_type VARCHAR,
                    
                    -- Team affiliation
                    team_name VARCHAR,
                    
                    -- Seed information
                    seed_number INTEGER,
                    
                    -- Match linking to existing system
                    player_match_id INTEGER REFERENCES player_matches(id),
                    
                    -- Position status
                    is_bye BOOLEAN DEFAULT FALSE,
                    is_winner BOOLEAN DEFAULT FALSE,
                    advanced_to_position INTEGER,
                    
                    -- Timestamps
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.execute(positions_sql)
            conn.commit()
            
            logging.info("Tournament draw visualization tables created successfully")
        
        return engine
        
    except Exception as e:
        logging.error(f"Error creating tournament draw visualization tables: {str(e)}")
        raise

def normalize_existing_ids(database_url: str):
    """Normalize existing IDs to lowercase in relevant tables"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Normalizing existing IDs to lowercase...")
        
        with engine.connect() as conn:
            # Update tournament_draws table if it has data
            normalize_draws_sql = text("""
                UPDATE tournament_draws SET
                    draw_id = LOWER(draw_id),
                    tournament_id = LOWER(tournament_id),
                    event_id = LOWER(event_id)
                WHERE draw_id IS NOT NULL;
            """)
            
            # Update tournament_bracket_positions table if it has data
            normalize_positions_sql = text("""
                UPDATE tournament_bracket_positions SET
                    draw_id = LOWER(draw_id),
                    participant_id = LOWER(participant_id)
                WHERE participant_id IS NOT NULL;
            """)
            
            try:
                conn.execute(normalize_draws_sql)
                conn.commit()
                logging.info("Normalized tournament_draws IDs to lowercase")
            except Exception as e:
                logging.warning(f"Could not normalize tournament_draws (table may be empty): {str(e)}")
            
            try:
                conn.execute(normalize_positions_sql)
                conn.commit()
                logging.info("Normalized tournament_bracket_positions IDs to lowercase")
            except Exception as e:
                logging.warning(f"Could not normalize tournament_bracket_positions (table may be empty): {str(e)}")
        
        logging.info("ID normalization completed")
        
    except Exception as e:
        logging.error(f"Error normalizing IDs: {str(e)}")
        logging.warning("Continuing migration despite ID normalization issues")

def add_indexes(database_url: str):
    """Add database indexes for better query performance"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Adding database indexes...")
        
        with engine.connect() as conn:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_tournament_id ON tournament_draws(tournament_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_event_id ON tournament_draws(event_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_event_type ON tournament_draws(event_type);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_gender ON tournament_draws(gender);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_completed ON tournament_draws(draw_completed);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_active ON tournament_draws(draw_active);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_draw_id ON tournament_bracket_positions(draw_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_participant_id ON tournament_bracket_positions(participant_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_draw_position ON tournament_bracket_positions(draw_position);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_round_number ON tournament_bracket_positions(round_number);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_team_name ON tournament_bracket_positions(team_name);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_player_match_id ON tournament_bracket_positions(player_match_id);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_seed_number ON tournament_bracket_positions(seed_number);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_tournament_event ON tournament_draws(tournament_id, event_type);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_tournament_gender ON tournament_draws(tournament_id, gender);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_draw_round ON tournament_bracket_positions(draw_id, round_number);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_draw_position_combo ON tournament_bracket_positions(draw_id, draw_position);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_draws_updated_at_api ON tournament_draws(updated_at_api);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_is_bye ON tournament_bracket_positions(is_bye);",
                "CREATE INDEX IF NOT EXISTS idx_tournament_bracket_positions_is_winner ON tournament_bracket_positions(is_winner);",
            ]
            
            for index_sql in indexes:
                try:
                    conn.execute(text(index_sql))
                    conn.commit()
                    logging.info(f"Created index: {index_sql.split()[-1]}")
                except Exception as e:
                    logging.warning(f"Index may already exist or failed to create: {str(e)}")
                    continue
        
        logging.info("Database indexes added successfully")
        
    except Exception as e:
        logging.error(f"Error adding indexes: {str(e)}")
        raise

def run_initial_data_collection(database_url: str):
    """Run initial collection of tournament draw visualization data from API for ALL tournaments"""
    try:
        logging.info("Starting comprehensive tournament draw visualization data collection for ALL tournaments...")
        
        from collector.tournament_draw_visualization_collector import TournamentDrawVisualizationCollector
        
        collector = TournamentDrawVisualizationCollector(database_url)
        
        if not collector.Session:
            logging.warning("Database session not available, skipping data collection")
            return
            
        session = collector.Session()
        try:
            # Get ALL tournaments that have events (no date filter)
            tournaments_with_events = session.query(
                Tournament.tournament_id, Tournament.name, Tournament.start_date_time
            ).join(
                TournamentEvent, Tournament.tournament_id == TournamentEvent.tournament_id
            ).filter(
                Tournament.is_cancelled == False
            ).order_by(Tournament.start_date_time.desc()).distinct().all()
            
            logging.info(f"Found {len(tournaments_with_events)} total tournaments with events for draw visualization collection")
            
            if not tournaments_with_events:
                logging.info("No tournaments found, skipping data collection")
                return
            
            success_count = 0
            error_count = 0
            
            # Process tournaments in batches to avoid overwhelming the API
            batch_size = 50
            total_tournaments = len(tournaments_with_events)
            
            for i in range(0, total_tournaments, batch_size):
                batch = tournaments_with_events[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_tournaments + batch_size - 1) // batch_size
                
                logging.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tournaments)")
                
                for tournament_id, tournament_name, start_date in batch:
                    try:
                        # Normalize tournament_id to lowercase for consistency
                        tournament_id_normalized = tournament_id.lower() if tournament_id else ""
                        
                        logging.info(f"Collecting draw visualization for: {tournament_name} ({tournament_id_normalized}) - {start_date}")
                        collector.collect_draws_for_tournament_events(tournament_id_normalized)
                        success_count += 1
                        
                        # Add small delay between tournaments to be respectful to the API
                        import time
                        time.sleep(1)
                        
                    except Exception as e:
                        logging.warning(f"Failed to collect draw visualization for {tournament_id}: {str(e)}")
                        error_count += 1
                        continue
                
                # Longer delay between batches
                if i + batch_size < total_tournaments:
                    logging.info(f"Completed batch {batch_num}/{total_batches}. Waiting 10 seconds before next batch...")
                    time.sleep(10)
            
            logging.info(f"Comprehensive draw visualization collection completed: {success_count} successful, {error_count} failed out of {total_tournaments} total tournaments")
                    
        except Exception as e:
            logging.warning(f"Error querying tournaments with events: {str(e)}")
        finally:
            session.close()
        
        logging.info("Comprehensive tournament draw visualization collection completed")
        
    except Exception as e:
        logging.error(f"Error in comprehensive tournament draw visualization collection: {str(e)}")
        logging.warning("Continuing with migration despite collection error")

def run_migration():
    """Run the complete migration"""
    try:
        # Get database URL from environment variable
        database_url = os.getenv('DATABASE_URL')
        
        # Fallback for development
        if not database_url:
            database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
            logging.warning("Using fallback database URL - consider setting DATABASE_URL environment variable")

        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set and no fallback available")
        
        logging.info("Starting tournament draw visualization tables migration...")
        
        # Step 1: Create tournament draw visualization tables
        engine = create_tournament_draw_visualization_tables(database_url)
        
        # Step 2: Normalize existing IDs to lowercase
        normalize_existing_ids(database_url)
        
        # Step 3: Add database indexes
        add_indexes(database_url)
        
        # Step 4: Run initial data collection for ALL tournaments (no hardcoded IDs)
        try:
            run_initial_data_collection(database_url)
        except Exception as e:
            logging.warning(f"Tournament draw visualization collection failed, but migration can continue: {str(e)}")
        
        logging.info("Tournament draw visualization tables migration completed successfully!")
        
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    setup_logging()
    run_migration()
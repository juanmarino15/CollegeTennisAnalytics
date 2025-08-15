# migrations/rollback_tournament_draw_visualization_tables.py
"""
Rollback migration for tournament draw visualization tables
Run this from the backend directory: python migrations/rollback_tournament_draw_visualization_tables.py
"""

import os
import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add the backend directory to Python path
backend_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_root))

def setup_logging():
    """Set up logging for rollback"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('tournament_draw_visualization_rollback.log'),
            logging.StreamHandler()
        ]
    )

def backup_data_before_rollback(database_url: str):
    """Backup tournament draw data before rollback"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Creating backup of tournament draw data...")
        
        with engine.connect() as conn:
            # Create backup tables with timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Backup tournament_draws data
            backup_draws_sql = text(f"""
                CREATE TABLE tournament_draws_backup_{timestamp} AS 
                SELECT * FROM tournament_draws;
            """)
            
            # Backup tournament_bracket_positions data  
            backup_positions_sql = text(f"""
                CREATE TABLE tournament_bracket_positions_backup_{timestamp} AS 
                SELECT * FROM tournament_bracket_positions;
            """)
            
            try:
                conn.execute(backup_draws_sql)
                conn.commit()
                logging.info(f"Created backup table: tournament_draws_backup_{timestamp}")
            except Exception as e:
                logging.warning(f"Could not backup tournament_draws table: {str(e)}")
            
            try:
                conn.execute(backup_positions_sql)
                conn.commit()
                logging.info(f"Created backup table: tournament_bracket_positions_backup_{timestamp}")
            except Exception as e:
                logging.warning(f"Could not backup tournament_bracket_positions table: {str(e)}")
        
        logging.info("Data backup completed")
        
    except Exception as e:
        logging.error(f"Error creating backup: {str(e)}")
        # Don't raise - user might want to proceed anyway
        logging.warning("Proceeding with rollback despite backup issues")

def drop_indexes(database_url: str):
    """Drop all indexes created for tournament draw visualization"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Dropping tournament draw visualization indexes...")
        
        with engine.connect() as conn:
            indexes_to_drop = [
                # Tournament draws indexes
                "DROP INDEX IF EXISTS idx_tournament_draws_tournament_id;",
                "DROP INDEX IF EXISTS idx_tournament_draws_event_id;",
                "DROP INDEX IF EXISTS idx_tournament_draws_event_type;",
                "DROP INDEX IF EXISTS idx_tournament_draws_gender;",
                "DROP INDEX IF EXISTS idx_tournament_draws_completed;",
                "DROP INDEX IF EXISTS idx_tournament_draws_active;",
                
                # Bracket positions indexes
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_draw_id;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_participant_id;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_draw_position;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_round_number;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_team_name;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_player_match_id;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_seed_number;",
                
                # Composite indexes
                "DROP INDEX IF EXISTS idx_tournament_draws_tournament_event;",
                "DROP INDEX IF EXISTS idx_tournament_draws_tournament_gender;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_draw_round;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_draw_position_combo;",
                
                # Performance indexes
                "DROP INDEX IF EXISTS idx_tournament_draws_updated_at_api;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_is_bye;",
                "DROP INDEX IF EXISTS idx_tournament_bracket_positions_is_winner;",
            ]
            
            for drop_sql in indexes_to_drop:
                try:
                    conn.execute(text(drop_sql))
                    conn.commit()
                    index_name = drop_sql.split()[-1].rstrip(';')
                    logging.info(f"Dropped index: {index_name}")
                except Exception as e:
                    logging.warning(f"Could not drop index (may not exist): {str(e)}")
                    continue
        
        logging.info("Tournament draw visualization indexes dropped successfully")
        
    except Exception as e:
        logging.error(f"Error dropping indexes: {str(e)}")
        # Continue with table dropping even if indexes fail
        logging.warning("Continuing with table removal despite index dropping issues")

def drop_tables(database_url: str):
    """Drop tournament draw visualization tables"""
    try:
        engine = create_engine(database_url)
        
        logging.info("Dropping tournament draw visualization tables...")
        
        with engine.connect() as conn:
            # Drop tables in reverse order due to foreign key constraints
            tables_to_drop = [
                "DROP TABLE IF EXISTS tournament_bracket_positions CASCADE;",
                "DROP TABLE IF EXISTS tournament_draws CASCADE;"
            ]
            
            for drop_sql in tables_to_drop:
                try:
                    conn.execute(text(drop_sql))
                    conn.commit()
                    table_name = drop_sql.split()[4]  # Extract table name
                    logging.info(f"Dropped table: {table_name}")
                except Exception as e:
                    logging.error(f"Could not drop table: {drop_sql} - {str(e)}")
                    continue
        
        logging.info("Tournament draw visualization tables dropped successfully")
        
    except Exception as e:
        logging.error(f"Error dropping tables: {str(e)}")
        raise

def clean_up_backup_tables(database_url: str, keep_days: int = 7):
    """Clean up old backup tables older than specified days"""
    try:
        engine = create_engine(database_url)
        
        logging.info(f"Cleaning up backup tables older than {keep_days} days...")
        
        with engine.connect() as conn:
            # Find old backup tables
            find_backup_tables_sql = text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public' 
                AND (tablename LIKE 'tournament_draws_backup_%' 
                     OR tablename LIKE 'tournament_bracket_positions_backup_%');
            """)
            
            result = conn.execute(find_backup_tables_sql)
            backup_tables = [row[0] for row in result.fetchall()]
            
            from datetime import datetime, timedelta
            cutoff_date = datetime.now() - timedelta(days=keep_days)
            
            for table_name in backup_tables:
                try:
                    # Extract timestamp from table name
                    if '_backup_' in table_name:
                        timestamp_str = table_name.split('_backup_')[1]
                        table_date = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                        
                        if table_date < cutoff_date:
                            drop_sql = text(f"DROP TABLE IF EXISTS {table_name};")
                            conn.execute(drop_sql)
                            conn.commit()
                            logging.info(f"Cleaned up old backup table: {table_name}")
                        else:
                            logging.info(f"Keeping recent backup table: {table_name}")
                            
                except Exception as e:
                    logging.warning(f"Could not process backup table {table_name}: {str(e)}")
                    continue
        
        logging.info("Backup table cleanup completed")
        
    except Exception as e:
        logging.warning(f"Error during backup cleanup: {str(e)}")
        # This is not critical, so don't raise

def run_rollback():
    """Run the complete rollback"""
    try:
        # Get database URL from environment variable
        database_url = os.getenv('DATABASE_URL')
        
        # Fallback for development
        if not database_url:
            database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
            logging.warning("Using fallback database URL - consider setting DATABASE_URL environment variable")

        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set and no fallback available")
        
        # Confirm rollback
        print("WARNING: This will permanently remove all tournament draw visualization tables and data!")
        print("The following will be removed:")
        print("- tournament_draws table")
        print("- tournament_bracket_positions table") 
        print("- All associated indexes")
        print("\nBackup tables will be created before removal.")
        
        confirm = input("\nAre you sure you want to proceed? Type 'YES' to confirm: ")
        
        if confirm != 'YES':
            logging.info("Rollback cancelled by user")
            return
        
        logging.info("Starting tournament draw visualization tables rollback...")
        
        # Step 1: Backup existing data
        backup_data_before_rollback(database_url)
        
        # Step 2: Drop indexes
        drop_indexes(database_url)
        
        # Step 3: Drop tables
        drop_tables(database_url)
        
        # Step 4: Clean up old backup tables (optional)
        clean_up_backup_tables(database_url)
        
        logging.info("Tournament draw visualization tables rollback completed successfully!")
        print("\nRollback completed successfully!")
        print("Your data has been backed up in tables with '_backup_YYYYMMDD_HHMMSS' suffix.")
        
    except Exception as e:
        logging.error(f"Rollback failed: {str(e)}")
        print(f"\nRollback failed: {str(e)}")
        raise

if __name__ == "__main__":
    setup_logging()
    run_rollback()
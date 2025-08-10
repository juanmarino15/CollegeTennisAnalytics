# migrations/rollback_tournament_players_table.py
"""
Rollback script to remove tournament players table if needed
Use with caution! This will permanently delete all tournament registration data.
"""

import os
import logging
from sqlalchemy import create_engine, text

def rollback_tournament_players_table():
    """Remove tournament_players table (use with caution!)"""
    try:
        database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
        if not database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        engine = create_engine(database_url)
        
        logging.warning("Rolling back tournament players table...")
        
        with engine.connect() as conn:
            # Drop indexes first
            index_drops = [
                "DROP INDEX IF EXISTS idx_tournament_players_tournament_id;",
                "DROP INDEX IF EXISTS idx_tournament_players_player_id;",
                "DROP INDEX IF EXISTS idx_tournament_players_gender;",
                "DROP INDEX IF EXISTS idx_tournament_players_state;",
                "DROP INDEX IF EXISTS idx_tournament_players_events_participating;",
                "DROP INDEX IF EXISTS idx_tournament_players_player2_id;",
                "DROP INDEX IF EXISTS idx_tournament_players_player_name;",
                "DROP INDEX IF EXISTS idx_tournament_players_created_at;",
                "DROP INDEX IF EXISTS idx_tournament_players_tournament_gender;",
                "DROP INDEX IF EXISTS idx_tournament_players_tournament_events;",
                "DROP INDEX IF EXISTS idx_tournament_players_player_tournament;",
            ]
            
            for drop_sql in index_drops:
                try:
                    conn.execute(text(drop_sql))
                    conn.commit()
                    logging.info(f"Dropped index: {drop_sql}")
                except Exception as e:
                    logging.warning(f"Index may not exist: {drop_sql} - {str(e)}")
                    continue
            
            # Drop the table
            rollback_sql = [
                "DROP TABLE IF EXISTS tournament_players CASCADE;"
            ]
            
            for sql in rollback_sql:
                conn.execute(text(sql))
                conn.commit()
                logging.info(f"Executed: {sql}")
        
        logging.info("Tournament players table rolled back successfully")
        
    except Exception as e:
        logging.error(f"Error during rollback: {str(e)}")
        raise

def confirm_rollback():
    """Ask for confirmation before rolling back"""
    print("WARNING: This will permanently delete the tournament_players table and all data!")
    print("This action cannot be undone.")
    confirmation = input("Are you sure you want to proceed? Type 'DELETE_TOURNAMENT_PLAYERS' to confirm: ")
    
    if confirmation == "DELETE_TOURNAMENT_PLAYERS":
        return True
    else:
        print("Rollback cancelled.")
        return False

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if confirm_rollback():
        rollback_tournament_players_table()
    else:
        logging.info("Rollback operation cancelled by user")
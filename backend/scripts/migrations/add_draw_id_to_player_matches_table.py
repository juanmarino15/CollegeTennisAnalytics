#!/usr/bin/env python3
"""
Database migration script to add draw_id field to player_matches table
"""

import os
import sys
from pathlib import Path
import logging
from sqlalchemy import create_engine, text

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def add_draw_id_to_player_matches(database_url: str):
    """Add draw_id column to player_matches table"""
    
    engine = create_engine(database_url)
    
    try:
        with engine.connect() as conn:
            # Check if the column already exists
            check_column_sql = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'player_matches' 
                AND column_name = 'draw_id'
            """
            
            result = conn.execute(text(check_column_sql)).fetchone()
            
            if result:
                logging.info("draw_id column already exists in player_matches table")
                return
            
            # Add the draw_id column
            alter_table_sql = """
                ALTER TABLE player_matches 
                ADD COLUMN draw_id VARCHAR(255) NULL
            """
            
            logging.info("Adding draw_id column to player_matches table...")
            conn.execute(text(alter_table_sql))
            conn.commit()
            
            # Create index for better query performance
            create_index_sql = """
                CREATE INDEX IF NOT EXISTS idx_player_matches_draw_id 
                ON player_matches(draw_id)
            """
            
            logging.info("Creating index on draw_id column...")
            conn.execute(text(create_index_sql))
            conn.commit()
            
            # Create composite index for tournament_id and draw_id
            create_composite_index_sql = """
                CREATE INDEX IF NOT EXISTS idx_player_matches_tournament_draw 
                ON player_matches(tournament_id, draw_id)
            """
            
            logging.info("Creating composite index on tournament_id and draw_id...")
            conn.execute(text(create_composite_index_sql))
            conn.commit()
            
            logging.info("Successfully added draw_id column and indexes to player_matches table")
            
    except Exception as e:
        logging.error(f"Error adding draw_id column: {str(e)}")
        raise

def verify_migration(database_url: str):
    """Verify that the migration was successful"""
    
    engine = create_engine(database_url)
    
    try:
        with engine.connect() as conn:
            # Check table structure
            check_structure_sql = """
                SELECT column_name, data_type, is_nullable 
                FROM information_schema.columns 
                WHERE table_name = 'player_matches' 
                AND column_name = 'draw_id'
            """
            
            result = conn.execute(text(check_structure_sql)).fetchone()
            
            if result:
                logging.info(f"✅ draw_id column verified: {result[0]} ({result[1]}, nullable: {result[2]})")
            else:
                logging.error("❌ draw_id column not found after migration")
                return False
                
            # Check indexes
            check_indexes_sql = """
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = 'player_matches' 
                AND indexname LIKE '%draw_id%'
            """
            
            indexes = conn.execute(text(check_indexes_sql)).fetchall()
            logging.info(f"✅ Found {len(indexes)} draw_id related indexes:")
            for idx in indexes:
                logging.info(f"   - {idx[0]}")
                
            return True
            
    except Exception as e:
        logging.error(f"Error verifying migration: {str(e)}")
        return False

if __name__ == "__main__":
    # Update this with your actual database URL
    DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
    
    try:
        logging.info("Starting migration to add draw_id to player_matches table")
        add_draw_id_to_player_matches(DATABASE_URL)
        
        logging.info("Verifying migration...")
        if verify_migration(DATABASE_URL):
            logging.info("✅ Migration completed successfully!")
        else:
            logging.error("❌ Migration verification failed!")
            
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        sys.exit(1)
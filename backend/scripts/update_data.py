# scripts/update_data.py

import os
import sys
from pathlib import Path
import asyncio
import logging
from datetime import datetime
import argparse

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from collector.update_matches import MatchUpdatesService
from collector.player_matches_collector import PlayerMatchesCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_updates.log'),
        logging.StreamHandler()
    ]
)

# Get DATABASE_URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    # Fallback for local development
    # DATABASE_URL = "postgresql://juanmarino@localhost:5432/college_tennis_db"
    DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"


async def update_matches():
    """Update team matches"""
    try:
        logging.info("Starting match updates")
        start_time = datetime.now()
        
        updater = MatchUpdatesService(DATABASE_URL)
        await updater.update_matches()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed match updates. Duration: {duration}")
    except Exception as e:
        logging.error(f"Error in match updates: {str(e)}")
        raise

def update_player_matches():
    """Update individual player matches"""
    try:
        logging.info("Starting player matches update")
        start_time = datetime.now()
        
        collector = PlayerMatchesCollector(DATABASE_URL)
        collector.store_all_player_matches()
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed player matches update. Duration: {duration}")
    except Exception as e:
        logging.error(f"Error in player matches update: {str(e)}")
        raise

async def update_recent_rosters():
    """Update rosters for schools with recent matches"""
    try:
        logging.info("Starting roster updates for schools with recent matches")
        start_time = datetime.now()
        
        collector = MatchUpdatesService(DATABASE_URL)
        season_id = "0e384cf2-fba6-4bd3-a441-7eb5b2c40300"  # 2024 season
        await collector.process_recent_school_rosters(season_id)
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed roster updates. Duration: {duration}")
    except Exception as e:
        logging.error(f"Error in roster updates: {str(e)}")
        raise

async def main():
    parser = argparse.ArgumentParser(description='Update tennis data')
    parser.add_argument('--matches', action='store_true', help='Update team matches')
    parser.add_argument('--player-matches', action='store_true', help='Update player matches')
    parser.add_argument('--rosters', action='store_true', help='Update rosters for teams with recent matches')
    parser.add_argument('--all', action='store_true', help='Update all data')
    
    args = parser.parse_args()
    
    if not (args.matches or args.player_matches or args.rosters or args.all):
        parser.print_help()
        return

    try:
        if args.all or args.matches:
            await update_matches()
            
        if args.all or args.player_matches:
            update_player_matches()
            
        if args.all or args.rosters:
            await update_recent_rosters()
            
    except Exception as e:
        logging.error(f"Error in update process: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
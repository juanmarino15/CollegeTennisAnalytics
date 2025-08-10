import os
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from collector.tournament_collector import TournamentCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tournament_updates.log'),
        logging.StreamHandler()
    ]
)

DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

def update_tournaments():
    """Update tournaments from 2 weeks before today to 2 weeks after today"""
    try:
        logging.info("Starting tournament collection")
        start_time = datetime.now()
        
        collector = TournamentCollector(DATABASE_URL)
        
        # 2 weeks before today to 2 weeks after today
        today = datetime.now()
        two_weeks_before = today - timedelta(days=7)
        two_weeks_after = today + timedelta(days=7)
        
        start_date = two_weeks_before.strftime('%Y-%m-%dT00:00:00.000Z')
        end_date = two_weeks_after.strftime('%Y-%m-%dT23:59:59.000Z')
        
        logging.info(f"Collecting tournaments from {start_date} to {end_date}")
        
        collector.collect_tournaments_range(
            start_date=start_date,
            end_date=end_date,
            batch_size=200
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed tournament collection. Duration: {duration}")
        
    except Exception as e:
        logging.error(f"Error in tournament collection: {str(e)}")
        raise

if __name__ == "__main__":
    update_tournaments()
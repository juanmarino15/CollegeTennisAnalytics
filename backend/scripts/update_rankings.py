# scripts/update_rankings.py

import os
import sys
from pathlib import Path
import logging
from datetime import datetime
import argparse

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from collector.rankings_collector import RankingsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rankings_updates.log'),
        logging.StreamHandler()
    ]
)

DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

def update_all_rankings(max_lists=3):
    """Update all types of tennis rankings data"""
    try:
        logging.info("Starting rankings update for all formats")
        start_time = datetime.now()
        
        collector = RankingsCollector(DATABASE_URL)
        collector.collect_all_rankings(max_lists_to_process=max_lists)
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed rankings update. Duration: {duration}")
    except Exception as e:
        logging.error(f"Error in rankings update: {str(e)}")
        raise

def update_team_rankings(max_lists=None):
    """Update team rankings only"""
    try:
        logging.info("Starting team rankings update")
        start_time = datetime.now()
        
        collector = RankingsCollector(DATABASE_URL)
        collector.collect_team_rankings(max_lists_to_process=max_lists)
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed team rankings update. Duration: {duration}")
    except Exception as e:
        logging.error(f"Error in team rankings update: {str(e)}")
        raise

def update_singles_rankings(max_lists=None):
    """Update singles rankings for both men and women"""
    try:
        logging.info("Starting singles rankings update")
        start_time = datetime.now()
        
        collector = RankingsCollector(DATABASE_URL)
        # Pass None to process all lists, and specify both genders
        collector.collect_singles_rankings(max_lists_to_process=max_lists, genders=['M', 'F'])
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed singles rankings update. Duration: {duration}")
    except Exception as e:
        logging.error(f"Error in singles rankings update: {str(e)}")
        raise

def update_doubles_rankings(max_lists=None):
    """Update doubles rankings for both men and women"""
    try:
        logging.info("Starting doubles rankings update")
        start_time = datetime.now()
        
        collector = RankingsCollector(DATABASE_URL)
        # Pass None to process all lists, and specify both genders
        collector.collect_doubles_rankings(max_lists_to_process=max_lists, genders=['M', 'F'])
        
        end_time = datetime.now()
        duration = end_time - start_time
        logging.info(f"Completed doubles rankings update. Duration: {duration}")
    except Exception as e:
        logging.error(f"Error in doubles rankings update: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Update tennis rankings data')
    parser.add_argument('--men', action='store_true', help='Update men\'s rankings')
    parser.add_argument('--women', action='store_true', help='Update women\'s rankings')
    parser.add_argument('--division', choices=['DIV1', 'DIV2', 'DIV3', 'ALL'], default='DIV1', help='Division to update')
    parser.add_argument('--format', choices=['TEAM', 'SINGLES', 'DOUBLES', 'ALL'], default='ALL', help='Match format to update')
    parser.add_argument('--max-lists', type=int, default=5, help='Maximum number of ranking lists to process per format')
    
    args = parser.parse_args()
    
    # By default, update men's rankings if no specific option is provided
    if not (args.men or args.women):
        args.men = True
    
    try:
        # Currently we're only implementing men's DIV1 updates
        
        if args.format == 'ALL':
            logging.info("Updating all ranking formats")
            update_all_rankings(max_lists=args.max_lists)
        elif args.format == 'TEAM':
            logging.info("Updating team rankings")
            update_team_rankings(max_lists=args.max_lists)
        elif args.format == 'SINGLES':
            logging.info("Updating singles rankings")
            update_singles_rankings(max_lists=args.max_lists)
        elif args.format == 'DOUBLES':
            logging.info("Updating doubles rankings")
            update_doubles_rankings(max_lists=args.max_lists)
            
    except Exception as e:
        logging.error(f"Error in update process: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
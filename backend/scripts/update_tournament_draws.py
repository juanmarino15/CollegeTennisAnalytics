# scripts/update_tournament_draws.py
import os
import sys
from pathlib import Path
import logging
import argparse
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from collector.tournament_draw_collector import StandaloneTournamentCollector
from models.models import Tournament, TournamentEvent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tournament_draws_updates.log'),
        logging.StreamHandler()
    ]
)

DATABASE_URL = os.getenv('DATABASE_URL', "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require")

def get_tournament_events_in_date_range(start_date: datetime, end_date: datetime, extended: bool = False):
    """Get tournament events within the specified date range"""
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        query = session.query(
            TournamentEvent.tournament_id,
            TournamentEvent.event_id,
            TournamentEvent.gender,
            TournamentEvent.event_type,
            Tournament.name.label('tournament_name'),
            Tournament.start_date_time,
            Tournament.end_date_time
        ).join(
            Tournament, TournamentEvent.tournament_id == Tournament.tournament_id
        ).filter(
            # Tournament overlaps with our date range
            Tournament.start_date_time <= end_date,
            Tournament.end_date_time >= start_date,
            Tournament.is_cancelled == False
        )
        
        if extended:
            # For extended mode, get more tournaments but limit for safety
            query = query.order_by(Tournament.start_date_time.desc()).limit(200)
        else:
            # For daily mode, no limit needed since date range is small
            query = query.order_by(Tournament.start_date_time.desc())
        
        tournament_events = query.all()
        session.close()
        
        # Convert IDs to uppercase for consistency
        tournament_events_upper = [
            (tournament_id.upper() if tournament_id else None,
             event_id.upper() if event_id else None,
             gender, event_type, tournament_name, start_date, end_date)
            for tournament_id, event_id, gender, event_type, tournament_name, start_date, end_date in tournament_events
        ]
        
        return tournament_events_upper
        
    except Exception as e:
        logging.error(f"Error querying tournament events: {str(e)}")
        return []

def update_tournament_draws(extended: bool = False):
    """Update tournament draws - daily or extended mode"""
    try:
        mode = "EXTENDED" if extended else "DAILY"
        logging.info(f"Starting {mode} tournament draws collection")
        start_time = datetime.now()
        
        collector = StandaloneTournamentCollector(DATABASE_URL, dry_run=False)
        
        # Set date range based on mode
        today = datetime.now()
        if extended:
            # Extended mode: last 30 days to next 7 days
            start = today - timedelta(days=30)
            end = today + timedelta(days=7)
            logging.info("Extended mode: Collecting from last 30 days to next 7 days")
        else:
            # Daily mode: last 7 days to next 1 day  
            start = today - timedelta(days=7)
            end = today + timedelta(days=1)
            logging.info("Daily mode: Collecting from last 7 days to tomorrow")
        
        # Set to start and end of days for full coverage
        start_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        logging.info(f"Collecting tournament draws for tournaments from {start_date.date()} to {end_date.date()}")
        
        # Get tournament events in the date range
        tournament_events = get_tournament_events_in_date_range(start_date, end_date, extended)
        
        if not tournament_events:
            logging.info("No tournament events found in the specified date range")
            return
        
        logging.info(f"Found {len(tournament_events)} tournament events to process")
        
        success_count = 0
        error_count = 0
        total_draws_collected = 0
        total_matches_collected = 0
        
        for i, event_row in enumerate(tournament_events, 1):
            tournament_id, event_id, gender, event_type, tournament_name, tournament_start_date, tournament_end_date = event_row
            
            try:
                logging.info(f"Processing {i}/{len(tournament_events)}: {tournament_name}")
                logging.info(f"   Tournament: {tournament_id}")
                logging.info(f"   Event: {event_id} ({gender} {event_type})")
                logging.info(f"   Dates: {tournament_start_date.date()} to {tournament_end_date.date()}")
                
                # Collect draws and matches for this tournament event
                success = collector.collect_tournament_event(tournament_id, event_id)
                
                if success:
                    success_count += 1
                    logging.info(f"✅ Successfully processed tournament event {i}/{len(tournament_events)}")
                    # Note: Individual draw/match counts are logged within the collector
                else:
                    error_count += 1
                    logging.error(f"❌ Failed to process tournament event {i}/{len(tournament_events)}")
                
                # Small delay between requests to be respectful to the API
                import time
                time.sleep(1.0)  # Slightly longer delay for GraphQL API
                
            except Exception as e:
                error_count += 1
                logging.error(f"❌ Error processing tournament event {tournament_id}/{event_id} ({tournament_name}): {str(e)}")
                continue
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info("="*60)
        logging.info(f"{mode} TOURNAMENT DRAWS UPDATE SUMMARY")
        logging.info("="*60)
        logging.info(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logging.info(f"Tournament Events Processed: {len(tournament_events)}")
        logging.info(f"Successful: {success_count}")
        logging.info(f"Errors: {error_count}")
        logging.info(f"Success Rate: {(success_count/len(tournament_events)*100):.1f}%" if tournament_events else "0%")
        logging.info(f"Duration: {duration}")
        logging.info("="*60)
        
        # Return success if we processed something or there was nothing to process
        return success_count > 0 or len(tournament_events) == 0
        
    except Exception as e:
        logging.error(f"Error in {mode.lower()} tournament draws collection: {str(e)}")
        raise

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description='Update Tournament Draws')
    parser.add_argument('--extended', action='store_true', 
                       help='Run extended collection (last 30 days vs last 7 days)')
    
    args = parser.parse_args()
    
    try:
        success = update_tournament_draws(extended=args.extended)
        if success:
            logging.info("🏆 Tournament draws update completed successfully!")
            sys.exit(0)
        else:
            logging.error("💥 Tournament draws update failed!")
            sys.exit(1)
    except Exception as e:
        logging.error(f"💥 Tournament draws update failed with exception: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
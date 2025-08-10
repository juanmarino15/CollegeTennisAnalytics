# scripts/update_tournament_players.py
import os
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from collector.tournament_players_collector import TournamentPlayersCollector
from models.models import Tournament

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tournament_players_updates.log'),
        logging.StreamHandler()
    ]
)

DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

def get_tournaments_in_date_range(start_date: datetime, end_date: datetime):
    """Get tournaments within the specified date range"""
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        tournaments = session.query(Tournament.tournament_id, Tournament.name, Tournament.start_date_time).filter(
            Tournament.is_cancelled == False,
            Tournament.start_date_time >= start_date,
            Tournament.start_date_time <= end_date
        ).order_by(Tournament.start_date_time.desc()).all()
        
        session.close()
        return tournaments
        
    except Exception as e:
        logging.error(f"Error querying tournaments: {str(e)}")
        return []

def update_tournament_players():
    """Update tournament players from yesterday to tomorrow"""
    try:
        logging.info("Starting daily tournament players collection")
        start_time = datetime.now()
        
        collector = TournamentPlayersCollector(DATABASE_URL)
        
        # Yesterday to tomorrow (3-day window)
        today = datetime.now()
        start = today - timedelta(days=7)
        end = today + timedelta(days=7)
        
        # Set to start of yesterday and end of tomorrow for full coverage
        start_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        logging.info(f"Collecting tournament players for tournaments from {start_date} to {end_date}")
        
        # Get tournaments in the date range
        tournaments = get_tournaments_in_date_range(start_date, end_date)
        
        if not tournaments:
            logging.info("No tournaments found in the specified date range")
            return
        
        logging.info(f"Found {len(tournaments)} tournaments to process")
        
        success_count = 0
        error_count = 0
        total_players_collected = 0
        
        for tournament_id, tournament_name, tournament_start_date in tournaments:
            try:
                logging.info(f"Processing tournament: {tournament_name} ({tournament_id}) - Start: {tournament_start_date}")
                
                # Collect players for this tournament
                players_data = collector.fetch_tournament_players(tournament_id, limit=0, offset=0)
                
                if players_data and 'data' in players_data and 'paginatedPublicTournamentRegistrations' in players_data['data']:
                    registrations_data = players_data['data']['paginatedPublicTournamentRegistrations']
                    player_count = len(registrations_data.get('items', []))
                    
                    if player_count > 0:
                        collector.store_tournament_players(tournament_id, players_data)
                        total_players_collected += player_count
                        logging.info(f"✅ Successfully collected {player_count} players for {tournament_name}")
                    else:
                        logging.info(f"⚠️  No players found for {tournament_name}")
                else:
                    logging.info(f"⚠️  No player data available for {tournament_name}")
                
                success_count += 1
                
                # Small delay between requests to be respectful to the API
                import time
                time.sleep(0.5)
                
            except Exception as e:
                error_count += 1
                logging.error(f"❌ Error processing tournament {tournament_id} ({tournament_name}): {str(e)}")
                continue
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logging.info("="*60)
        logging.info("DAILY TOURNAMENT PLAYERS UPDATE SUMMARY")
        logging.info("="*60)
        logging.info(f"Date Range: {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}")
        logging.info(f"Tournaments Processed: {len(tournaments)}")
        logging.info(f"Successful: {success_count}")
        logging.info(f"Errors: {error_count}")
        logging.info(f"Total Players Collected: {total_players_collected}")
        logging.info(f"Duration: {duration}")
        logging.info("="*60)
        
    except Exception as e:
        logging.error(f"Error in daily tournament players collection: {str(e)}")
        raise


if __name__ == "__main__":

    update_tournament_players()
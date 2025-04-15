#!/usr/bin/env python3
# Script to update the collection_position field in player_matches table
# for existing matches by fetching data from the API

import os
import sys
import logging
from pathlib import Path
import requests
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Set up logging to both console and file
def setup_logging():
    log_file = f"collection_position_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file)
    
    # Create formatters
    console_format = logging.Formatter('%(message)s')
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Set formatters
    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    print(f"Logging to {log_file}")
    return logger

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from models.models import Base, PlayerMatch, PlayerMatchParticipant
from collector.player_matches_collector import PlayerMatchesCollector

# Get DATABASE_URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    # Fallback for local development
    DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

def setup_database():
    """Set up database connection and return session maker"""
    try:
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(engine)
        logging.info(f"Database connection established successfully")
        return sessionmaker(bind=engine)
    except Exception as e:
        logging.error(f"Database initialization error: {e}")
        return None

def get_matches_without_position(Session):
    """Get matches that don't have collection_position set within a specific date range"""
    session = Session()
    try:
        # Get July 1st, 2024 as the start date
        start_date = datetime(2024, 7, 1, 0, 0, 0)
        
        # Get current datetime as the end date
        end_date = datetime.now()
        
        # Query matches with NULL collection_position within the date range
        matches = session.query(PlayerMatch).filter(
            PlayerMatch.collection_position == None,
            PlayerMatch.start_time >= start_date,
            PlayerMatch.start_time <= end_date
        ).all()
        
        logging.info(f"Found {len(matches)} matches without collection_position between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}")
        return matches
    except Exception as e:
        logging.error(f"Error querying matches: {e}")
        return []
    finally:
        session.close()

def get_player_ids_for_match(Session, match_id):
    """Get player IDs for a specific match"""
    session = Session()
    try:
        participants = session.query(PlayerMatchParticipant).filter(
            PlayerMatchParticipant.match_id == match_id
        ).all()
        
        player_ids = [p.person_id for p in participants if p.person_id]
        logging.info(f"Found {len(player_ids)} players for match {match_id}")
        return player_ids
    except Exception as e:
        logging.error(f"Error getting player IDs: {e}")
        return []
    finally:
        session.close()

def fetch_match_data(player_id, match_start_date, logger):
    """Fetch match data for a specific player and date from API"""
    collector = PlayerMatchesCollector(DATABASE_URL)
    
    # Format date for API query
    # Use 7-day window centered around the match date to increase chances of finding it
    match_date = match_start_date.strftime('%Y-%m-%d')
    
    # Calculate date range (3 days before and 3 days after)
    start_date = (match_start_date - timedelta(days=3)).strftime('%Y-%m-%d')
    end_date = (match_start_date + timedelta(days=3)).strftime('%Y-%m-%d')
    
    logger.info(f"Searching for matches between {start_date} and {end_date}")
    
    # Set up query with specific date range
    query = """query matchUps($personFilter: [td_PersonFilterOptions], $filter: td_MatchUpFilterOptions) {
        td_matchUps(personFilter: $personFilter, filter: $filter) {
            totalItems
            items {
                score {
                    scoreString
                }
                sides {
                    sideNumber
                    players {
                        person {
                            externalID
                        }
                    }
                }
                winningSide
                start
                end
                type
                matchUpFormat
                status
                tournament {
                    providerTournamentId
                }
                roundName
                collectionPosition
            }
        }
    }"""

    variables = {
        "personFilter": {
            "ids": [{
                "type": "ExternalID",
                "identifier": player_id
            }]
        },
        "filter": {
            "start": {"after": start_date, "before": end_date},
        }
    }

    try:
        response = requests.post(
            collector.api_url,
            json={
                'operationName': 'matchUps',
                'query': query,
                'variables': variables
            },
            headers=collector.headers,
            verify=False
        )
        
        if response.status_code == 200:
            data = response.json()
            
            # Debug the API response
            logger.info(f"API response for player {player_id} in date range:")
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Keys in response: {data.keys() if isinstance(data, dict) else 'Not a dict'}")
            
            if isinstance(data, dict) and 'data' in data:
                logger.info(f"Keys in data: {data['data'].keys() if isinstance(data['data'], dict) else 'Not a dict'}")
                
                if isinstance(data['data'], dict) and 'td_matchUps' in data['data']:
                    logger.info(f"Keys in td_matchUps: {data['data']['td_matchUps'].keys() if isinstance(data['data']['td_matchUps'], dict) else 'Not a dict'}")
                    
                    if isinstance(data['data']['td_matchUps'], dict) and 'items' in data['data']['td_matchUps']:
                        items = data['data']['td_matchUps']['items']
                        if items is not None:
                            logger.info(f"Found {len(items)} matches for player {player_id} in date range")
                            # Print all match dates for verification
                            if len(items) > 0:
                                logger.info("Matches found on dates:")
                                for item in items:
                                    if 'start' in item:
                                        match_date = item['start'].split('T')[0]
                                        logger.info(f"  - {match_date} (Type: {item.get('type', 'unknown')}, Position: {item.get('collectionPosition', 'unknown')})")
                            return items
                        else:
                            logger.info(f"Items is None for player {player_id}")
                    else:
                        logger.info(f"No 'items' in td_matchUps for player {player_id}")
                else:
                    logger.info(f"No 'td_matchUps' in data for player {player_id}")
            else:
                logger.info(f"No 'data' in response for player {player_id}")
        
        logger.error(f"Error fetching match data: Status {response.status_code}")
        return []
            
    except Exception as e:
        logger.error(f"Error fetching match data: {e}")
        logger.error(f"Response content: {response.text if 'response' in locals() else 'No response'}")
        return []

def match_corresponds_to_record(api_match, db_match, player_ids, logger):
    """Check if API match corresponds to database record"""
    try:
        # Create unique identifier for API match
        api_player_ids = []
        
        # Check if sides exists and is not None
        if 'sides' not in api_match or api_match['sides'] is None:
            logger.warning(f"API match missing sides data: {api_match.keys()}")
            return False
            
        for side in api_match['sides']:
            if 'players' not in side or side['players'] is None:
                continue
                
            for player in side['players']:
                if 'person' not in player or player['person'] is None:
                    continue
                    
                if 'externalID' not in player['person'] or player['person']['externalID'] is None:
                    continue
                    
                api_player_ids.append(player['person']['externalID'])
        
        # Check overlap between player IDs
        common_players = set(api_player_ids) & set(player_ids)
        
        # Check if required fields exist
        if 'start' not in api_match or not api_match['start']:
            logger.warning("API match missing start time")
            return False
            
        if 'type' not in api_match or not api_match['type']:
            logger.warning("API match missing type")
            return False
        
        # Check if start times match
        api_start = datetime.fromisoformat(api_match['start'].replace('Z', '+00:00'))
        db_start = db_match.start_time
        
        # Time might be slightly different, so just check date
        date_matches = api_start.date() == db_start.date()
        
        # Check if match type matches
        type_matches = api_match['type'] == db_match.match_type
        
        # Match if at least one player is common and date+type match
        result = len(common_players) > 0 and date_matches and type_matches
        
        if result:
            logger.info(f"Match found! Common players: {common_players}, Date match: {date_matches}, Type match: {type_matches}")
        
        return result
    except Exception as e:
        logger.error(f"Error in match_corresponds_to_record: {e}")
        logger.error(f"API match keys: {api_match.keys() if isinstance(api_match, dict) else 'Not a dict'}")
        return False

def update_collection_positions():
    """Main function to update collection positions"""
    logger = setup_logging()
    logger.info("Starting collection_position update process...")
    
    # Set up database
    Session = setup_database()
    if not Session:
        logger.error("Failed to initialize database, exiting.")
        return
    
    # Get matches without position
    matches = get_matches_without_position(Session)
    
    updated_count = 0
    error_count = 0
    
    # Allow user to specify a start position
    start_from = 0
    try:
        user_input = input(f"Found {len(matches)} matches to process. Start from which position? (0 for beginning): ")
        if user_input.strip():
            start_from = int(user_input)
    except ValueError:
        logger.info("Invalid input, starting from beginning")
        start_from = 0
    
    # Process each match
    for i, match in enumerate(matches[start_from:], start_from + 1):
        logger.info(f"\nProcessing match {i}/{len(matches)}: ID {match.id}")
        
        try:
            # Print match details for debugging
            logger.info(f"Match details: Type: {match.match_type}, Time: {match.start_time}, Score: {match.score_string}")
            
            # Get player IDs for this match
            player_ids = get_player_ids_for_match(Session, match.id)
            if not player_ids:
                logger.warning(f"No players found for match {match.id}, skipping")
                error_count += 1
                continue
            
            logger.info(f"Players in match: {player_ids}")
            
            # Extend window range to account for potential date differences
            match_date = match.start_time
            
            # Try fetching data using first player
            match_items = fetch_match_data(player_ids[0], match_date, logger)
            
            if not match_items and len(player_ids) > 1:
                # If first player doesn't work, try another player
                logger.info(f"Trying with second player {player_ids[1]}")
                match_items = fetch_match_data(player_ids[1], match_date, logger)
                
                if not match_items and len(player_ids) > 2:
                    # Try a third player if available
                    logger.info(f"Trying with third player {player_ids[2]}")
                    match_items = fetch_match_data(player_ids[2], match_date, logger)
            
            # Find matching match in API results
            collection_position = None
            for api_match in match_items:
                if match_corresponds_to_record(api_match, match, player_ids, logger):
                    collection_position = api_match.get('collectionPosition')
                    if collection_position is None:
                        logger.warning(f"Found matching match but collection_position is null in API response")
                    else:
                        logger.info(f"Found match with collection_position: {collection_position}")
                    break
            
            # Only update if we found a non-null collection_position
            if collection_position is not None:
                # Update database record
                session = Session()
                try:
                    session.query(PlayerMatch).filter(
                        PlayerMatch.id == match.id
                    ).update({
                        "collection_position": collection_position
                    })
                    session.commit()
                    updated_count += 1
                    logger.info(f"Successfully updated match {match.id} with position {collection_position}")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error updating match {match.id}: {e}")
                    error_count += 1
                finally:
                    session.close()
            else:
                logger.warning(f"Could not find non-null collection_position for match {match.id}")
                error_count += 1
            
            # Rate limiting
            time.sleep(1)
            
            # Option to pause after each batch
            # if i % 10 == 0:
            #     choice = input(f"Processed {i}/{len(matches)} matches. Continue? (y/n): ")
            #     if choice.lower() != 'y':
            #         logger.info("Exiting by user request")
            #         break
            
        except Exception as e:
            logger.error(f"Error processing match {match.id}: {e}")
            error_count += 1
            continue
    
    logger.info("\nUpdate process completed!")
    logger.info(f"Matches updated: {updated_count}")
    logger.info(f"Errors/skipped: {error_count}")
    logger.info(f"Total processed: {i - start_from}")  # Only count the ones we actually processed

if __name__ == "__main__":
    # Set up logging before any operations
    setup_logging()
    update_collection_positions()
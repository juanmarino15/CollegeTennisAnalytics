# truncate_and_repopulate_player_matches.py

import os
import sys
from pathlib import Path
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from models.models import (
    Base, PlayerMatch, PlayerMatchSet, PlayerMatchParticipant,
    Player, PlayerRoster, Season
)
from collector.player_matches_collector import PlayerMatchesCollector

# Configure database connection
DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

def truncate_player_matches_tables(db_url):
    """Truncate all player matches related tables"""
    print("Starting truncate operation...")
    
    engine = create_engine(db_url)
    connection = engine.connect()
    
    try:
        # Disable foreign key constraints during truncate
        connection.execute(text("SET CONSTRAINTS ALL DEFERRED;"))
        
        # Truncate tables in proper order
        print("Truncating player_match_sets...")
        connection.execute(text("TRUNCATE TABLE player_match_sets CASCADE;"))
        
        print("Truncating player_match_participants...")
        connection.execute(text("TRUNCATE TABLE player_match_participants CASCADE;"))
        
        print("Truncating player_matches...")
        connection.execute(text("TRUNCATE TABLE player_matches CASCADE;"))
        
        # Commit the transaction
        connection.execute(text("COMMIT;"))
        print("All tables truncated successfully.")
        
    except Exception as e:
        print(f"Error during truncate operation: {e}")
        raise
    finally:
        connection.close()
        engine.dispose()

def repopulate_player_matches(db_url, days_back=90):
    """Repopulate player matches with improved duplicate detection"""
    print(f"Starting repopulation of player matches for the last {days_back} days...")
    
    # Create collector instance
    collector = PlayerMatchesCollector(db_url)
    
    # Get active players
    try:
        # First try the ORM method
        active_players = collector.get_recently_active_players_orm()
        if not active_players:
            print("No players found with ORM method, trying SQL method")
            active_players = collector.get_recently_active_players()
    except Exception as e:
        print(f"Error getting active players: {e}")
        active_players = collector.get_recently_active_players()
    
    total_players = len(active_players)
    print(f"Found {total_players} recently active players to process")
    
    # Add additional tracking metrics
    total_matches_processed = 0
    total_matches_stored = 0
    total_sets_stored = 0
    total_participants_stored = 0
    
    # Create direct database session for bulk operations
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Track processed match identifiers to avoid duplicates
    processed_identifiers = set()
    
    try:
        for idx, player_id in enumerate(active_players, 1):
            try:
                print(f"\nProcessing player {idx}/{total_players}: ID: {player_id}")
                
                # Fetch match data with modified date range to get more history
                matches_data = fetch_player_matches_with_extended_range(collector, player_id, days_back)
                
                if not matches_data or 'data' not in matches_data or \
                   'td_matchUps' not in matches_data['data'] or \
                   not matches_data['data']['td_matchUps']['items']:
                    print(f"No match data found for player {player_id}")
                    continue
                
                # Process matches
                matches = matches_data['data']['td_matchUps']['items']
                print(f"Found {len(matches)} matches for player {player_id}")
                total_matches_processed += len(matches)
                
                # Process each match
                player_matches_stored = 0
                
                for match_item in matches:
                    # Create unique identifier with improved logic
                    match_identifier = create_improved_match_identifier(match_item)
                    
                    # Skip if already processed in this run
                    if match_identifier in processed_identifiers:
                        print(f"Skipping already processed match: {match_identifier}")
                        continue
                    
                    # Add to processed set
                    processed_identifiers.add(match_identifier)
                    
                    # Process the match
                    success, sets_stored, participants_stored = store_match_with_error_handling(
                        session, match_item, match_identifier
                    )
                    
                    if success:
                        player_matches_stored += 1
                        total_matches_stored += 1
                        total_sets_stored += sets_stored
                        total_participants_stored += participants_stored
                
                print(f"Successfully stored {player_matches_stored} matches for player {player_id}")
                
                # Commit after each player to avoid losing all data if there's an error
                session.commit()
                
            except Exception as e:
                print(f"Error processing player {player_id}: {e}")
                session.rollback()
                continue
                
            # Add a slight delay between players to avoid API rate limits
            time.sleep(0.5)
        
        # Final statistics
        print("\nRepopulation completed!")
        print(f"Total matches processed: {total_matches_processed}")
        print(f"Total matches stored: {total_matches_stored}")
        print(f"Total sets stored: {total_sets_stored}")
        print(f"Total participants stored: {total_participants_stored}")
        
    except Exception as e:
        print(f"Error during repopulation process: {e}")
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()

def fetch_player_matches_with_extended_range(collector, person_id, days_back=90):
    """Fetch match results for a player with extended date range"""
    query = """query matchUps($personFilter: [td_PersonFilterOptions], $filter: td_MatchUpFilterOptions) {
        td_matchUps(personFilter: $personFilter, filter: $filter) {
            totalItems
            items {
                score {
                    scoreString
                    sets {
                        winnerGamesWon
                        loserGamesWon
                        winRatio
                        tiebreaker {
                            winnerPointsWon
                            loserPointsWon
                        }
                    }
                    superTiebreak {
                        winnerPointsWon
                        loserPointsWon
                    }
                }
                sides {
                    sideNumber
                    players {
                        person {
                            externalID
                            nativeFamilyName
                            nativeGivenName
                        }
                    }
                    extensions {
                        name
                        value
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
                extensions {
                    name
                    value
                }
                roundName
                collectionPosition
            }
        }
    }"""

    # Calculate dates for extended range
    today = datetime.now()
    days_ago = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
    current_date = today.strftime('%Y-%m-%d')

    variables = {
        "personFilter": {
            "ids": [{
                "type": "ExternalID",
                "identifier": person_id
            }]
        },
        "filter": {
            "start": {"after": days_ago},
            "end": {"before": current_date},
            "statuses": ["DEFAULTED", "RETIRED", "WALKOVER", "COMPLETED", "ABANDONED"]
        }
    }

    try:
        import requests
        print(f"Fetching matches for player {person_id} from {days_ago} to {current_date}...")
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
            if 'data' in data and 'td_matchUps' in data['data']:
                items = data['data']['td_matchUps']['items']
                print(f"Found {len(items)} matches for player")
            return data
        else:
            print(f"Error fetching matches: Status {response.status_code}")
            return {}
            
    except Exception as e:
        print(f"Error fetching matches: {e}")
        return {}

def create_improved_match_identifier(match_data):
    """Create a unique identifier for a match with improved logic"""
    try:
        # Extract all player IDs from both sides
        player_ids = []
        for side in match_data.get('sides', []):
            if not side:
                continue
            for player in side.get('players', []):
                if player and 'person' in player and player['person'] and 'externalID' in player['person']:
                    player_ids.append(player['person']['externalID'])
        
        # Sort player IDs for consistency
        player_ids.sort()
        
        # Extract date information
        date_str = match_data.get('start', '').split('T')[0] if match_data.get('start') else 'unknown_date'
        
        # Extract tournament ID
        tournament_data = match_data.get('tournament', {})
        tournament_id = tournament_data.get('providerTournamentId', 'unknown_tournament') if tournament_data else 'unknown_tournament'
        
        # Extract match type
        match_type = match_data.get('type', 'unknown_type')
        
        # Extract collection position (for dual matches)
        collection_pos = match_data.get('collectionPosition', 'np')
        
        # Create a comprehensive identifier that minimizes collision risk
        identifier = f"{date_str}-{tournament_id}-{match_type}-{collection_pos}-{'-'.join(player_ids)}"
        
        return identifier
    except Exception as e:
        print(f"Error creating match identifier: {e}")
        # Fallback to timestamp to avoid duplication errors
        return f"error-identifier-{datetime.now().timestamp()}"

def store_match_with_error_handling(session, match_item, match_identifier):
    """Store a match with comprehensive error handling"""
    try:
        # Check for required fields before proceeding
        if not match_item or 'sides' not in match_item or not match_item.get('sides'):
            print(f"Skipping match with incomplete data structure: {match_identifier}")
            return False, 0, 0

        # Parse dates
        try:
            start_time = datetime.fromisoformat(match_item['start'].replace('Z', '+00:00')) if match_item.get('start') else None
            end_time = datetime.fromisoformat(match_item['end'].replace('Z', '+00:00')) if match_item.get('end') else None
        except (ValueError, KeyError) as e:
            print(f"Date parsing error: {e}")
            start_time = datetime.now()
            end_time = datetime.now()
        
        # Create match record with safe access to nested objects
        tournament_data = match_item.get('tournament') or {}
        score_data = match_item.get('score') or {}
        
        match = PlayerMatch(
            match_identifier=match_identifier,
            winning_side=match_item.get('winningSide'),
            start_time=start_time,
            end_time=end_time,
            match_type=match_item.get('type'),
            match_format=match_item.get('matchUpFormat'),
            status=match_item.get('status'),
            round_name=match_item.get('roundName'),
            tournament_id=tournament_data.get('providerTournamentId', ''),
            score_string=score_data.get('scoreString', ''),
            collection_position=match_item.get('collectionPosition')
        )
        session.add(match)
        session.flush()
        
        # Track counts for reporting
        sets_stored = 0
        participants_stored = 0
        
        # Store sets information (safely check for existence)
        sets_data = score_data.get('sets') or []
        
        for set_idx, set_data in enumerate(sets_data, 1):
            if not set_data:
                continue
                
            try:
                tiebreaker_data = set_data.get('tiebreaker') or {}
                
                match_set = PlayerMatchSet(
                    match_id=match.id,
                    set_number=set_idx,
                    winner_games_won=set_data.get('winnerGamesWon'),
                    loser_games_won=set_data.get('loserGamesWon'),
                    win_ratio=set_data.get('winRatio'),
                    tiebreak_winner_points=tiebreaker_data.get('winnerPointsWon'),
                    tiebreak_loser_points=tiebreaker_data.get('loserPointsWon')
                )
                session.add(match_set)
                sets_stored += 1
            except Exception as e:
                print(f"Error storing set {set_idx}: {e}")
                continue
        
        # Store participants
        for side in match_item.get('sides', []):
            if not side:
                continue
                
            # Get team ID from extensions
            team_id = None
            for ext in side.get('extensions', []):
                if ext and ext.get('name') in ['teamId', 'schoolId']:
                    team_id = ext.get('value')
                    break
            
            for player in side.get('players', []):
                try:
                    if not player or 'person' not in player or not player.get('person'):
                        continue

                    person_data = player.get('person') or {}
                    
                    participant = PlayerMatchParticipant(
                        match_id=match.id,
                        person_id=person_data.get('externalID', ''),
                        team_id=team_id,
                        side_number=side.get('sideNumber', ''),
                        family_name=person_data.get('nativeFamilyName', ''),
                        given_name=person_data.get('nativeGivenName', ''),
                        is_winner=(side.get('sideNumber') == match_item.get('winningSide'))
                    )
                    session.add(participant)
                    participants_stored += 1
                except Exception as e:
                    print(f"Error storing participant: {e}")
                    continue
        
        return True, sets_stored, participants_stored
        
    except Exception as e:
        print(f"Error storing match: {e}")
        return False, 0, 0

def main():
    """Main function to run the truncate and repopulate process"""
    print("Starting player matches cleanup and repopulation process...")
    
    # Confirm with user before proceeding
    confirm = input("This will TRUNCATE all player matches tables and repopulate them. Continue? (y/n): ")
    if confirm.lower() != 'y':
        print("Operation cancelled.")
        return
    
    try:
        # Step 1: Truncate the tables
        truncate_player_matches_tables(DATABASE_URL)
        
        # Step 2: Repopulate with improved logic
        days_back = int(input("Enter number of days to look back for matches (default 90): ") or "90")
        repopulate_player_matches(DATABASE_URL, days_back)
        
        print("Process completed successfully!")
        
    except Exception as e:
        print(f"Error during process: {e}")
        print("Process failed.")

if __name__ == "__main__":
    main()
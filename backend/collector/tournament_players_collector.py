# collector/tournament_players_collector.py
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models.models import Base, TournamentPlayer, Tournament

class TournamentPlayersCollector:
    def __init__(self, database_url: str):
        """Initialize the tournament players collector with database connection"""
        self.database_url = database_url
        self.engine = create_engine(database_url)
        Base.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)
        
        # API configuration - GraphQL endpoint for tournament players
        self.api_url = "https://prd-itat-kube-tournaments.clubspark.pro/"
        
        # Headers similar to your other collectors
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Origin': 'https://www.collegetennis.com',
            'Referer': 'https://www.collegetennis.com/',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('tournament_players_updates.log'),
                logging.StreamHandler()
            ]
        )

    def create_players_query(self, tournament_id: str, limit: int = 0, offset: int = 0) -> Dict[str, Any]:
        """Create the GraphQL query payload for tournament players"""
        
        payload = {
            "operationName": "GetPlayers",
            "query": """
                query GetPlayers($id: UUID!, $queryParameters: QueryParametersPaged!) {
                    paginatedPublicTournamentRegistrations(
                        tournamentId: $id
                        queryParameters: $queryParameters
                    ) {
                        totalItems
                        items {
                            firstName: playerFirstName
                            gender: playerGender  
                            lastName: playerLastName
                            city: playerCity
                            state: playerState
                            playerName
                            playerId {
                                key
                                value
                                __typename
                            }
                            playerCustomIds {
                                key
                                value
                                __typename
                            }
                            eventEntries {
                                eventId
                                partnershipStatus
                                players {
                                    firstName
                                    lastName
                                    customId {
                                        key
                                        value
                                        __typename
                                    }
                                    customIds {
                                        key
                                        value
                                        __typename
                                    }
                                    __typename
                                }
                                __typename
                            }
                            events {
                                id
                                division {
                                    ballColour
                                    gender
                                    ageCategory {
                                        todsCode
                                        minimumAge
                                        maximumAge
                                        type
                                        __typename
                                    }
                                    eventType
                                    wheelchairRating
                                    familyType
                                    ratingCategory {
                                        ratingType
                                        ratingCategoryType
                                        value
                                        minimumValue
                                        maximumValue
                                        __typename
                                    }
                                    __typename
                                }
                                level {
                                    name
                                    category
                                    __typename
                                }
                                formatConfiguration {
                                    ballColour
                                    drawSize
                                    entriesLimit
                                    eventFormat
                                    scoreFormat
                                    selectionProcess
                                    __typename
                                }
                                __typename
                            }
                            __typename
                        }
                        __typename
                    }
                }
            """,
            "variables": {
                "id": tournament_id,
                "queryParameters": {
                    "limit": limit,
                    "offset": offset,
                    "sorts": [
                        {
                            "property": "playerLastName",
                            "sortDirection": "ASCENDING"
                        }
                    ],
                    "filters": []
                }
            }
        }
        
        return payload

    def fetch_tournament_players(self, tournament_id: str, limit: int = 0, offset: int = 0) -> Dict[str, Any]:
        """Fetch tournament players data from the GraphQL API"""
        try:
            payload = self.create_players_query(tournament_id, limit, offset)
            
            logging.info(f"Fetching players for tournament: {tournament_id}")
            
            response = requests.post(
                self.api_url,
                json=payload,
                headers=self.headers,
                timeout=30,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data and 'paginatedPublicTournamentRegistrations' in data['data']:
                    registrations_data = data['data']['paginatedPublicTournamentRegistrations']
                    total_items = registrations_data.get('totalItems', 0)
                    items = registrations_data.get('items', [])
                    
                    logging.info(f"Successfully fetched {len(items)} players out of {total_items} total for tournament {tournament_id}")
                    return data
                else:
                    logging.warning(f"No registration data found for tournament {tournament_id}")
                    return {}
            else:
                logging.error(f"API request failed with status {response.status_code}: {response.text}")
                return {}
                
        except Exception as e:
            logging.error(f"Error fetching tournament players: {str(e)}")
            return {}

    def extract_player_id(self, player_data: Dict[str, Any]) -> Optional[str]:
        """Extract the primary player ID from various ID sources"""
        
        # First try playerCustomIds for personId
        if 'playerCustomIds' in player_data and player_data['playerCustomIds']:
            for custom_id in player_data['playerCustomIds']:
                if custom_id.get('key') == 'personId':
                    return custom_id.get('value')
        
        # Fallback to playerId if available
        if 'playerId' in player_data and player_data['playerId']:
            return player_data['playerId'].get('value')
        
        return None

    def ensure_tournament_exists(self, session, tournament_id: str):
        """Ensure the tournament exists in the database before adding players"""
        existing_tournament = session.query(Tournament).filter_by(tournament_id=tournament_id).first()
        
        if not existing_tournament:
            logging.info(f"Tournament {tournament_id} not found in database, creating basic record...")
            
            # Create a minimal tournament record
            tournament = Tournament(
                tournament_id=tournament_id,
                name="Tournament (from players API)",  # Placeholder name
                is_cancelled=False,
                start_date_time=datetime.utcnow(),  # Placeholder date
                is_dual_match=False,
                tournament_type='TOURNAMENT',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(tournament)
            session.flush()  # Make sure it's available for the foreign key
            logging.info(f"Created basic tournament record for {tournament_id}")

    def store_tournament_players(self, tournament_id: str, players_data: Dict[str, Any]):
        """Store tournament players in the database"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        # Normalize tournament ID to lowercase to match database storage
        tournament_id = tournament_id.lower()
        
        session = self.Session()
        
        try:
            if 'data' not in players_data or 'paginatedPublicTournamentRegistrations' not in players_data['data']:
                logging.warning(f"No players data to store for tournament {tournament_id}")
                return
            
            registrations_data = players_data['data']['paginatedPublicTournamentRegistrations']
            items = registrations_data.get('items', [])
            
            stored_count = 0
            updated_count = 0
            
            for player_data in items:
                try:
                    # Extract player information
                    player_id = self.extract_player_id(player_data)
                    if not player_id:
                        logging.warning(f"No valid player ID found for player: {player_data.get('playerName', 'Unknown')}")
                        continue
                    
                    # Create unique registration ID
                    registration_id = f"{tournament_id}_{player_id}"
                    
                    # Extract event participation
                    events_participating = []
                    singles_event_id = None
                    doubles_event_id = None
                    player2_info = {}
                    
                    if 'events' in player_data:
                        for event in player_data['events']:
                            event_type = event.get('division', {}).get('eventType', '').upper()
                            if event_type == 'SINGLES':
                                events_participating.append('singles')
                                singles_event_id = event.get('id')
                            elif event_type == 'DOUBLES':
                                events_participating.append('doubles')
                                doubles_event_id = event.get('id')
                    
                    # Extract player 2 information for doubles
                    if 'eventEntries' in player_data:
                        for entry in player_data['eventEntries']:
                            if entry.get('partnershipStatus') and 'players' in entry:
                                # Find player 2 (the player that's not the current player)
                                for other_player in entry['players']:
                                    other_player_id = None
                                    if 'customIds' in other_player:
                                        for custom_id in other_player['customIds']:
                                            if custom_id.get('key') == 'personId':
                                                other_player_id = custom_id.get('value')
                                                break
                                    if not other_player_id and 'customId' in other_player:
                                        other_player_id = other_player['customId'].get('value')
                                    
                                    if other_player_id and other_player_id != player_id:
                                        player2_info = {
                                            'player2_id': other_player_id,
                                            'player2_first_name': other_player.get('firstName'),
                                            'player2_last_name': other_player.get('lastName')
                                        }
                                        break
                    
                    # Check if registration already exists
                    existing_registration = session.query(TournamentPlayer).filter_by(
                        id=registration_id
                    ).first()
                    
                    if existing_registration:
                        # Update existing registration
                        existing_registration.first_name = player_data.get('firstName')
                        existing_registration.last_name = player_data.get('lastName')
                        existing_registration.player_name = player_data.get('playerName')
                        existing_registration.gender = player_data.get('gender')
                        existing_registration.city = player_data.get('city')
                        existing_registration.state = player_data.get('state')
                        existing_registration.events_participating = ','.join(events_participating)
                        existing_registration.singles_event_id = singles_event_id
                        existing_registration.doubles_event_id = doubles_event_id
                        existing_registration.player2_id = player2_info.get('player2_id')
                        existing_registration.player2_first_name = player2_info.get('player2_first_name')
                        existing_registration.player2_last_name = player2_info.get('player2_last_name')
                        existing_registration.updated_at = datetime.utcnow()
                        
                        updated_count += 1
                        logging.debug(f"Updated registration for player: {player_data.get('playerName')}")
                    else:
                        # Create new registration
                        registration = TournamentPlayer(
                            id=registration_id,
                            tournament_id=tournament_id,
                            player_id=player_id,
                            first_name=player_data.get('firstName'),
                            last_name=player_data.get('lastName'),
                            player_name=player_data.get('playerName'),
                            gender=player_data.get('gender'),
                            city=player_data.get('city'),
                            state=player_data.get('state'),
                            events_participating=','.join(events_participating),
                            singles_event_id=singles_event_id,
                            doubles_event_id=doubles_event_id,
                            player2_id=player2_info.get('player2_id'),
                            player2_first_name=player2_info.get('player2_first_name'),
                            player2_last_name=player2_info.get('player2_last_name')
                        )
                        
                        session.add(registration)
                        stored_count += 1
                        logging.debug(f"Stored new registration for player: {player_data.get('playerName')}")
                
                except Exception as e:
                    logging.error(f"Error processing player data: {str(e)}")
                    continue
            
            # Commit all changes
            session.commit()
            logging.info(f"Tournament {tournament_id}: Stored {stored_count} new registrations, updated {updated_count} existing registrations")
            
        except Exception as e:
            logging.error(f"Error storing tournament players: {str(e)}")
            session.rollback()
            raise
        finally:
            session.close()

    def collect_players_for_tournament(self, tournament_id: str):
        """Collect all players for a specific tournament"""
        try:
            logging.info(f"Starting collection of players for tournament: {tournament_id}")
            
            # Fetch all players (limit=0 means get all)
            players_data = self.fetch_tournament_players(tournament_id, limit=0, offset=0)
            
            if players_data:
                self.store_tournament_players(tournament_id, players_data)
                logging.info(f"Successfully completed collection for tournament: {tournament_id}")
            else:
                logging.warning(f"No player data retrieved for tournament: {tournament_id}")
                
        except Exception as e:
            logging.error(f"Error collecting players for tournament {tournament_id}: {str(e)}")
            raise

    def collect_players_for_all_tournaments(self, from_date: Optional[str] = None, to_date: Optional[str] = None):
        """Collect players for all tournaments in the database within date range"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        session = self.Session()
        
        try:
            # Build query for tournaments
            query = session.query(Tournament.tournament_id, Tournament.name)
            
            # Add date filters if provided
            if from_date:
                from_datetime = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
                query = query.filter(Tournament.start_date_time >= from_datetime)
            
            if to_date:
                to_datetime = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
                query = query.filter(Tournament.start_date_time <= to_datetime)
            
            tournaments = query.all()
            
            logging.info(f"Found {len(tournaments)} tournaments to process")
            
            success_count = 0
            error_count = 0
            
            for tournament_id, tournament_name in tournaments:
                try:
                    logging.info(f"Processing tournament: {tournament_name} (ID: {tournament_id})")
                    self.collect_players_for_tournament(tournament_id)
                    success_count += 1
                    
                except Exception as e:
                    logging.error(f"Failed to process tournament {tournament_id}: {str(e)}")
                    error_count += 1
                    continue
            
            logging.info(f"Completed processing tournaments. Success: {success_count}, Errors: {error_count}")
            
        except Exception as e:
            logging.error(f"Error in collect_players_for_all_tournaments: {str(e)}")
            raise
        finally:
            session.close()


def main():
    """Main function for testing"""
    database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
    
    collector = TournamentPlayersCollector(database_url)
    
    # Test with the provided tournament ID
    test_tournament_id = "90B70C67-DA64-4F5F-A896-6626779327B7"
    collector.collect_players_for_tournament(test_tournament_id)


if __name__ == "__main__":
    main()
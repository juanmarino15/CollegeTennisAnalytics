# Updated tournament_draw_visualization_collector.py
# Modified to run for a specific tournament ID

import sys
import os
from pathlib import Path

# Add the parent directory to Python path so we can import models
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

import requests
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models.models import (
    Base, Tournament, TournamentEvent, TournamentDraw, 
    TournamentBracketPosition, PlayerMatch
)

class TournamentDrawVisualizationCollector:
    def __init__(self, database_url: str):
        """Initialize the tournament draw visualization collector"""
        self.database_url = database_url
        self.engine = create_engine(database_url)
        Base.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)
        
        # API configuration
        self.api_url = "https://prd-itat-kube-tournamentevent-api.clubspark.pro/"
        
        # Headers for API requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Origin': 'https://www.collegetennis.com',
            'Referer': 'https://www.collegetennis.com/',
        }
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('tournament_draw_visualization.log'),
                logging.StreamHandler()
            ]
        )

    def extract_seed_data_from_positions(self, draw_data: Dict[str, Any]) -> Dict[int, int]:
        """
        Extract seed numbers from position assignments
        Returns a dict mapping draw_position -> seed_number
        """
        position_to_seed = {}
        
        # Get position assignments from structures
        structures = draw_data.get('structures', [])
        for structure in structures:
            position_assignments = structure.get('positionAssignments', [])
            for assignment in position_assignments:
                draw_position = assignment.get('drawPosition')
                seed_number = assignment.get('seedNumber')
                seed_value = assignment.get('seedValue')
                
                if draw_position and (seed_number or seed_value):
                    # Use seedNumber if available, otherwise try to parse seedValue
                    if seed_number:
                        position_to_seed[draw_position] = seed_number
                    elif seed_value and str(seed_value).isdigit():
                        position_to_seed[draw_position] = int(seed_value)
        
        logging.info(f"Extracted seed data for {len(position_to_seed)} positions")
        return position_to_seed

    def create_draws_query(self, event_id: str, tournament_id: str) -> Dict[str, Any]:
        """Create the GraphQL query payload for tournament draws data"""
        
        # API expects uppercase IDs, but we store lowercase internally
        event_id_for_api = event_id.upper() if event_id else ""
        tournament_id_for_api = tournament_id.upper() if tournament_id else ""
        
        # Debug logging to see what we're sending
        logging.info(f"API Request - Original IDs: tournament={tournament_id}, event={event_id}")
        logging.info(f"API Request - Uppercase IDs: tournament={tournament_id_for_api}, event={event_id_for_api}")
        
        payload = {
            "operationName": "TournamentPublicEventData",
            "query": """
                query TournamentPublicEventData($eventId: ID!, $tournamentId: ID!) {
                    tournamentPublicEventData(eventId: $eventId, tournamentId: $tournamentId)
                }
            """,
            "variables": {
                "eventId": event_id_for_api,
                "tournamentId": tournament_id_for_api
            }
        }
        
        return payload

    def fetch_draw_visualization_data(self, tournament_id: str, event_id: str) -> Dict[str, Any]:
        """Fetch tournament draw visualization data from the API"""
        
        # Log what we're about to fetch
        logging.info(f"Fetching draw visualization data for tournament: {tournament_id}, event: {event_id} (sending UPPERCASE to API)")
        
        try:
            payload = self.create_draws_query(event_id, tournament_id)
            
            response = requests.post(
                self.api_url,
                json=payload,
                headers=self.headers,
                verify=False,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data and 'tournamentPublicEventData' in data['data']:
                    event_data = data['data']['tournamentPublicEventData']
                    
                    # Handle the case where the API returns a JSON string
                    if isinstance(event_data, str):
                        try:
                            # Parse the JSON string
                            event_data = json.loads(event_data)
                        except json.JSONDecodeError as e:
                            logging.error(f"Failed to parse JSON string from API: {str(e)}")
                            return {}
                    
                    # Check if we have the expected structure
                    if isinstance(event_data, dict) and 'eventData' in event_data:
                        event_data_inner = event_data['eventData']
                        
                        # Extract the data we need for visualization
                        draws_data = event_data_inner.get('drawsData', [])
                        # Participants are at the top level of event_data, not inside eventData
                        participants_data = event_data.get('participants', [])
                        tournament_info = event_data_inner.get('tournamentInfo', {})
                        event_info = event_data_inner.get('eventInfo', {})
                        
                        logging.info(f"Successfully fetched {len(draws_data)} draws and {len(participants_data)} participants")
                        
                        return {
                            'drawsData': draws_data,
                            'participants': participants_data,
                            'tournamentInfo': tournament_info,
                            'eventInfo': event_info
                        }
                    else:
                        logging.warning(f"Unexpected event data structure: {type(event_data)}")
                        if isinstance(event_data, dict):
                            logging.info(f"Available keys: {list(event_data.keys())}")
                        return {}
                else:
                    logging.warning(f"No event data found for tournament {tournament_id}, event {event_id}")
                    return {}
            else:
                logging.error(f"API request failed with status {response.status_code}: {response.text}")
                return {}
                
        except Exception as e:
            logging.error(f"Error fetching draw visualization data: {str(e)}")
            return {}

    def extract_draw_info(self, draw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract draw information from API response"""
        draw_info = {
            'draw_id': draw_data.get('drawId', '').lower(),
            'draw_name': draw_data.get('drawName', ''),
            'draw_type': draw_data.get('drawType', ''),
            'draw_size': draw_data.get('drawSize', 0),
            'draw_active': draw_data.get('drawActive', True),
            'draw_completed': draw_data.get('drawCompleted', False),
            'updated_at': draw_data.get('updatedAt')
        }
        
        # Extract position assignments from structures (this is where they actually are in the API)
        structures = draw_data.get('structures', [])
        all_position_assignments = []
        
        for structure in structures:
            structure_positions = structure.get('positionAssignments', [])
            all_position_assignments.extend(structure_positions)
        
        draw_info['position_assignments'] = all_position_assignments
        
        # Determine event type and gender from draw name
        draw_name_lower = draw_info['draw_name'].lower()
        
        if 'doubles' in draw_name_lower:
            draw_info['event_type'] = 'DOUBLES'
        else:
            draw_info['event_type'] = 'SINGLES'
            
        if 'men' in draw_name_lower or 'male' in draw_name_lower:
            draw_info['gender'] = 'MALE'
        elif 'women' in draw_name_lower or 'female' in draw_name_lower:
            draw_info['gender'] = 'FEMALE'
        elif 'mixed' in draw_name_lower:
            draw_info['gender'] = 'MIXED'
        else:
            draw_info['gender'] = 'UNKNOWN'
        
        return draw_info

    def extract_participant_info(self, participant_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract participant information from API response"""
        # Based on the API response format
        participant_info = {
            'participant_id': participant_data.get('participantId', '').lower() if participant_data.get('participantId') else None,
            'participant_name': participant_data.get('participantName', ''),
            'participant_type': participant_data.get('participantType', 'INDIVIDUAL'),
            'teams': participant_data.get('teams', []),
            'players': participant_data.get('players', [])
        }
        
        # Extract team name if available
        teams_data = participant_info['teams']
        if teams_data and len(teams_data) > 0:
            participant_info['team_name'] = teams_data[0].get('participantOtherName')
        else:
            participant_info['team_name'] = None
            
        return participant_info

    def find_matching_player_match(self, session, tournament_id: str, participant_ids: List[str], round_name: str = None) -> Optional[int]:
        """Find matching PlayerMatch in existing system"""
        try:
            # Standardize tournament_id to lowercase for comparison
            tournament_id = tournament_id.lower() if tournament_id else ""
            
            # Standardize participant_ids to lowercase for comparison
            participant_ids = [pid.lower() if pid else "" for pid in participant_ids]
            
            # Look for player matches that match this tournament and participants
            # This links the bracket position to existing match data
            
            # Query for matches with these participants in this tournament
            for participant_id in participant_ids:
                if not participant_id:
                    continue
                    
                player_matches = session.query(PlayerMatch).filter(
                    PlayerMatch.tournament_id == tournament_id
                ).all()
                
                if round_name:
                    player_matches = [m for m in player_matches if m.round_name == round_name]
                
                for match in player_matches:
                    # Return the first match found - you might want better matching logic here
                    # Consider adding participant ID matching logic here
                    return match.id
            
            return None
            
        except Exception as e:
            logging.warning(f"Could not find matching player match: {str(e)}")
            return None

    def store_draw_visualization_data(self, tournament_id: str, event_id: str, draws_data: Dict[str, Any]):
        """Store tournament draw visualization data in the database"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        # Standardize IDs to lowercase for consistency
        tournament_id = tournament_id.lower() if tournament_id else ""
        event_id = event_id.lower() if event_id else ""
        
        session = self.Session()
        
        try:
            draws_list = draws_data.get('drawsData', [])
            participants_list = draws_data.get('participants', [])
            
            if not draws_list:
                logging.warning(f"No draws data to store for tournament {tournament_id}, event {event_id}")
                return
            
            logging.info(f"Processing {len(draws_list)} draws and {len(participants_list)} participants")
            
            stored_draws = 0
            stored_positions = 0
            
            # Create a lookup for participants
            participants_lookup = {}
            for participant in participants_list:
                participant_info = self.extract_participant_info(participant)
                participant_id = participant_info['participant_id']
                if participant_id:
                    participants_lookup[participant_id] = participant_info
            
            logging.info(f"Created participant lookup with {len(participants_lookup)} participants")
            
            # Process each draw
            for idx, draw_data in enumerate(draws_list):
                logging.info(f"Processing draw {idx + 1}/{len(draws_list)}: {draw_data.get('drawName', 'Unknown')}")
                
                try:
                    draw_info = self.extract_draw_info(draw_data)
                    draw_id = draw_info['draw_id']
                    
                    logging.info(f"Draw {idx + 1} ID: {draw_id}")
                    
                    if not draw_id:
                        logging.warning(f"Draw {idx + 1} has no ID, skipping")
                        continue
                    
                    # Extract seed number mapping
                    position_to_seed = self.extract_seed_data_from_positions(draw_data)
                    logging.info(f"Seed mapping extracted: {len(position_to_seed)} positions, sample: {dict(list(position_to_seed.items())[:5])}")
                    
                    # Check if draw already exists
                    existing_draw = session.query(TournamentDraw).filter_by(draw_id=draw_id).first()
                    
                    if existing_draw:
                        logging.info(f"Updating existing draw: {draw_id}")
                        # Update existing draw
                        existing_draw.tournament_id = tournament_id
                        existing_draw.event_id = event_id
                        existing_draw.draw_name = draw_info['draw_name']
                        existing_draw.draw_type = draw_info['draw_type']
                        existing_draw.draw_size = draw_info['draw_size']
                        existing_draw.event_type = draw_info['event_type']
                        existing_draw.gender = draw_info['gender']
                        existing_draw.draw_completed = draw_info['draw_completed']
                        existing_draw.draw_active = draw_info['draw_active']
                        existing_draw.updated_at_api = datetime.fromisoformat(
                            draw_info['updated_at'].replace('Z', '+00:00')
                        ) if draw_info['updated_at'] else None
                        existing_draw.updated_at = datetime.utcnow()
                    else:
                        logging.info(f"Creating new draw: {draw_id}")
                        # Create new draw
                        draw = TournamentDraw(
                            draw_id=draw_id,
                            tournament_id=tournament_id,
                            event_id=event_id,
                            draw_name=draw_info['draw_name'],
                            draw_type=draw_info['draw_type'],
                            draw_size=draw_info['draw_size'],
                            event_type=draw_info['event_type'],
                            gender=draw_info['gender'],
                            draw_completed=draw_info['draw_completed'],
                            draw_active=draw_info['draw_active'],
                            updated_at_api=datetime.fromisoformat(
                                draw_info['updated_at'].replace('Z', '+00:00')
                            ) if draw_info['updated_at'] else None
                        )
                        session.add(draw)
                        stored_draws += 1
                    
                    session.flush()
                    
                    # Clear existing bracket positions for this draw
                    deleted_positions = session.query(TournamentBracketPosition).filter_by(draw_id=draw_id).delete()
                    logging.info(f"Cleared {deleted_positions} existing bracket positions for draw {draw_id}")
                    
                    # Store bracket positions for visualization
                    position_assignments = draw_info['position_assignments']
                    logging.info(f"Draw {draw_info['draw_name']} has {len(position_assignments)} position assignments")
                    
                    if len(position_assignments) > 0:
                        logging.info(f"Sample position assignment: {position_assignments[0] if position_assignments else 'None'}")
                    
                    for position in position_assignments:
                        participant_id_raw = position.get('participantId')
                        if not participant_id_raw:
                            logging.warning(f"Position assignment missing participantId: {position}")
                            continue
                            
                        # Standardize participant ID to lowercase for lookup
                        participant_id = participant_id_raw.lower()
                        draw_position = position.get('drawPosition')
                        
                        # Get seed number
                        seed_number = position_to_seed.get(draw_position)
                        
                        logging.info(f"Processing position {draw_position} for participant {participant_id} - Seed: {seed_number} (from mappings: seed={draw_position in position_to_seed})")
                        
                        if participant_id in participants_lookup:
                            participant_info = participants_lookup[participant_id]
                            
                            # Try to find matching player match from existing system
                            player_match_id = self.find_matching_player_match(
                                session, tournament_id, [participant_id]
                            )
                            
                            bracket_position = TournamentBracketPosition(
                                draw_id=draw_id,
                                draw_position=draw_position,
                                participant_id=participant_id,  # Store as lowercase
                                participant_name=participant_info['participant_name'],
                                participant_type=participant_info['participant_type'],
                                team_name=participant_info['team_name'],
                                seed_number=seed_number,
                                player_match_id=player_match_id
                            )
                            session.add(bracket_position)
                            stored_positions += 1
                            logging.info(f"Added bracket position for {participant_info['participant_name']} - Seed {seed_number}")
                        else:
                            logging.warning(f"Participant {participant_id} not found in participants lookup")
                
                except Exception as e:
                    logging.error(f"Error processing draw {draw_data.get('drawId', 'unknown')}: {str(e)}")
                    continue
            
            session.commit()
            logging.info(f"Stored {stored_draws} draws and {stored_positions} bracket positions for tournament {tournament_id}")
            
        except Exception as e:
            session.rollback()
            logging.error(f"Error storing draw visualization data: {str(e)}")
            raise
        finally:
            session.close()

    def collect_draws_for_tournament_events(self, tournament_id: str):
        """Collect draws for all events in a tournament"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        # Standardize tournament_id to lowercase for database operations
        tournament_id_lowercase = tournament_id.lower() if tournament_id else ""
        
        session = self.Session()
        
        try:
            # Get events for this tournament
            events = session.query(TournamentEvent).filter_by(
                tournament_id=tournament_id_lowercase
            ).all()
            
            if not events:
                logging.warning(f"No events found for tournament {tournament_id_lowercase}")
                return
                
            logging.info(f"Found {len(events)} events for tournament {tournament_id_lowercase}")
            
            for event in events:
                try:
                    # Fetch and store draw data for this event
                    draws_data = self.fetch_draw_visualization_data(
                        tournament_id_lowercase, 
                        event.event_id
                    )
                    
                    if draws_data:
                        self.store_draw_visualization_data(
                            tournament_id_lowercase,
                            event.event_id,
                            draws_data
                        )
                    else:
                        logging.warning(f"No draw data found for event {event.event_id}")
                        
                except Exception as e:
                    logging.error(f"Error processing event {event.event_id}: {str(e)}")
                    continue
        
        except Exception as e:
            logging.error(f"Error collecting draws for tournament {tournament_id}: {str(e)}")
            raise
        finally:
            session.close()

    def run_for_specific_tournament(self, tournament_id: str):
        """Run the collector for a specific tournament ID"""
        logging.info(f"Starting draw collection for specific tournament: {tournament_id}")
        
        try:
            self.collect_draws_for_tournament_events(tournament_id)
            logging.info(f"Successfully completed draw collection for tournament: {tournament_id}")
        except Exception as e:
            logging.error(f"Failed to collect draws for tournament {tournament_id}: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    # Initialize collector
    database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
    collector = TournamentDrawVisualizationCollector(database_url)
    
    # Run for the specific tournament
    tournament_id = "92BC5EA2-B793-4E41-8252-9838A350538E"
    collector.run_for_specific_tournament(tournament_id)
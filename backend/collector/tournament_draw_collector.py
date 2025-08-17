# tournament_draw_collector.py - Updated to process all tournament_event combinations
import sys
import os
from pathlib import Path

# Add the parent directory to Python path so we can import models
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

import requests
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from models.models import Base, TournamentEvent, Tournament

def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('tournament_draw_collector.log'),
            logging.StreamHandler()
        ]
    )

class StandaloneTournamentCollector:
    def __init__(self, database_url: str, dry_run: bool = False):
        """Initialize the tournament draw collector"""
        self.database_url = database_url
        self.dry_run = dry_run
        self.engine = create_engine(database_url)
        Base.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)
        
        # API configuration - using the working endpoint from your original code
        self.api_url = "https://prd-itat-kube-tournamentevent-api.clubspark.pro/"
        
        # Headers for API requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Origin': 'https://www.collegetennis.com',
            'Referer': 'https://www.collegetennis.com/',
        }

    def get_all_tournament_events(self) -> List[tuple]:
        """Get all tournament_id and event_id combinations from tournament_events table"""
        session = self.Session()
        try:
            # Query all tournament events - uppercase both IDs for consistency
            tournament_events = session.query(
                TournamentEvent.tournament_id,
                TournamentEvent.event_id,
                TournamentEvent.gender,
                TournamentEvent.event_type
            ).all()
            
            # Convert IDs to uppercase
            tournament_events_upper = [
                (tournament_id.upper() if tournament_id else None,
                 event_id.upper() if event_id else None,
                 gender, event_type)
                for tournament_id, event_id, gender, event_type in tournament_events
            ]
            
            logging.info(f"Found {len(tournament_events_upper)} tournament/event combinations to process")
            return tournament_events_upper
            
        except Exception as e:
            logging.error(f"Error querying tournament events: {str(e)}")
            return []
        finally:
            session.close()

    def get_tournament_events_for_backfill(self, limit: Optional[int] = None, 
                                         only_recent: bool = False,
                                         days_back: int = 30) -> List[tuple]:
        """
        Get tournament events for backfilling the new tables
        
        Args:
            limit: Maximum number of events to process (None for all)
            only_recent: If True, only get events from recent tournaments
            days_back: How many days back to consider for recent tournaments
        """
        session = self.Session()
        try:
            query = session.query(
                TournamentEvent.tournament_id,
                TournamentEvent.event_id,
                TournamentEvent.gender,
                TournamentEvent.event_type,
                Tournament.name.label('tournament_name'),
                Tournament.start_date_time
            ).join(
                Tournament, TournamentEvent.tournament_id == Tournament.tournament_id
            )
            
            # Filter for recent tournaments if requested
            if only_recent:
                cutoff_date = datetime.now() - timedelta(days=days_back)
                query = query.filter(Tournament.start_date_time >= cutoff_date)
            
            # Order by tournament start date (most recent first)
            query = query.order_by(Tournament.start_date_time.desc())
            
            # Apply limit if specified
            if limit:
                query = query.limit(limit)
            
            tournament_events = query.all()
            
            # Convert IDs to uppercase
            tournament_events_upper = [
                (tournament_id.upper() if tournament_id else None,
                 event_id.upper() if event_id else None,
                 gender, event_type, tournament_name, start_date)
                for tournament_id, event_id, gender, event_type, tournament_name, start_date in tournament_events
            ]
            
            logging.info(f"Found {len(tournament_events_upper)} tournament/event combinations for backfill")
            if only_recent:
                logging.info(f"Filtered to tournaments from last {days_back} days")
            if limit:
                logging.info(f"Limited to {limit} events")
                
            return tournament_events_upper
            
        except Exception as e:
            logging.error(f"Error querying tournament events for backfill: {str(e)}")
            return []
        finally:
            session.close()

    def create_tables_if_not_exist(self):
        """Create the simplified tournament tables if they don't exist"""
        if self.dry_run:
            logging.info("DRY RUN: Would create tournament tables")
            return
            
        try:
            with self.engine.connect() as conn:
                # Create tournament_draws table
                draws_sql = text("""
                    CREATE TABLE IF NOT EXISTS tournament_draws (
                        draw_id VARCHAR PRIMARY KEY,
                        tournament_id VARCHAR,
                        event_id VARCHAR,
                        draw_name VARCHAR,
                        draw_type VARCHAR,
                        draw_size INTEGER,
                        draw_active BOOLEAN,
                        draw_completed BOOLEAN,
                        event_type VARCHAR,
                        gender VARCHAR,
                        match_up_format VARCHAR,
                        updated_at_api TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                conn.execute(draws_sql)
                conn.commit()
                
                # Create tournament_matches table with all participant and school data
                matches_sql = text("""
                    CREATE TABLE IF NOT EXISTS tournament_matches (
                        id SERIAL PRIMARY KEY,
                        match_up_id VARCHAR NOT NULL UNIQUE,
                        draw_id VARCHAR REFERENCES tournament_draws(draw_id),
                        tournament_id VARCHAR,
                        event_id VARCHAR,
                        round_name VARCHAR,
                        round_number INTEGER,
                        round_position INTEGER,
                        match_type VARCHAR,
                        match_format VARCHAR,
                        match_status VARCHAR,
                        stage VARCHAR,
                        structure_name VARCHAR,
                        
                        -- Side 1 complete data
                        side1_participant_id VARCHAR,
                        side1_participant_name VARCHAR,
                        side1_draw_position INTEGER,
                        side1_seed_number INTEGER,
                        side1_school_name VARCHAR,
                        side1_school_id VARCHAR,
                        side1_player1_id VARCHAR,
                        side1_player1_name VARCHAR,
                        side1_player2_id VARCHAR,
                        side1_player2_name VARCHAR,
                        
                        -- Side 2 complete data
                        side2_participant_id VARCHAR,
                        side2_participant_name VARCHAR,
                        side2_draw_position INTEGER,
                        side2_seed_number INTEGER,
                        side2_school_name VARCHAR,
                        side2_school_id VARCHAR,
                        side2_player1_id VARCHAR,
                        side2_player1_name VARCHAR,
                        side2_player2_id VARCHAR,
                        side2_player2_name VARCHAR,
                        
                        -- Match outcome
                        winning_side INTEGER,
                        winner_participant_id VARCHAR,
                        winner_participant_name VARCHAR,
                        
                        -- Scores
                        score_side1 VARCHAR,
                        score_side2 VARCHAR,
                        
                        -- Scheduling
                        scheduled_date DATE,
                        scheduled_time TIME,
                        venue_name VARCHAR,
                        
                        -- API timestamps
                        created_at_api TIMESTAMP,
                        updated_at_api TIMESTAMP,
                        
                        -- Timestamps
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                conn.execute(matches_sql)
                conn.commit()
                
                logging.info("Tournament tables created successfully")
        
        except Exception as e:
            logging.error(f"Error creating tournament tables: {str(e)}")
            raise

    def create_api_query(self, tournament_id: str, event_id: str) -> Dict[str, Any]:
        """Create the GraphQL query payload"""
        event_id_for_api = event_id.upper() if event_id else ""
        tournament_id_for_api = tournament_id.upper() if tournament_id else ""
        
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

    def fetch_tournament_data(self, tournament_id: str, event_id: str) -> Optional[Dict[str, Any]]:
        """Fetch tournament event data from API using GraphQL"""
        # Ensure IDs are uppercase for API requests
        tournament_id_upper = tournament_id.upper()
        event_id_upper = event_id.upper()
        
        logging.info(f"🌐 Fetching tournament data for tournament: {tournament_id_upper}, event: {event_id_upper}")
        
        try:
            if self.dry_run:
                logging.info(f"DRY RUN: Would fetch GraphQL data for {tournament_id_upper}/{event_id_upper}")
                return {"eventData": {"drawsData": []}}
            
            payload = self.create_api_query(tournament_id_upper, event_id_upper)
            
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
                    
                    if isinstance(event_data, str):
                        try:
                            event_data = json.loads(event_data)
                        except json.JSONDecodeError as e:
                            logging.error(f"Failed to parse JSON string: {str(e)}")
                            return None
                    
                    logging.info(f"✅ Successfully fetched event data for {tournament_id_upper}/{event_id_upper}")
                    return event_data
                else:
                    logging.warning("No event data found in response")
                    return None
            else:
                logging.error(f"API request failed with status {response.status_code}")
                return None
                
        except Exception as e:
            logging.error(f"❌ Error fetching tournament data: {str(e)}")
            return None

    def build_participants_lookup(self, event_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Build lookup dictionary for participants with uppercase keys"""
        participants_lookup = {}
        individual_participants = {}
        
        participants = event_data.get('participants', [])
        
        # First pass: collect all individual participants
        for participant in participants:
            participant_id = participant.get('participantId', '').upper()
            participant_name = participant.get('participantName', 'Unknown Player')
            participant_type = participant.get('participantType', 'INDIVIDUAL')
            
            if not participant_id:
                continue
            
            # Extract school/team information
            teams = participant.get('teams', [])
            school_name = None
            school_id = None
            
            if teams and len(teams) > 0:
                team = teams[0]  # Take first team
                school_name = team.get('participantOtherName') or team.get('participantName')
                school_id = team.get('teamId') or team.get('participantId')
                if school_id:
                    school_id = school_id.upper()
            
            if participant_type == 'INDIVIDUAL':
                individual_participants[participant_id] = {
                    'name': participant_name,
                    'school_name': school_name,
                    'school_id': school_id
                }
                participants_lookup[participant_id] = {
                    'name': participant_name,
                    'type': 'INDIVIDUAL',
                    'individual_ids': [participant_id],
                    'individual_names': [participant_name],
                    'school_name': school_name,
                    'school_id': school_id
                }
            
            elif participant_type == 'PAIR':
                individual_ids = participant.get('individualParticipantIds', [])
                individual_ids = [pid.upper() for pid in individual_ids]
                
                # Get individual names and school info for the pair
                individual_names = []
                pair_school_name = school_name  # Pair's school (if any)
                pair_school_id = school_id
                
                for ind_id in individual_ids:
                    # Look for the individual in the current participants list
                    ind_name = None
                    ind_school_name = None
                    ind_school_id = None
                    
                    for p in participants:
                        if p.get('participantId', '').upper() == ind_id:
                            ind_name = p.get('participantName', 'Unknown Player')
                            
                            # Get school info from individual participant
                            ind_teams = p.get('teams', [])
                            if ind_teams and len(ind_teams) > 0:
                                ind_team = ind_teams[0]
                                ind_school_name = ind_team.get('participantOtherName') or ind_team.get('participantName')
                                ind_school_id = ind_team.get('teamId') or ind_team.get('participantId')
                                if ind_school_id:
                                    ind_school_id = ind_school_id.upper()
                            break
                    
                    if not ind_name:
                        # Fallback to individual_participants if already processed
                        ind_info = individual_participants.get(ind_id, {})
                        ind_name = ind_info.get('name', f'Player_{ind_id[:8]}')
                        ind_school_name = ind_info.get('school_name')
                        ind_school_id = ind_info.get('school_id')
                    
                    individual_names.append(ind_name)
                    
                    # Use individual's school info if pair doesn't have school info
                    if not pair_school_name and ind_school_name:
                        pair_school_name = ind_school_name
                        pair_school_id = ind_school_id
                
                participants_lookup[participant_id] = {
                    'name': participant_name,  # e.g., "Main/Tsai"
                    'type': 'PAIR',
                    'individual_ids': individual_ids,
                    'individual_names': individual_names,
                    'school_name': pair_school_name,
                    'school_id': pair_school_id
                }
        
        logging.info(f"📋 Built participants lookup with {len(participants_lookup)} participants")
        return participants_lookup

    def extract_draw_info_from_working_api(self, draw_data: Dict[str, Any], tournament_id: str, event_id: str) -> Dict[str, Any]:
        """Extract draw information for tournament_draws table from working API format"""
        draw_info = {
            'draw_id': draw_data.get('drawId', '').upper(),
            'tournament_id': tournament_id.upper(),
            'event_id': event_id.upper(),
            'draw_name': draw_data.get('drawName', ''),
            'draw_type': draw_data.get('drawType', ''),
            'draw_size': len(draw_data.get('structures', [{}])[0].get('positionAssignments', [])),
            'draw_active': draw_data.get('drawActive', True),
            'draw_completed': draw_data.get('drawCompleted', False),
            'updated_at_api': draw_data.get('updatedAt'),
            'match_up_format': draw_data.get('matchUpFormat', '')
        }
        
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

    def extract_tournament_match_from_working_api(self, match_data: Dict[str, Any], participants_lookup: Dict[str, Any], tournament_id: str, event_id: str) -> Dict[str, Any]:
        """Extract match information for tournament_matches table from working API format"""
        
        # Extract basic match info
        match_info = {
            'match_up_id': match_data.get('matchUpId', '').upper(),
            'draw_id': match_data.get('drawId', '').upper(),
            'tournament_id': tournament_id.upper(),
            'event_id': event_id.upper(),
            'round_name': match_data.get('roundName', ''),
            'round_number': match_data.get('roundNumber', 0),
            'round_position': match_data.get('roundPosition', 0),
            'match_type': match_data.get('matchUpType', ''),
            'match_format': match_data.get('matchUpFormat', ''),
            'match_status': match_data.get('matchUpStatus', ''),
            'stage': match_data.get('stage', ''),
            'structure_name': match_data.get('structureName', ''),
            'winning_side': match_data.get('winningSide'),
            'winner_participant_id': None,
            'winner_participant_name': None,
            'scheduled_date': match_data.get('schedule', {}).get('scheduledDate'),
            'scheduled_time': match_data.get('schedule', {}).get('scheduledTime'),
            'venue_name': match_data.get('schedule', {}).get('venueName'),
            'score_side1': match_data.get('score', {}).get('scoreStringSide1', ''),
            'score_side2': match_data.get('score', {}).get('scoreStringSide2', ''),
            'created_at_api': match_data.get('createdAt'),
            'updated_at_api': match_data.get('updatedAt'),
            
            # Initialize all participant fields
            'side1_participant_id': None,
            'side1_participant_name': None,
            'side1_draw_position': None,
            'side1_seed_number': None,
            'side1_school_name': None,
            'side1_school_id': None,
            'side1_player1_id': None,
            'side1_player1_name': None,
            'side1_player2_id': None,
            'side1_player2_name': None,
            'side2_participant_id': None,
            'side2_participant_name': None,
            'side2_draw_position': None,
            'side2_seed_number': None,
            'side2_school_name': None,
            'side2_school_id': None,
            'side2_player1_id': None,
            'side2_player1_name': None,
            'side2_player2_id': None,
            'side2_player2_name': None
        }
        
        # Extract participant information from sides
        sides = match_data.get('sides', [])
        
        for side in sides:
            side_number = side.get('sideNumber')
            participant_id = side.get('participantId', '').upper()
            draw_position = side.get('drawPosition')
            seed_number = side.get('seedNumber')
            
            if not participant_id:
                continue
                
            participant_info = participants_lookup.get(participant_id, {
                'name': 'Unknown Player',
                'type': 'INDIVIDUAL',
                'individual_ids': [participant_id],
                'individual_names': ['Unknown Player'],
                'school_name': None,
                'school_id': None
            })
            
            participant_name = participant_info['name']
            participant_type = participant_info['type']
            individual_ids = participant_info['individual_ids']
            individual_names = participant_info['individual_names']
            school_name = participant_info['school_name']
            school_id = participant_info['school_id']
            
            if side_number == 1:
                match_info.update({
                    'side1_participant_id': participant_id,
                    'side1_participant_name': participant_name,
                    'side1_draw_position': draw_position,
                    'side1_seed_number': seed_number,
                    'side1_school_name': school_name,
                    'side1_school_id': school_id
                })
                
                # Add individual player information
                if len(individual_ids) >= 1:
                    match_info['side1_player1_id'] = individual_ids[0]
                    match_info['side1_player1_name'] = individual_names[0] if len(individual_names) >= 1 else 'Unknown'
                
                if len(individual_ids) >= 2:
                    match_info['side1_player2_id'] = individual_ids[1]
                    match_info['side1_player2_name'] = individual_names[1] if len(individual_names) >= 2 else 'Unknown'
                
            elif side_number == 2:
                match_info.update({
                    'side2_participant_id': participant_id,
                    'side2_participant_name': participant_name,
                    'side2_draw_position': draw_position,
                    'side2_seed_number': seed_number,
                    'side2_school_name': school_name,
                    'side2_school_id': school_id
                })
                
                # Add individual player information
                if len(individual_ids) >= 1:
                    match_info['side2_player1_id'] = individual_ids[0]
                    match_info['side2_player1_name'] = individual_names[0] if len(individual_names) >= 1 else 'Unknown'
                
                if len(individual_ids) >= 2:
                    match_info['side2_player2_id'] = individual_ids[1]
                    match_info['side2_player2_name'] = individual_names[1] if len(individual_names) >= 2 else 'Unknown'
        
        # Set winner information based on winning_side
        winning_side = match_info.get('winning_side')
        if winning_side == 1:
            match_info['winner_participant_id'] = match_info['side1_participant_id']
            match_info['winner_participant_name'] = match_info['side1_participant_name']
        elif winning_side == 2:
            match_info['winner_participant_id'] = match_info['side2_participant_id']
            match_info['winner_participant_name'] = match_info['side2_participant_name']
        
        return match_info

    def store_draw_data(self, draw_info: Dict[str, Any]):
        """Store or update draw information"""
        if self.dry_run:
            logging.info(f"DRY RUN: Would store draw {draw_info['draw_id']}")
            return
            
        session = self.Session()
        try:
            # Check if draw already exists
            existing_draw = session.execute(
                text("SELECT draw_id FROM tournament_draws WHERE draw_id = :draw_id"),
                {"draw_id": draw_info['draw_id']}
            ).fetchone()
            
            if existing_draw:
                # Update existing draw
                update_sql = text("""
                    UPDATE tournament_draws SET
                        tournament_id = :tournament_id,
                        event_id = :event_id,
                        draw_name = :draw_name,
                        draw_type = :draw_type,
                        draw_size = :draw_size,
                        draw_active = :draw_active,
                        draw_completed = :draw_completed,
                        event_type = :event_type,
                        gender = :gender,
                        match_up_format = :match_up_format,
                        updated_at_api = :updated_at_api,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE draw_id = :draw_id
                """)
                session.execute(update_sql, draw_info)
                logging.info(f"Updated existing draw: {draw_info['draw_id']}")
            else:
                # Insert new draw
                insert_sql = text("""
                    INSERT INTO tournament_draws (
                        draw_id, tournament_id, event_id, draw_name, draw_type, 
                        draw_size, draw_active, draw_completed, event_type, gender,
                        match_up_format, updated_at_api
                    ) VALUES (
                        :draw_id, :tournament_id, :event_id, :draw_name, :draw_type,
                        :draw_size, :draw_active, :draw_completed, :event_type, :gender,
                        :match_up_format, :updated_at_api
                    )
                """)
                session.execute(insert_sql, draw_info)
                logging.info(f"Inserted new draw: {draw_info['draw_id']}")
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logging.error(f"Error storing draw data: {str(e)}")
            raise
        finally:
            session.close()

    def store_match_data(self, match_info: Dict[str, Any]):
        """Store or update match information"""
        if self.dry_run:
            logging.info(f"DRY RUN: Would store match {match_info['match_up_id']}")
            return
            
        session = self.Session()
        try:
            # Check if match already exists
            existing_match = session.execute(
                text("SELECT match_up_id FROM tournament_matches WHERE match_up_id = :match_up_id"),
                {"match_up_id": match_info['match_up_id']}
            ).fetchone()
            
            if existing_match:
                # Update existing match (complete field list)
                update_sql = text("""
                    UPDATE tournament_matches SET
                        draw_id = :draw_id,
                        tournament_id = :tournament_id,
                        event_id = :event_id,
                        round_name = :round_name,
                        round_number = :round_number,
                        round_position = :round_position,
                        match_type = :match_type,
                        match_format = :match_format,
                        match_status = :match_status,
                        stage = :stage,
                        structure_name = :structure_name,
                        side1_participant_id = :side1_participant_id,
                        side1_participant_name = :side1_participant_name,
                        side1_draw_position = :side1_draw_position,
                        side1_seed_number = :side1_seed_number,
                        side1_school_name = :side1_school_name,
                        side1_school_id = :side1_school_id,
                        side1_player1_id = :side1_player1_id,
                        side1_player1_name = :side1_player1_name,
                        side1_player2_id = :side1_player2_id,
                        side1_player2_name = :side1_player2_name,
                        side2_participant_id = :side2_participant_id,
                        side2_participant_name = :side2_participant_name,
                        side2_draw_position = :side2_draw_position,
                        side2_seed_number = :side2_seed_number,
                        side2_school_name = :side2_school_name,
                        side2_school_id = :side2_school_id,
                        side2_player1_id = :side2_player1_id,
                        side2_player1_name = :side2_player1_name,
                        side2_player2_id = :side2_player2_id,
                        side2_player2_name = :side2_player2_name,
                        winning_side = :winning_side,
                        winner_participant_id = :winner_participant_id,
                        winner_participant_name = :winner_participant_name,
                        score_side1 = :score_side1,
                        score_side2 = :score_side2,
                        scheduled_date = :scheduled_date,
                        scheduled_time = :scheduled_time,
                        venue_name = :venue_name,
                        created_at_api = :created_at_api,
                        updated_at_api = :updated_at_api,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE match_up_id = :match_up_id
                """)
                session.execute(update_sql, match_info)
                logging.debug(f"Updated existing match: {match_info['match_up_id']}")
            else:
                # Insert new match
                insert_sql = text("""
                    INSERT INTO tournament_matches (
                        match_up_id, draw_id, tournament_id, event_id, round_name, round_number, round_position,
                        match_type, match_format, match_status, stage, structure_name,
                        side1_participant_id, side1_participant_name, side1_draw_position, side1_seed_number,
                        side1_school_name, side1_school_id, side1_player1_id, side1_player1_name,
                        side1_player2_id, side1_player2_name,
                        side2_participant_id, side2_participant_name, side2_draw_position, side2_seed_number,
                        side2_school_name, side2_school_id, side2_player1_id, side2_player1_name,
                        side2_player2_id, side2_player2_name,
                        winning_side, winner_participant_id, winner_participant_name,
                        score_side1, score_side2, scheduled_date, scheduled_time, venue_name,
                        created_at_api, updated_at_api
                    ) VALUES (
                        :match_up_id, :draw_id, :tournament_id, :event_id, :round_name, :round_number, :round_position,
                        :match_type, :match_format, :match_status, :stage, :structure_name,
                        :side1_participant_id, :side1_participant_name, :side1_draw_position, :side1_seed_number,
                        :side1_school_name, :side1_school_id, :side1_player1_id, :side1_player1_name,
                        :side1_player2_id, :side1_player2_name,
                        :side2_participant_id, :side2_participant_name, :side2_draw_position, :side2_seed_number,
                        :side2_school_name, :side2_school_id, :side2_player1_id, :side2_player1_name,
                        :side2_player2_id, :side2_player2_name,
                        :winning_side, :winner_participant_id, :winner_participant_name,
                        :score_side1, :score_side2, :scheduled_date, :scheduled_time, :venue_name,
                        :created_at_api, :updated_at_api
                    )
                """)
                session.execute(insert_sql, match_info)
                logging.debug(f"Inserted new match: {match_info['match_up_id']}")
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logging.error(f"Error storing match data: {str(e)}")
            raise
        finally:
            session.close()

    def collect_tournament_event(self, tournament_id: str, event_id: str) -> bool:
        """Collect and store tournament event data"""
        logging.info(f"🎾 Collecting tournament event: {tournament_id}/{event_id}")
        
        # Create tables if they don't exist
        if not self.dry_run:
            self.create_tables_if_not_exist()
        
        # Fetch data from API
        event_data = self.fetch_tournament_data(tournament_id, event_id)
        
        if not event_data or 'eventData' not in event_data:
            logging.error("❌ Failed to fetch event data")
            return False
        
        # Build participants lookup
        participants_lookup = self.build_participants_lookup(event_data)
        
        event_data_inner = event_data['eventData']
        draws_data = event_data_inner.get('drawsData', [])
        
        if not draws_data:
            logging.error("❌ No draws data found")
            return False
        
        logging.info(f"🎯 Found {len(draws_data)} draws to process")
        
        total_matches_stored = 0
        total_draws_stored = 0
        
        # Process each draw
        for draw_idx, draw in enumerate(draws_data):
            draw_name = draw.get('drawName', 'Unknown')
            logging.info(f"Processing Draw {draw_idx + 1}/{len(draws_data)}: {draw_name}")
            
            # Extract and store draw info
            draw_info = self.extract_draw_info_from_working_api(draw, tournament_id, event_id)
            
            self.store_draw_data(draw_info)
            total_draws_stored += 1
            
            # Process structures and matches
            structures = draw.get('structures', [])
            draw_matches_count = 0
            
            for struct_idx, structure in enumerate(structures):
                struct_name = structure.get('structureName', f'Structure {struct_idx + 1}')
                logging.info(f"  📊 Processing Structure: {struct_name}")
                
                # Process round matchups
                round_matchups = structure.get('roundMatchUps', {})
                
                for round_num, matches in round_matchups.items():
                    if not isinstance(matches, list):
                        continue
                    
                    logging.info(f"    🏆 Processing Round {round_num}: {len(matches)} matches")
                    
                    for match_data in matches:
                        try:
                            # Extract and store tournament match info
                            tournament_match = self.extract_tournament_match_from_working_api(match_data, participants_lookup, tournament_id, event_id)
                            self.store_match_data(tournament_match)
                            draw_matches_count += 1
                            total_matches_stored += 1
                        except Exception as e:
                            logging.error(f"Error processing match: {str(e)}")
                            continue
            
            logging.info(f"✅ Draw {draw_idx + 1} completed: {draw_matches_count} matches stored")
        
        logging.info(f"🎉 Tournament collection completed!")
        logging.info(f"   - Draws stored: {total_draws_stored}")
        logging.info(f"   - Matches stored: {total_matches_stored}")
        
        return True

    def run_backfill_all_events(self):
        """Run backfill for all tournament events in the database"""
        logging.info("🚀 Starting backfill for ALL tournament events")
        logging.info("="*80)
        
        # Get all tournament events
        tournament_events = self.get_all_tournament_events()
        
        if not tournament_events:
            logging.error("No tournament events found in database")
            return False
        
        total_success = 0
        total_failed = 0
        
        for i, (tournament_id, event_id, gender, event_type) in enumerate(tournament_events, 1):
            logging.info(f"\n📊 Processing {i}/{len(tournament_events)}: {tournament_id}/{event_id}")
            logging.info(f"   Event: {gender} {event_type}")
            
            try:
                success = self.collect_tournament_event(tournament_id, event_id)
                if success:
                    total_success += 1
                    logging.info(f"✅ Successfully processed event {i}/{len(tournament_events)}")
                else:
                    total_failed += 1
                    logging.error(f"❌ Failed to process event {i}/{len(tournament_events)}")
                    
            except Exception as e:
                total_failed += 1
                logging.error(f"❌ Exception processing event {i}/{len(tournament_events)}: {str(e)}")
        
        logging.info("\n" + "="*80)
        logging.info(f"🏁 Backfill completed!")
        logging.info(f"   ✅ Successful: {total_success}")
        logging.info(f"   ❌ Failed: {total_failed}")
        logging.info(f"   📊 Total: {len(tournament_events)}")
        
        return total_success > 0

    def run_backfill_recent_events(self, days_back: int = 30, limit: Optional[int] = None):
        """Run backfill for recent tournament events only"""
        logging.info(f"🚀 Starting backfill for recent tournament events (last {days_back} days)")
        if limit:
            logging.info(f"   Limited to {limit} events")
        logging.info("="*80)
        
        # Get recent tournament events
        tournament_events = self.get_tournament_events_for_backfill(
            limit=limit, 
            only_recent=True, 
            days_back=days_back
        )
        
        if not tournament_events:
            logging.error("No recent tournament events found in database")
            return False
        
        total_success = 0
        total_failed = 0
        
        for i, event_row in enumerate(tournament_events, 1):
            tournament_id, event_id, gender, event_type, tournament_name, start_date = event_row
            
            logging.info(f"\n📊 Processing {i}/{len(tournament_events)}: {tournament_name}")
            logging.info(f"   Tournament: {tournament_id}")
            logging.info(f"   Event: {event_id} ({gender} {event_type})")
            logging.info(f"   Start Date: {start_date}")
            
            try:
                success = self.collect_tournament_event(tournament_id, event_id)
                if success:
                    total_success += 1
                    logging.info(f"✅ Successfully processed event {i}/{len(tournament_events)}")
                else:
                    total_failed += 1
                    logging.error(f"❌ Failed to process event {i}/{len(tournament_events)}")
                    
            except Exception as e:
                total_failed += 1
                logging.error(f"❌ Exception processing event {i}/{len(tournament_events)}: {str(e)}")
        
        logging.info("\n" + "="*80)
        logging.info(f"🏁 Recent events backfill completed!")
        logging.info(f"   ✅ Successful: {total_success}")
        logging.info(f"   ❌ Failed: {total_failed}")
        logging.info(f"   📊 Total: {len(tournament_events)}")
        
        return total_success > 0


def main():
    """Main function to run the backfill"""
    setup_logging()
    
    # Database configuration
    database_url = os.getenv('DATABASE_URL', 
        "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require")
    
    # Initialize collector
    collector = StandaloneTournamentCollector(database_url, dry_run=False)
    
    logging.info("🚀 Starting Tournament Draw Collector for ALL EVENTS")
    logging.info("="*80)
    
    # Choose your backfill strategy:
    
    # Option 1: Backfill ALL tournament events (could be many!)
    success = collector.run_backfill_all_events()
    
    # Option 2: Backfill only recent events (safer for testing)
    # success = collector.run_backfill_recent_events(days_back=30, limit=50)
    
    # Option 3: Test with a single known tournament/event first
    # tournament_id = "92BC5EA2-B793-4E41-8252-9838A350538E"
    # event_id = "C5E4C725-9F1F-4754-9B33-F145E1DE1623"
    # success = collector.collect_tournament_event(tournament_id, event_id)
    
    if success:
        logging.info("🏆 Backfill completed successfully!")
    else:
        logging.error("💥 Backfill failed!")


if __name__ == "__main__":
    main()
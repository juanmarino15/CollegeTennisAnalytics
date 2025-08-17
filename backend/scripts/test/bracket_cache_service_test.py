#!/usr/bin/env python3
"""
Ultra-Simple Tournament Collector using roundMatchUps data
Uses only 2 tables: tournament_draws and tournament_matches
All tournament data is captured in these two tables with no redundancy
"""

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
from datetime import datetime
from typing import Dict, List, Any, Optional

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ultra_simple_tournament_collector.log'),
            logging.StreamHandler()
        ]
    )

class UltraSimpleTournamentCollector:
    def __init__(self, database_url: str = None, dry_run: bool = True):
        """Initialize the collector"""
        self.database_url = database_url
        self.dry_run = dry_run
        
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

    def fetch_tournament_data(self, tournament_id: str, event_id: str) -> Dict[str, Any]:
        """Fetch complete tournament data from API"""
        logging.info(f"Fetching tournament data for tournament: {tournament_id}, event: {event_id}")
        
        try:
            payload = self.create_api_query(tournament_id, event_id)
            
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
                            return {}
                    
                    return event_data
                else:
                    logging.warning("No event data found in response")
                    return {}
            else:
                logging.error(f"API request failed with status {response.status_code}")
                return {}
                
        except Exception as e:
            logging.error(f"Error fetching tournament data: {str(e)}")
            return {}

    def extract_draw_info(self, draw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract draw information for tournament_draws table"""
        draw_info = {
            'draw_id': draw_data.get('drawId', '').lower(),
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

    def extract_tournament_match(self, match_data: Dict[str, Any], participants_lookup: Dict[str, Any]) -> Dict[str, Any]:
        """Extract match information for tournament_matches table (everything in one record)"""
        
        # Extract basic match info
        match_info = {
            'match_up_id': match_data.get('matchUpId', ''),
            'draw_id': match_data.get('drawId', '').lower(),
            'tournament_id': match_data.get('tournamentId', '').lower(),
            'event_id': match_data.get('eventId', '').lower(),
            'round_name': match_data.get('roundName', ''),
            'round_number': match_data.get('roundNumber', 0),
            'round_position': match_data.get('roundPosition', 0),
            'match_type': match_data.get('matchUpType', ''),
            'match_format': match_data.get('matchUpFormat', ''),
            'match_status': match_data.get('matchUpStatus', ''),
            'stage': match_data.get('stage', ''),
            'structure_name': match_data.get('structureName', ''),
            'winning_side': match_data.get('winningSide'),
            'winner_match_up_id': match_data.get('winnerMatchUpId'),
            'loser_match_up_id': match_data.get('loserMatchUpId'),
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
            participant_id = side.get('participantId', '').lower()
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
        
        return match_info

    def build_participants_lookup(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build comprehensive lookup dictionary for participant data"""
        participants_lookup = {}
        individual_participants = {}
        
        participants = event_data.get('participants', [])
        
        # First pass: collect all individual participants
        for participant in participants:
            participant_id = participant.get('participantId', '').lower()
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
                individual_ids = [pid.lower() for pid in individual_ids]
                
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
                        if p.get('participantId', '').lower() == ind_id:
                            ind_name = p.get('participantName', 'Unknown Player')
                            
                            # Get school info from individual participant
                            ind_teams = p.get('teams', [])
                            if ind_teams and len(ind_teams) > 0:
                                ind_team = ind_teams[0]
                                ind_school_name = ind_team.get('participantOtherName') or ind_team.get('participantName')
                                ind_school_id = ind_team.get('teamId') or ind_team.get('participantId')
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
        
        return participants_lookup

    def log_table_data(self, table_name: str, data: Any, max_records: int = 3):
        """Log what would be inserted into database tables"""
        logging.info(f"\n{'='*80}")
        logging.info(f"TABLE: {table_name}")
        logging.info(f"{'='*80}")
        
        if isinstance(data, list):
            logging.info(f"Total records to insert: {len(data)}")
            
            for i, record in enumerate(data[:max_records]):
                logging.info(f"\nRecord {i+1}:")
                for key, value in record.items():
                    logging.info(f"  {key}: {value}")
            
            if len(data) > max_records:
                logging.info(f"\n... and {len(data) - max_records} more records")
        
        elif isinstance(data, dict):
            logging.info(f"Single record to insert:")
            for key, value in data.items():
                logging.info(f"  {key}: {value}")
        
        logging.info(f"{'='*80}\n")

    def log_ultra_simple_schemas(self):
        """Log the ultra-simple 2-table schema"""
        logging.info(f"\n{'='*80}")
        logging.info(f"ULTRA-SIMPLE 2-TABLE SCHEMA")
        logging.info(f"{'='*80}")
        
        logging.info(f"\n-- EVERYTHING YOU NEED IN JUST 2 TABLES!")
        logging.info(f"-- No redundancy, complete tournament data capture")
        
        # Tournament Draws table
        logging.info(f"\nCREATE TABLE tournament_draws (")
        logging.info(f"    draw_id VARCHAR PRIMARY KEY,")
        logging.info(f"    tournament_id VARCHAR,")
        logging.info(f"    event_id VARCHAR,")
        logging.info(f"    draw_name VARCHAR,")
        logging.info(f"    draw_type VARCHAR,")
        logging.info(f"    draw_size INTEGER,")
        logging.info(f"    draw_active BOOLEAN,")
        logging.info(f"    draw_completed BOOLEAN,")
        logging.info(f"    event_type VARCHAR,")
        logging.info(f"    gender VARCHAR,")
        logging.info(f"    match_up_format VARCHAR,")
        logging.info(f"    updated_at_api TIMESTAMP,")
        logging.info(f"    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        logging.info(f");")
        
        # Tournament Matches table (everything in one place)
        logging.info(f"\nCREATE TABLE tournament_matches (")
        logging.info(f"    id SERIAL PRIMARY KEY,")
        logging.info(f"    match_up_id VARCHAR NOT NULL UNIQUE,")
        logging.info(f"    draw_id VARCHAR REFERENCES tournament_draws(draw_id),")
        logging.info(f"    tournament_id VARCHAR,")
        logging.info(f"    event_id VARCHAR,")
        logging.info(f"    round_name VARCHAR,")
        logging.info(f"    round_number INTEGER,")
        logging.info(f"    round_position INTEGER,")
        logging.info(f"    match_type VARCHAR,")
        logging.info(f"    match_status VARCHAR,")
        logging.info(f"    structure_name VARCHAR,")
        logging.info(f"    -- Side 1 data (pair info + individual players + school)")
        logging.info(f"    side1_participant_id VARCHAR,")
        logging.info(f"    side1_participant_name VARCHAR,")
        logging.info(f"    side1_draw_position INTEGER,")
        logging.info(f"    side1_seed_number INTEGER,")
        logging.info(f"    side1_school_name VARCHAR,  -- School/Team name (e.g., 'Pepperdine', 'Arizona')")
        logging.info(f"    side1_school_id VARCHAR,   -- School/Team ID")
        logging.info(f"    side1_player1_id VARCHAR,  -- Individual player ID")
        logging.info(f"    side1_player1_name VARCHAR,  -- Individual player name")
        logging.info(f"    side1_player2_id VARCHAR,  -- Partner ID (doubles only)")
        logging.info(f"    side1_player2_name VARCHAR,  -- Partner name (doubles only)")
        logging.info(f"    -- Side 2 data (pair info + individual players + school)")
        logging.info(f"    side2_participant_id VARCHAR,")
        logging.info(f"    side2_participant_name VARCHAR,")
        logging.info(f"    side2_draw_position INTEGER,")
        logging.info(f"    side2_seed_number INTEGER,")
        logging.info(f"    side2_school_name VARCHAR,  -- School/Team name (e.g., 'Oklahoma State', 'Cal')")
        logging.info(f"    side2_school_id VARCHAR,   -- School/Team ID")
        logging.info(f"    side2_player1_id VARCHAR,  -- Individual player ID")
        logging.info(f"    side2_player1_name VARCHAR,  -- Individual player name")
        logging.info(f"    side2_player2_id VARCHAR,  -- Partner ID (doubles only)")
        logging.info(f"    side2_player2_name VARCHAR,  -- Partner name (doubles only)")
        logging.info(f"    -- Match outcome and progression")
        logging.info(f"    winning_side INTEGER,")
        logging.info(f"    winner_match_up_id VARCHAR,")
        logging.info(f"    loser_match_up_id VARCHAR,")
        logging.info(f"    -- Complete scores")
        logging.info(f"    score_side1 VARCHAR,")
        logging.info(f"    score_side2 VARCHAR,")
        logging.info(f"    -- Scheduling")
        logging.info(f"    scheduled_date DATE,")
        logging.info(f"    venue_name VARCHAR,")
        logging.info(f"    created_at_api TIMESTAMP")
        logging.info(f");")
        
        logging.info(f"\n-- With this enhanced schema you can:")
        logging.info(f"-- ✅ Track individual players in both singles and doubles")
        logging.info(f"-- ✅ Analyze player partnerships in doubles")
        logging.info(f"-- ✅ Track school/team performance in tournaments")
        logging.info(f"-- ✅ Build complete player statistics across tournaments")
        logging.info(f"-- ✅ Analyze school vs school matchups")
        logging.info(f"-- ✅ Query player performance regardless of match type")
        logging.info(f"-- ✅ Generate team-based tournament analytics")
        
        logging.info(f"{'='*80}\n")

    def process_single_tournament_event(self, tournament_id: str, event_id: str):
        """Process a single tournament event with ultra-simple 2-table approach"""
        
        logging.info(f"🧪 PROCESSING TOURNAMENT EVENT (ULTRA-SIMPLE 2-TABLE SCHEMA)")
        logging.info(f"Tournament ID: {tournament_id}")
        logging.info(f"Event ID: {event_id}")
        
        # Log ultra-simple schema first
        self.log_ultra_simple_schemas()
        
        # Fetch data
        event_data = self.fetch_tournament_data(tournament_id, event_id)
        
        if not event_data or 'eventData' not in event_data:
            logging.error("❌ Failed to fetch event data")
            return
        
        # Build participants lookup
        participants_lookup = self.build_participants_lookup(event_data)
        logging.info(f"📋 Built participants lookup with {len(participants_lookup)} participants")
        
        event_data_inner = event_data['eventData']
        draws_data = event_data_inner.get('drawsData', [])
        
        if not draws_data:
            logging.error("❌ No draws data found")
            return
        
        logging.info(f"🎾 Found {len(draws_data)} draws to process")
        
        # Process each draw
        for draw_idx, draw in enumerate(draws_data):
            logging.info(f"\n🎯 Processing Draw {draw_idx + 1}: {draw.get('drawName', 'Unknown')}")
            
            # Extract draw info
            draw_info = self.extract_draw_info(draw)
            draw_info['tournament_id'] = tournament_id.lower()
            draw_info['event_id'] = event_id.lower()
            
            self.log_table_data("tournament_draws", draw_info)
            
            # Process structures and extract all matches
            structures = draw.get('structures', [])
            all_tournament_matches = []
            
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
                        # Extract complete tournament match info (everything in one record)
                        tournament_match = self.extract_tournament_match(match_data, participants_lookup)
                        all_tournament_matches.append(tournament_match)
            
            # Log all extracted match data
            if all_tournament_matches:
                self.log_table_data("tournament_matches", all_tournament_matches)
            
            # Summary for this draw
            logging.info(f"✅ Draw {draw_idx + 1} Summary:")
            logging.info(f"   - Tournament Matches: {len(all_tournament_matches)}")
            logging.info(f"   - Complete tournament data captured in just 2 tables!")

def main():
    """Main function to test the ultra-simple collector"""
    setup_logging()
    
    # Initialize collector in dry run mode
    collector = UltraSimpleTournamentCollector(dry_run=True)
    
    # Test with known tournament and event
    tournament_id = "92BC5EA2-B793-4E41-8252-9838A350538E"
    event_id = "67D230CA-7061-4ED6-9C2F-14F886A96F84"
    
    logging.info("🚀 Starting Ultra-Simple Tournament Collector Test")
    logging.info("="*80)
    
    collector.process_single_tournament_event(tournament_id, event_id)
    
    logging.info("\n✅ Test completed! Just 2 tables capture everything you need for tournament analysis.")

if __name__ == "__main__":
    main()
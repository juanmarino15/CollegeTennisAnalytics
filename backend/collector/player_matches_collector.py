# collector/player_matches_collector.py

import requests
import time
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Set, Dict, Optional
from sqlalchemy import create_engine, and_, extract, func
from sqlalchemy.orm import sessionmaker
import logging
from datetime import date
from sqlalchemy import between


# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from models.models import (
    Base, PlayerMatch, PlayerMatchSet, PlayerMatchParticipant,
    Match, MatchLineup, MatchTeam, Player, PlayerRoster, Season
)

class PlayerMatchesCollector:
    def __init__(self, database_url: str):
        self.api_url = 'https://prd-itat-kube.clubspark.pro/mesh-api/graphql'
        
        try:
            self.engine = create_engine(database_url)
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)
        except Exception as e:
            print(f"Database initialization error: {e}")
            self.engine = None
            self.Session = None

    def get_recently_active_players(self) -> List[str]:
        """Get players from rosters of teams that played since January 1st"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        session = self.Session()
        try:
            # First get the correct season_id from seasons table
            print("\n=== Getting Season ID ===")
            season = session.query(Season)\
                .filter(Season.name.like('%2024%'))\
                .first()
                
            if not season:
                print("Error: Could not find 2024 season in seasons table")
                return []
                
            season_id = season.id
            print(f"Found season_id: {season_id} for 2024 season")

            # Get teams from recent matches
            print("\n=== Getting Recent Matches ===")

            recent_matches = (
                session.query(Match)
                .filter(
                    Match.start_date.between('2025-01-01', date.today()),
                    Match.season == '2024'
                )
                .all()
            )

            print(f"Found {len(recent_matches)} matches between January 1, 2025, and today")
            
            
            # Collect team IDs
            active_teams = set()
            for match in recent_matches:
                if match.home_team_id:
                    team_id = match.home_team_id.upper()
                    active_teams.add(team_id)
                if match.away_team_id:
                    team_id = match.away_team_id.upper()
                    active_teams.add(team_id)
            
            print(f"\nFound {len(active_teams)} unique teams")

            # Get players from rosters using correct season_id
            print(f"\n=== Getting Players for Season {season_id} ===")
            active_players = (
                session.query(PlayerRoster.person_id)
                .filter(
                    func.upper(PlayerRoster.team_id).in_([tid for tid in active_teams]),
                    PlayerRoster.season_id == season_id
                )
                .distinct()
            )
                        
            active_players = active_players.all()
            
            # Print some sample roster entries for verification
            sample_rosters = session.query(PlayerRoster)\
                .filter(PlayerRoster.season_id == season_id)\
                .limit(5)\
                .all()
                

            player_list = [p[0] for p in active_players if p[0]]

            
            return player_list
            
        finally:
            session.close()

    def fetch_player_matches(self, person_id: str) -> Dict:
        """Fetch match results for a player from January 1st onwards"""
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

        variables = {
            "personFilter": {
                "ids": [{
                    "type": "ExternalID",
                    "identifier": person_id
                }]
            },
            "filter": {
                "start": {"after": "2025-01-01"},
                "end": {"before": "2025-12-31"},
                "statuses": ["DEFAULTED", "RETIRED", "WALKOVER", "COMPLETED", "ABANDONED"]
            }
        }

        try:
            print(f"Fetching matches for player {person_id}...")
            response = requests.post(
                self.api_url,
                json={
                    'operationName': 'matchUps',
                    'query': query,
                    'variables': variables
                },
                headers={'Content-Type': 'application/json'},
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

    def create_match_identifier(self, match_data: Dict) -> str:
        """Create a unique identifier for a match"""
        player_ids = []
        for side in match_data['sides']:
            for player in side['players']:
                player_ids.append(player['person']['externalID'])
        player_ids.sort()
        
        date = match_data['start'].split('T')[0]
        tournament_id = match_data['tournament']['providerTournamentId']
        
        return f"{date}-{tournament_id}-{'-'.join(player_ids)}-{match_data['type']}"

    def store_player_matches(self, matches_data: Dict) -> None:
        """Store player match data from the API response"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            if not matches_data.get('data', {}).get('td_matchUps', {}).get('items'):
                raise ValueError("No match items found in data")
                
            matches = matches_data['data']['td_matchUps']['items']
            print(f"Processing {len(matches)} matches...")
            
            stored_count = 0
            skipped_count = 0
            
            for match_item in matches:
                try:
                    match_identifier = self.create_match_identifier(match_item)
                    
                    existing_match = session.query(PlayerMatch).filter_by(
                        match_identifier=match_identifier
                    ).first()
                    
                    if existing_match:
                        skipped_count += 1
                        continue
                    
                    start_time = datetime.fromisoformat(match_item['start'].replace('Z', '+00:00'))
                    end_time = datetime.fromisoformat(match_item['end'].replace('Z', '+00:00'))
                    
                    match = PlayerMatch(
                        match_identifier=match_identifier,
                        winning_side=match_item['winningSide'],
                        start_time=start_time,
                        end_time=end_time,
                        match_type=match_item['type'],
                        match_format=match_item['matchUpFormat'],
                        status=match_item['status'],
                        round_name=match_item['roundName'],
                        tournament_id=match_item['tournament']['providerTournamentId'],
                        score_string=match_item['score']['scoreString']
                    )
                    session.add(match)
                    session.flush()
                    
                    # Store sets information
                    for set_idx, set_data in enumerate(match_item['score']['sets'], 1):
                        match_set = PlayerMatchSet(
                            match_id=match.id,
                            set_number=set_idx,
                            winner_games_won=set_data.get('winnerGamesWon'),
                            loser_games_won=set_data.get('loserGamesWon'),
                            win_ratio=set_data.get('winRatio'),
                            tiebreak_winner_points=(
                                set_data.get('tiebreaker', {}).get('winnerPointsWon') 
                                if set_data.get('tiebreaker') else None
                            ),
                            tiebreak_loser_points=(
                                set_data.get('tiebreaker', {}).get('loserPointsWon')
                                if set_data.get('tiebreaker') else None
                            )
                        )
                        session.add(match_set)
                    
                    # Store participants
                    for side in match_item['sides']:
                        team_id = next(
                            (ext['value'] for ext in side['extensions'] 
                             if ext['name'] in ['teamId', 'schoolId']), 
                            None
                        )
                        
                        for player in side['players']:
                            participant = PlayerMatchParticipant(
                                match_id=match.id,
                                person_id=player['person']['externalID'],
                                team_id=team_id,
                                side_number=side['sideNumber'],
                                family_name=player['person']['nativeFamilyName'],
                                given_name=player['person']['nativeGivenName'],
                                is_winner=(side['sideNumber'] == match_item['winningSide'])
                            )
                            session.add(participant)
                    
                    session.commit()
                    stored_count += 1
                    
                except Exception as e:
                    print(f"Error storing match: {e}")
                    session.rollback()
                    continue
            
            print(f"\nCompleted processing {len(matches)} matches:")
            print(f"New matches stored: {stored_count}")
            print(f"Duplicate matches skipped: {skipped_count}")
            
        except Exception as e:
            print(f"Error storing matches: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def store_all_player_matches(self) -> None:
        """Update matches for all recently active players"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            active_players = self.get_recently_active_players()
            total_players = len(active_players)
            print(f"Found {total_players} recently active players to process")
            
            success_count = 0
            error_count = 0
            
            for idx, player_id in enumerate(active_players, 1):
                try:
                    print(f"\nProcessing player {idx}/{total_players}: ID: {player_id}")
                    
                    # Fetch and store new matches
                    matches_data = self.fetch_player_matches(player_id)
                    if matches_data and 'data' in matches_data and 'td_matchUps' in matches_data['data']:
                        self.store_player_matches(matches_data)
                        success_count += 1
                    
                except Exception as e:
                    error_count += 1
                    print(f"Error processing player {player_id}: {e}")
                    continue
                
                time.sleep(1)  # Rate limiting
            
            print("\nProcessing completed!")
            print(f"Successfully processed: {success_count} players")
            print(f"Errors: {error_count} players")
            print(f"Total: {total_players} players")
            
        except Exception as e:
            print(f"Error in main process: {e}")
        finally:
            session.close()
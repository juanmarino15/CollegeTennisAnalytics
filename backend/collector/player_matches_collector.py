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
        
        try:
            self.engine = create_engine(database_url)
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)
        except Exception as e:
            print(f"Database initialization error: {e}")
            self.engine = None
            self.Session = None

    def get_recently_active_players(self) -> List[str]:
        session = self.Session()
        try:
            # Get season ID and active players in a single query
            query = """
            WITH season AS (
                SELECT id FROM seasons WHERE name LIKE '%2024%' LIMIT 1
            ),
            recent_teams AS (
                SELECT DISTINCT UPPER(home_team_id) AS team_id FROM matches 
                WHERE start_date BETWEEN '2025-01-01' AND CURRENT_DATE AND season = '2024'
                UNION
                SELECT DISTINCT UPPER(away_team_id) AS team_id FROM matches 
                WHERE start_date BETWEEN '2025-01-01' AND CURRENT_DATE AND season = '2024'
            )
            SELECT DISTINCT pr.person_id 
            FROM player_rosters pr
            JOIN season s ON pr.season_id = s.id
            WHERE UPPER(pr.team_id) IN (SELECT team_id FROM recent_teams)
            """
            
            result = session.execute(query).fetchall()
            return [r[0] for r in result if r[0]]
            
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
                headers=self.headers,
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
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            if not matches_data.get('data', {}).get('td_matchUps', {}).get('items'):
                return
                
            matches = matches_data['data']['td_matchUps']['items']
            matches_to_add = []
            sets_to_add = []
            participants_to_add = []
            
            for match_item in matches:
                try:
                    match_identifier = self.create_match_identifier(match_item)
                    
                    # Skip existing matches (check in bulk to reduce queries)
                    existing_match = session.query(PlayerMatch.match_identifier).filter(
                        PlayerMatch.match_identifier.in_([match_identifier])
                    ).first()
                    
                    if existing_match:
                        continue
                    
                    # Create match object
                    match = PlayerMatch(
                        match_identifier=match_identifier,
                        # other fields...
                    )
                    
                    matches_to_add.append(match)
                    # Collect sets and participants to add
                    
                except Exception as e:
                    logging.error(f"Error processing match: {str(e)}")
                    continue
            
            # Bulk insert
            if matches_to_add:
                session.bulk_save_objects(matches_to_add)
                session.flush()
                
                # Now process sets and participants with IDs from flushed matches
                if sets_to_add:
                    session.bulk_save_objects(sets_to_add)
                if participants_to_add:
                    session.bulk_save_objects(participants_to_add)
                    
                session.commit()
                
        except Exception as e:
            logging.error(f"Error storing matches: {str(e)}")
            session.rollback()
        finally:
            session.close()

    # For player matches collector
    async def store_all_player_matches(self) -> None:
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        active_players = self.get_recently_active_players()
        total_players = len(active_players)
        logging.info(f"Found {total_players} players to process")
        
        # Process players in batches
        batch_size = 10  # Process 10 players concurrently
        success_count, error_count = 0, 0
        
        for i in range(0, total_players, batch_size):
            batch = active_players[i:i+batch_size]
            tasks = []
            
            for player_id in batch:
                task = asyncio.create_task(self.process_single_player(player_id))
                tasks.append(task)
            
            # Wait for all tasks in batch to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    error_count += 1
                else:
                    success_count += 1
            
            logging.info(f"Processed batch {i//batch_size + 1}/{(total_players+batch_size-1)//batch_size}")
        
        logging.info(f"Successfully processed: {success_count}/{total_players} players")

    # Add this new method
    async def process_single_player(self, player_id: str) -> bool:
        try:
            # Convert fetch_player_matches to async
            matches_data = await self.fetch_player_matches_async(player_id)
            if matches_data and 'data' in matches_data and 'td_matchUps' in matches_data['data']:
                await self.store_player_matches_async(matches_data)
                return True
        except Exception as e:
            logging.error(f"Error processing player {player_id}: {str(e)}")
            return False
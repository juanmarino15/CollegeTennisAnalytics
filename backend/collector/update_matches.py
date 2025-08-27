# collector/match_updates_service.py

import asyncio
import httpx
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine, or_, func, and_
from sqlalchemy.orm import sessionmaker
from typing import Optional, Dict
import requests
import pytz

from models.models import (
    Base, Match, MatchTeam, MatchLineup, MatchLineupSet,
    Player, PlayerRoster, Season, SchoolInfo, PlayerSeason, PlayerWTN, Team, WebLink
)

class MatchUpdatesService:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)
        # Use correct API endpoint
        # Change in MatchUpdatesService __init__
        self.api_url = 'https://prd-itat-kube-tournamentdesk-api.clubspark.pro/'  # Instead of tournamentdesk-api
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

    async def fetch_matches_batch(self, skip: int = 0, limit: int = 100, is_completed: bool = True) -> Optional[Dict]:
        """Fetch multiple matches in one request"""
        print(is_completed)
        query = """
        query dualMatchesPaginated($skip: Int!, $limit: Int!, $filter: DualMatchesFilter, $sort: DualMatchesSort) {
            dualMatchesPaginated(skip: $skip, limit: $limit, filter: $filter, sort: $sort) {
                totalItems
                items {
                    id
                    startDateTime {
                        timezoneName
                        noScheduledTime
                        dateTimeString
                        __typename
                    }
                    homeTeam {
                        name
                        abbreviation
                        id
                        division
                        conference
                        region
                        score
                        didWin
                        sideNumber
                        __typename
                    }
                    teams {
                        name
                        abbreviation
                        id
                        division
                        conference
                        region
                        score
                        didWin
                        sideNumber
                        __typename
                    }
                    isConferenceMatch
                    gender
                    webLinks {
                        name
                        url
                        __typename
                    }
                    __typename
                }
            }
        }
        """

        today = datetime.now()
        one_week_ago = today - timedelta(days=7)

        variables = {
            "skip": skip,
            "limit": limit,
            "sort": {
                "field": "START_DATE",
                "direction": "DESCENDING" 
            },
            "filter": {
                "seasonStarting": "2025",
                "isCompleted": is_completed,
                "divisions": ["DIVISION_1"]
            }
        }

        try:
            async with httpx.AsyncClient(verify=False) as client:
                logging.debug(f"Fetching matches with variables: {variables}")
                print(self.headers)
                response = await client.post(
                    self.api_url,
                    json={
                        "operationName": "dualMatchesPaginated",
                        "query": query,
                        "variables": variables
                    },
                    headers=self.headers,
                    timeout=30.0
                )

                if response.status_code == 200:
                    data = response.json()
                    matches_data = data.get('data', {}).get('dualMatchesPaginated', {})
                    items = matches_data.get('items', [])
                    total = matches_data.get('totalItems', 0)
                    logging.info(f"Found {total} total matches, received {len(items)} in this batch")
                    return matches_data
                else:
                    logging.error(f"Error response from API: Statuss {response.status_code}")
                    logging.error(f"Response body: {response.text}")
                    return None

        except Exception as e:
            logging.error(f"Request error: {str(e)}")
            return None

    async def process_matches_batch(self, is_completed: bool = True) -> int:
        """Process either completed or upcoming matches"""
        skip = 0
        limit = 100
        total_processed = 0
        match_type = "completed" if is_completed else "upcoming"
        cutoff_date = (datetime.today() - timedelta(days=15)).date()  # 15 days before today

        while True:
            logging.info(f"Fetching {match_type} matches batch (skip={skip}, limit={limit})")
            batch_data = await self.fetch_matches_batch(skip, limit, is_completed)
            
            if not batch_data or not batch_data.get('items'):
                logging.info(f"No more {match_type} matches to process")
                break

            matches = batch_data['items']
            total_items = batch_data.get('totalItems', 0)
            
            future_match_count = 0  # Counter for future matches in this batch
            should_skip_batch = False  # Flag to indicate if we should skip to next batch

            for match_data in matches:
                try:
                    # Parse match date and get just the date part
                    match_date_str = match_data['startDateTime']['dateTimeString']
                    match_date = datetime.fromisoformat(match_date_str.replace('Z', '+00:00')).date()
                    
                    # Get today's date and calculate one month from now
                    today = datetime.now().date()
                    days_in_future = today + timedelta(days=10)

                    # If we hit matches more than a 10 days in the future, skip to next batch
                    if match_date > days_in_future:
                        logging.info(f"Found match on {match_date} (more than a month away). Skipping to next batch.")
                        should_skip_batch = True
                        break  # Break out of the for loop

                    # If we hit a match before the last 10 days, stop processing completely
                    if match_date < cutoff_date:
                        logging.info(f"Reached match before last 10 days (date: {match_date}). Stopping processing.")
                        return total_processed

                    match_id = match_data.get('id')
                    logging.debug(f"Processing {match_type} match {match_id} from {match_date}")
                    self.store_single_match(match_data)

                    lineup_data = await self.fetch_dual_match_details(match_id)
                    if lineup_data and 'data' in lineup_data and 'dualMatch' in lineup_data['data']:
                        await self.store_match_lineup(match_id, lineup_data)
                        # await self.update_lineup_team_names(match_id, lineup_data)
                    
                    total_processed += 1
                except Exception as e:
                    match_id = match_data.get('id', 'unknown')
                    logging.error(f"Error processing {match_type} match {match_id}: {str(e)}")
                    continue

            if should_skip_batch:
                skip += limit
                continue  # Skip to next iteration of while loop

            logging.info(f"Processed {total_processed} out of {total_items} {match_type} matches")
            
            if len(matches) < limit:
                break
                
            skip += limit
            await asyncio.sleep(1)

        return total_processed

    async def update_matches(self) -> None:
        """Update both completed and upcoming matches, then check for matches that dont have scores and update them"""

        try:
            # Process completed matches from last week
            logging.info("Processing completed matches from last week...")
            completed_count = await self.process_matches_batch(is_completed=True)
            logging.info(f"Completed processing {completed_count} completed matches")

            # Process upcoming matches
            logging.info("Processing upcoming matches...")
            upcoming_count = await self.process_matches_batch(is_completed=False)
            logging.info(f"Completed processing {upcoming_count} upcoming matches")

        except Exception as e:
            logging.error(f"Error in update_matches: {str(e)}")
            raise
    
    async def fetch_dual_match_details(self, match_id: str) -> Dict:
        """Fetch detailed lineup information for a match"""
        query = """query dualMatch($id: ID!) {
            dualMatch(id: $id) {
                id
                startDateTime {
                    dateTimeString
                }
                teams {
                    name
                    id
                    score
                    sideNumber
                    abbreviation
                }
                isConferenceMatch
                tieMatchUps {
                    id
                    type
                    status
                    side1 {
                        participants {
                            firstName
                            lastName
                            personId
                        }
                        score {
                            scoreString
                            sets {
                                setScore
                                tiebreakScore
                                tiebreakSet
                                didWin
                            }
                        }
                        teamAbbreviation
                        didWin
                    }
                    side2 {
                        participants {
                            firstName
                            lastName
                            personId
                        }
                        score {
                            scoreString
                            sets {
                                setScore
                                tiebreakScore
                                tiebreakSet
                                didWin
                            }
                        }
                        teamAbbreviation
                        didWin
                    }
                    collectionPosition
                    collectionId
                }
            }
        }"""

        try:
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.post(
                    self.api_url,
                    json={
                        'operationName': 'dualMatch',
                        'query': query,
                        'variables': {'id': match_id}
                    },
                    timeout=30.0,
                    headers=self.headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    # logging.info(f"Successfully fetched lineup data for match {match_id}")
                    return data
                else:
                    logging.error(f"Error fetching match lineup: Status {response.status_code}")
                    return {}
                    
        except Exception as e:
            logging.error(f"Error fetching match lineup: {e}")
            return {}

    async def ensure_players_exist(self, session, tie_match: Dict, match_id: str) -> bool:
        """Ensure all players in the lineup exist in the database"""
        try:
            # Collect all player IDs and info from both sides
            players_to_check = []
            print('match id', match_id)
            
            # Process both sides
            for side in ['side1', 'side2']:
                for idx, participant in enumerate(tie_match[side].get('participants', [])):
                    if participant:
                        players_to_check.append({
                            'side': side,
                            'index': idx,
                            'person_id': participant.get('personId'),
                            'first_name': participant.get('firstName'),
                            'last_name': participant.get('lastName')
                        })

            # Check and create players if needed
            for player_info in players_to_check:
                print('player info', player_info)
                if not player_info['person_id']:
                    logging.warning(f"Missing person_id for player {player_info}")
                    continue

                existing_player = session.query(Player).get(player_info['person_id'])
                print('existing player', existing_player)
                
                if not existing_player:
                    # Try to find player by name
                    name_match = session.query(Player).filter(
                        func.upper(Player.first_name) == func.upper(player_info['first_name']),
                        func.upper(Player.last_name) == func.upper(player_info['last_name'])
                    ).first()
                    
                    if name_match:
                        # Update tie_match data with the matched player's ID
                        logging.info(f"Found player match by name: {name_match.first_name} {name_match.last_name} ({name_match.person_id})")
                        
                        # Update the participant data in tie_match
                        tie_match[player_info['side']]['participants'][player_info['index']]['personId'] = name_match.person_id
                    else:
                        new_player = Player(
                            person_id=player_info['person_id'],
                            first_name=player_info['first_name'],
                            last_name=player_info['last_name']
                        )
                        # session.add(new_player)
                        logging.info(f"Created new player: {player_info['first_name']} {player_info['last_name']}")

            session.flush()
            return True

        except Exception as e:
            logging.error(f"Error ensuring players exist: {e}")
            return False

    async def store_match_lineup(self, match_id: str, match_data: Dict) -> None:
        """Store lineup data for a match"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            match = match_data['data']['dualMatch']
            # print(match)
            # logging.info(f"Processing match {match_id} with {len(match['tieMatchUps'])} lineups")
            
            # Check if match exists
            existing_match = session.query(Match).get(match_id)
            if not existing_match:
                logging.warning(f"Match {match_id} not found in database")
                return
                
            # Check for existing lineups
            existing_lineups = session.query(MatchLineup).filter_by(match_id=match_id).all()
            if existing_lineups:
                logging.info(f"Match {match_id} already has lineups stored. Skipping...")
                return
                
            # Store each lineup position
            for tie_match in match['tieMatchUps']:
                try:
                    # Validate data
                    if not self.validate_lineup_data(tie_match):
                        continue

                    # Ensure all players exist in database
                    if not await self.ensure_players_exist(session, tie_match, match_id):
                        logging.error("Failed to ensure players exist, skipping lineup")
                        continue

                    # Create and store lineup
                    lineup = self.create_lineup(match_id, tie_match, match)
                    session.add(lineup)
                    
                    # Store set scores
                    self.store_lineup_sets(session, tie_match)
                                    
                except Exception as e:
                    logging.error(f"Error storing lineup: {e}")
                    session.rollback()
                    continue
                
            session.commit()
            logging.info(f"Successfully stored all lineup data for match {match_id}")
            
        except Exception as e:
            logging.error(f"Error storing match lineup: {e}")
            session.rollback()
        finally:
            session.close()

    async def update_lineup_team_names(self, match_id: str, match_data: Dict) -> None:
        """Update team names in match lineups"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            match = match_data['data']['dualMatch']
            
            # Check if match exists
            existing_match = session.query(Match).get(match_id)
            if not existing_match:
                logging.warning(f"Match {match_id} not found in database")
                return
                
            # Get existing lineups
            existing_lineups = session.query(MatchLineup).filter_by(match_id=match_id).all()
            if not existing_lineups:
                logging.info(f"Match {match_id} has no lineups to update")
                return
            
            # Create mapping of lineup ID to tie match data
            tie_match_by_id = {tm['id']: tm for tm in match['tieMatchUps']}
            
            # Update each lineup
            updates_count = 0
            for lineup in existing_lineups:
                try:
                    # Find corresponding tie match
                    tie_match = tie_match_by_id.get(lineup.id)
                    if not tie_match:
                        logging.warning(f"Couldn't find tie match data for lineup {lineup.id}")
                        continue
                    
                    # Extract team names
                    side1_name = None
                    side2_name = None

                    # Try to get team names from teamAbbreviation
                    if tie_match['side1'].get('teamAbbreviation'):
                        for team in match['teams']:
                            if team.get('abbreviation') == tie_match['side1']['teamAbbreviation']:
                                side1_name = team.get('abbreviation')
                                break

                    if tie_match['side2'].get('teamAbbreviation'):
                        for team in match['teams']:
                            if team.get('abbreviation') == tie_match['side2']['teamAbbreviation']:
                                side2_name = team.get('abbreviation')
                                break
                    
                    # Update the team names
                    lineup.side1_name = side1_name
                    lineup.side2_name = side2_name
                    session.add(lineup)  # This is crucial - add the updated object back to session
                    updates_count += 1
                    
                except Exception as e:
                    logging.error(f"Error updating lineup {lineup.id}: {e}")
                    continue
                
            if updates_count > 0:
                session.commit()  # Commit the changes
                logging.info(f"Successfully updated team names for {updates_count} lineups in match {match_id}")
            else:
                logging.info(f"No team name updates made for match {match_id}")
            
        except Exception as e:
            logging.error(f"Error updating team names: {e}")
            session.rollback()
        finally:
            session.close()

    def validate_lineup_data(self, tie_match: Dict) -> bool:
        """Validate lineup data is complete"""
        try:
            if not tie_match.get('side1') or not tie_match.get('side2'):
                return False
                
            if not tie_match['side1'].get('score') or not tie_match['side2'].get('score'):
                return False
                
            if not tie_match['side1'].get('participants') or not tie_match['side2'].get('participants'):
                return False
                
            if not tie_match['side1']['participants'] or not tie_match['side2']['participants']:
                return False

            side1_score = tie_match['side1']['score'].get('scoreString')
            side2_score = tie_match['side2']['score'].get('scoreString')
            
            if not side1_score or not side2_score:
                return False

            return True
        except Exception:
            return False

    def create_lineup(self, match_id: str, tie_match: Dict, match: Dict = None) -> MatchLineup:
        """Create a MatchLineup instance with proper team abbreviations"""
        # Get player IDs
        side1_player1_id = tie_match['side1']['participants'][0].get('personId')
        side2_player1_id = tie_match['side2']['participants'][0].get('personId')

        # Get team names from various sources
        side1_name = None
        side2_name = None

        # First try to get team abbreviations directly from the side data
        if tie_match['side1'].get('teamAbbreviation'):
            side1_name = tie_match['side1']['teamAbbreviation']
        
        if tie_match['side2'].get('teamAbbreviation'):
            side2_name = tie_match['side2']['teamAbbreviation']

        # If not found, try to get from match teams data
        if not side1_name or not side2_name:
            if match and match.get('teams'):
                # Create a mapping of sideNumber to team abbreviation
                team_abbrevs = {}
                for team in match['teams']:
                    if 'abbreviation' in team and 'sideNumber' in team:
                        team_abbrevs[team['sideNumber']] = team['abbreviation']
                
                # If we still don't have side1_name and a mapping exists
                if not side1_name and '1' in team_abbrevs:
                    side1_name = team_abbrevs['1']
                
                # If we still don't have side2_name and a mapping exists
                if not side2_name and '2' in team_abbrevs:
                    side2_name = team_abbrevs['2']

        # As a last resort, if we have the match data but couldn't get the abbreviations
        # from the usual sources, try to find the teams by ID and get their abbreviations
        if (not side1_name or not side2_name) and match and match.get('teams'):
            # Try to find which team is on which side by matching participants to teams
            for participant in tie_match['side1'].get('participants', []):
                for team in match.get('teams', []):
                    if team.get('abbreviation'):
                        side1_name = team.get('abbreviation')
                        break
                if side1_name:
                    break
                    
            for participant in tie_match['side2'].get('participants', []):
                for team in match.get('teams', []):
                    if team.get('abbreviation'):
                        side2_name = team.get('abbreviation')
                        break
                if side2_name:
                    break

        # Create the lineup object
        lineup = MatchLineup(
            id=tie_match['id'],
            match_id=match_id,
            match_type=tie_match.get('type'),
            position=tie_match.get('collectionPosition'),
            collection_id=tie_match.get('collectionId'),
            side1_player1_id=side1_player1_id,
            side1_score=tie_match['side1']['score'].get('scoreString'),
            side1_won=tie_match['side1'].get('didWin', False),
            side1_name=side1_name,  # Add the team abbreviation
            side2_player1_id=side2_player1_id,
            side2_score=tie_match['side2']['score'].get('scoreString'),
            side2_won=tie_match['side2'].get('didWin', False),
            side2_name=side2_name  # Add the team abbreviation
        )

        # Add doubles partners if exists
        if tie_match.get('type') == 'DOUBLES':
            if len(tie_match['side1']['participants']) > 1:
                lineup.side1_player2_id = tie_match['side1']['participants'][1].get('personId')
            if len(tie_match['side2']['participants']) > 1:
                lineup.side2_player2_id = tie_match['side2']['participants'][1].get('personId')

        return lineup

    def store_lineup_sets(self, session, tie_match: Dict) -> None:
        """Store set scores for a lineup"""
        if not tie_match['side1']['score'].get('sets') or not tie_match['side2']['score'].get('sets'):
            return

        for idx, set_data in enumerate(tie_match['side1']['score']['sets'], 1):
            try:
                if idx > len(tie_match['side2']['score']['sets']):
                    break
                    
                side1_set_score = set_data.get('setScore')
                side2_set_score = tie_match['side2']['score']['sets'][idx-1].get('setScore')
                
                if side1_set_score is None or side2_set_score is None:
                    continue
                    
                set_score = MatchLineupSet(
                    lineup_id=tie_match['id'],
                    set_number=idx,
                    side1_score=int(side1_set_score),
                    side2_score=int(side2_set_score),
                    side1_tiebreak=int(set_data['tiebreakScore']) if set_data.get('tiebreakScore') else None,
                    side2_tiebreak=int(tie_match['side2']['score']['sets'][idx-1].get('tiebreakScore')) 
                        if tie_match['side2']['score']['sets'][idx-1].get('tiebreakScore') else None,
                    side1_won=set_data.get('didWin', False)
                )
                session.add(set_score)
                
            except Exception as e:
                logging.error(f"Error storing set {idx}: {e}")
                continue

    #### this is what i need to run if the player doesnt exist
    def process_all_rosters(self, season_id: str):
        """Process all rosters for all schools"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            schools = session.query(SchoolInfo).all()
            print(f"Found {len(schools)} schools to process")
            
            for school in schools:
                print(f"\nProcessing school: {school.name}")
                
                # Process men's team if exists
                if school.man_id:
                    print("Processing men's roster...")
                    self.store_team_roster(school.id, school.man_id, season_id)
                    
                # Process women's team if exists
                if school.woman_id:
                    print("Processing women's roster...")
                    self.store_team_roster(school.id, school.woman_id, season_id)
                    
        except Exception as e:
            print(f"Error processing rosters: {e}")
        finally:
            session.close()

    def fetch_roster_members(self, roster_id: str, season_id: str) -> dict:
        """Fetch roster members using GraphQL query"""
        url = "https://prd-itat-kube.clubspark.pro/mesh-api/graphql"
        query = """
        query getRosterMembers($rosterId: String!, $role: RosterRoleEnum!, $seasonId: String!) {
            getRosterMembers(rosterId: $rosterId, role: $role, seasonId: $seasonId) {
                personId
                tennisId
                standardGivenName
                standardFamilyName
                class
                avatarUrl
                worldTennisNumbers {
                    confidence
                    type
                    tennisNumber
                    isRanked
                }
            }
        }
        """

        try:
            response = requests.post(
                url,
                json={
                    'query': query,
                    'operationName': 'getRosterMembers',
                    'variables': {
                        'rosterId': roster_id,
                        'role': 'PLAYER',
                        'seasonId': season_id
                    }
                },
                headers=self.headers,
                verify=False
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching roster members: Status {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"Error fetching roster members: {e}")
            return {}

    async def process_recent_school_rosters(self, season_id: str):
        """Process rosters only for schools that have played matches since Jan 1st 2025"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
                
        session = self.Session()
        try:
            today = datetime.now()
            
            # Get matches between Jan 1st and today
            recent_matches = session.query(Match).filter(
                and_(
                    Match.start_date >= datetime(2025, 1, 1),
                    Match.start_date <= today
                )
            ).all()
            # Get unique team IDs
            team_ids = set()
            for match in recent_matches:
                print(match.home_team_id)
                print(match.away_team_id)
                if match.home_team_id:
                    team_ids.add(match.home_team_id.upper())
                if match.away_team_id:
                    team_ids.add(match.away_team_id.upper())
                    
            logging.info(f"Found {len(team_ids)} teams with recent matches")
            
            # Get schools associated with these teams
            active_schools = session.query(SchoolInfo).filter(
                or_(
                    func.upper(SchoolInfo.man_id).in_(team_ids),
                    func.upper(SchoolInfo.woman_id).in_(team_ids)
                )
            ).all()
            
            logging.info(f"Found {len(active_schools)} schools to process")
            
            # Process rosters for these schools using collector
            for school in active_schools:
                logging.info(f"\nProcessing school: {school.name}")
                
                # Process men's team if exists and has played recently
                if school.man_id and school.man_id.upper() in team_ids:
                    logging.info("Processing men's roster...")
                    self.store_team_roster(school.id, school.man_id, season_id)
                    
                # Process women's team if exists and has played recently
                if school.woman_id and school.woman_id.upper() in team_ids:
                    logging.info("Processing women's roster...")
                    self.store_team_roster(school.id, school.woman_id, season_id)
                    
        except Exception as e:
            logging.error(f"Error processing rosters: {e}")
            raise
        finally:
            session.close()
    
    def store_single_match(self, match_data):
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            # First, validate and get team IDs
            home_team_id = None
            away_team_id = None
            teams = match_data.get('teams', [])
            
            if not teams:
                raise ValueError(f"No teams found for match {match_data['id']}")

            # Process home team
            if match_data.get('homeTeam'):
                # Check if team exists first to preserve existing data
                existing_home_team = session.query(Team).filter(
                    func.upper(Team.id) == func.upper(match_data['homeTeam']['id'])
                ).first()
                
                if existing_home_team:
                    # Log the existing values before update
                    logging.info(f"TEAM BEFORE UPDATE - ID: {existing_home_team.id}, Name: {existing_home_team.name}")
                    logging.info(f"  Current values - Conference: '{existing_home_team.conference}', Region: '{existing_home_team.region}', Division: '{existing_home_team.division}'")
                    
                    # Log the incoming values from the API
                    logging.info(f"  API values - Conference: '{match_data['homeTeam'].get('conference')}', Region: '{match_data['homeTeam'].get('region')}', Division: '{match_data['homeTeam'].get('division')}'")
                    
                    # Only update name which should always be present
                    existing_home_team.name = match_data['homeTeam']['name']
                    
                    # Log evaluation results for each field
                    if match_data['homeTeam'].get('abbreviation'):
                        existing_home_team.abbreviation = match_data['homeTeam']['abbreviation']
                    else:
                        logging.warning(f"  SKIPPED UPDATE - Team {existing_home_team.id}: abbreviation value '{match_data['homeTeam'].get('abbreviation')}' evaluates to False")
                    
                    if match_data['homeTeam'].get('division'):
                        existing_home_team.division = match_data['homeTeam']['division']
                    else:
                        logging.warning(f"  SKIPPED UPDATE - Team {existing_home_team.id}: division value '{match_data['homeTeam'].get('division')}' evaluates to False")
                    
                    if match_data['homeTeam'].get('conference'):
                        existing_home_team.conference = match_data['homeTeam']['conference']
                    else:
                        logging.warning(f"  SKIPPED UPDATE - Team {existing_home_team.id}: conference value '{match_data['homeTeam'].get('conference')}' evaluates to False")
                    
                    if match_data['homeTeam'].get('region'):
                        existing_home_team.region = match_data['homeTeam']['region']
                    else:
                        logging.warning(f"  SKIPPED UPDATE - Team {existing_home_team.id}: region value '{match_data['homeTeam'].get('region')}' evaluates to False")
                        
                    # Always update gender
                    existing_home_team.gender = match_data['gender']
                    session.merge(existing_home_team)
                    home_team_id = existing_home_team.id
                    
                    # Log the values after update for verification
                    logging.info(f"TEAM AFTER UPDATE - ID: {existing_home_team.id}, Name: {existing_home_team.name}")
                    logging.info(f"  Updated values - Conference: '{existing_home_team.conference}', Region: '{existing_home_team.region}', Division: '{existing_home_team.division}'")
                else:
                    # Create new team if it doesn't exist
                    home_team = Team(
                        id=match_data['homeTeam']['id'],
                        name=match_data['homeTeam']['name'],
                        abbreviation=match_data['homeTeam'].get('abbreviation'),
                        division=match_data['homeTeam'].get('division'),
                        conference=match_data['homeTeam'].get('conference'),
                        region=match_data['homeTeam'].get('region'),
                        typename=match_data['homeTeam'].get('__typename'),
                        gender=match_data['gender']
                    )
                    logging.info(f"CREATING NEW TEAM - ID: {home_team.id}, Name: {home_team.name}")
                    logging.info(f"  Initial values - Conference: '{home_team.conference}', Region: '{home_team.region}', Division: '{home_team.division}'")
                    session.merge(home_team)
                    home_team_id = home_team.id
            else:
                # If no home team specified, use first team from teams list
                home_team_data = teams[0]
                # Check if team exists first to preserve existing data
                existing_home_team = session.query(Team).filter(
                    func.upper(Team.id) == func.upper(home_team_data['id'])
                ).first()
                
                if existing_home_team:
                    # Log the existing values before update
                    logging.info(f"TEAM BEFORE UPDATE (from teams) - ID: {existing_home_team.id}, Name: {existing_home_team.name}")
                    logging.info(f"  Current values - Conference: '{existing_home_team.conference}', Region: '{existing_home_team.region}', Division: '{existing_home_team.division}'")
                    
                    # Log the incoming values from the API
                    logging.info(f"  API values - Conference: '{home_team_data.get('conference')}', Region: '{home_team_data.get('region')}', Division: '{home_team_data.get('division')}'")
                    
                    # Only update name which should always be present
                    existing_home_team.name = home_team_data['name']
                    
                    # Log evaluation results for each field
                    if home_team_data.get('abbreviation'):
                        existing_home_team.abbreviation = home_team_data['abbreviation']
                    else:
                        logging.warning(f"  SKIPPED UPDATE - Team {existing_home_team.id}: abbreviation value '{home_team_data.get('abbreviation')}' evaluates to False")
                    
                    if home_team_data.get('division'):
                        existing_home_team.division = home_team_data['division']
                    else:
                        logging.warning(f"  SKIPPED UPDATE - Team {existing_home_team.id}: division value '{home_team_data.get('division')}' evaluates to False")
                    
                    if home_team_data.get('conference'):
                        existing_home_team.conference = home_team_data['conference']
                    else:
                        logging.warning(f"  SKIPPED UPDATE - Team {existing_home_team.id}: conference value '{home_team_data.get('conference')}' evaluates to False")
                    
                    if home_team_data.get('region'):
                        existing_home_team.region = home_team_data['region']
                    else:
                        logging.warning(f"  SKIPPED UPDATE - Team {existing_home_team.id}: region value '{home_team_data.get('region')}' evaluates to False")
                        
                    # Always update gender
                    existing_home_team.gender = match_data['gender']
                    session.merge(existing_home_team)
                    home_team_id = existing_home_team.id
                    
                    # Log the values after update for verification
                    logging.info(f"TEAM AFTER UPDATE - ID: {existing_home_team.id}, Name: {existing_home_team.name}")
                    logging.info(f"  Updated values - Conference: '{existing_home_team.conference}', Region: '{existing_home_team.region}', Division: '{existing_home_team.division}'")
                else:
                    home_team = Team(
                        id=home_team_data['id'],
                        name=home_team_data['name'],
                        abbreviation=home_team_data.get('abbreviation'),
                        division=home_team_data.get('division'),
                        conference=home_team_data.get('conference'),
                        region=home_team_data.get('region'),
                        typename=home_team_data.get('__typename'),
                        gender=match_data['gender']
                    )
                    logging.info(f"CREATING NEW TEAM (from teams) - ID: {home_team.id}, Name: {home_team.name}")
                    logging.info(f"  Initial values - Conference: '{home_team.conference}', Region: '{home_team.region}', Division: '{home_team.division}'")
                    session.merge(home_team)
                    home_team_id = home_team.id
                
                match_data['homeTeam'] = home_team_data  # Update match_data for later use

            # Find and process away team
            if len(teams) > 1:
                away_team_data = next(
                    (team for team in teams if team['id'] != home_team_id),
                    teams[1]  # Fallback to second team if no other found
                )
                
                if away_team_data:
                    # Check if team exists first to preserve existing data
                    existing_away_team = session.query(Team).filter(
                        func.upper(Team.id) == func.upper(away_team_data['id'])
                    ).first()
                    
                    if existing_away_team:
                        # Log the existing values before update
                        logging.info(f"AWAY TEAM BEFORE UPDATE - ID: {existing_away_team.id}, Name: {existing_away_team.name}")
                        logging.info(f"  Current values - Conference: '{existing_away_team.conference}', Region: '{existing_away_team.region}', Division: '{existing_away_team.division}'")
                        
                        # Log the incoming values from the API
                        logging.info(f"  API values - Conference: '{away_team_data.get('conference')}', Region: '{away_team_data.get('region')}', Division: '{away_team_data.get('division')}'")
                        
                        # Only update name which should always be present
                        existing_away_team.name = away_team_data['name']
                        
                        # Log evaluation results for each field
                        if away_team_data.get('abbreviation'):
                            existing_away_team.abbreviation = away_team_data['abbreviation']
                        else:
                            logging.warning(f"  SKIPPED UPDATE - Team {existing_away_team.id}: abbreviation value '{away_team_data.get('abbreviation')}' evaluates to False")
                        
                        if away_team_data.get('division'):
                            existing_away_team.division = away_team_data['division']
                        else:
                            logging.warning(f"  SKIPPED UPDATE - Team {existing_away_team.id}: division value '{away_team_data.get('division')}' evaluates to False")
                        
                        if away_team_data.get('conference'):
                            existing_away_team.conference = away_team_data['conference']
                        else:
                            logging.warning(f"  SKIPPED UPDATE - Team {existing_away_team.id}: conference value '{away_team_data.get('conference')}' evaluates to False")
                        
                        if away_team_data.get('region'):
                            existing_away_team.region = away_team_data['region']
                        else:
                            logging.warning(f"  SKIPPED UPDATE - Team {existing_away_team.id}: region value '{away_team_data.get('region')}' evaluates to False")
                            
                        # Always update gender
                        existing_away_team.gender = match_data['gender']
                        session.merge(existing_away_team)
                        away_team_id = existing_away_team.id
                        
                        # Log the values after update for verification
                        logging.info(f"AWAY TEAM AFTER UPDATE - ID: {existing_away_team.id}, Name: {existing_away_team.name}")
                        logging.info(f"  Updated values - Conference: '{existing_away_team.conference}', Region: '{existing_away_team.region}', Division: '{existing_away_team.division}'")
                    else:
                        away_team = Team(
                            id=away_team_data['id'],
                            name=away_team_data['name'],
                            abbreviation=away_team_data.get('abbreviation'),
                            division=away_team_data.get('division'),
                            conference=away_team_data.get('conference'),
                            region=away_team_data.get('region'),
                            typename=away_team_data.get('__typename'),
                            gender=match_data['gender']
                        )
                        logging.info(f"CREATING NEW AWAY TEAM - ID: {away_team.id}, Name: {away_team.name}")
                        logging.info(f"  Initial values - Conference: '{away_team.conference}', Region: '{away_team.region}', Division: '{away_team.division}'")
                        session.merge(away_team)
                        away_team_id = away_team.id
            
            # Validate that we have both team IDs before proceeding
            if not home_team_id or not away_team_id:
                raise ValueError(
                    f"Invalid team IDs for match {match_data['id']}: "
                    f"home={home_team_id}, away={away_team_id}"
                )

            # Process and store match
            utc_time = datetime.fromisoformat(match_data['startDateTime']['dateTimeString'].replace('Z', '+00:00'))
            local_tz = pytz.timezone(match_data['startDateTime']['timezoneName'])
            start_date = utc_time.astimezone(local_tz)   

            # Set season to the previous year since matches are in spring
            season_year = str(start_date.year - 1)
            
            match = Match(
                id=match_data['id'],
                start_date=start_date,
                timezone=match_data['startDateTime']['timezoneName'],
                no_scheduled_time=match_data['startDateTime']['noScheduledTime'],
                is_conference_match=match_data['isConferenceMatch'],
                gender=match_data['gender'],
                typename=match_data.get('__typename'),
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                season=season_year,
                side_numbers=len(teams),
                completed=any(team.get('score') is not None for team in teams),
                scheduled_time=start_date if not match_data['startDateTime']['noScheduledTime'] else None
            )
            session.merge(match)

            # Process and store match_teams relationships with validated team info
            for team_data in teams:
                is_home = team_data['id'] == home_team_id
                team_match = MatchTeam(
                    match_id=match.id,
                    team_id=team_data['id'],
                    score=team_data.get('score'),
                    did_win=team_data.get('didWin'),
                    side_number=team_data.get('sideNumber'),
                    is_home_team=is_home,
                    order_of_play=team_data.get('sideNumber'),
                    team_position='home' if is_home else 'away'
                )
                session.merge(team_match)

            # Process and store web links
            for link_data in match_data.get('webLinks', []):
                web_link = WebLink(
                    match_id=match.id,
                    name=link_data['name'],
                    url=link_data['url'],
                    typename=link_data.get('__typename')
                )
                session.merge(web_link)

            session.commit()
            logging.info(f"Successfully stored match {match_data['id']}")

        except Exception as e:
            logging.error(f"Error storing match: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def store_team_roster(self, school_id: str, team_id: str, season_id: str):
        """Store roster information for a team"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            roster_data = self.fetch_roster_members(team_id, season_id)
                        
            if roster_data and 'data' in roster_data and 'getRosterMembers' in roster_data['data']:
                players = roster_data['data']['getRosterMembers']
                print(f"Found {len(players)} players to process")

                for player_info in players:
                    try:
                        # Store or update base player info
                        player = session.query(Player).get(player_info['personId'])
                        if not player:
                            player = Player(person_id=player_info['personId'])
                        
                        player.tennis_id = player_info['tennisId']
                        player.first_name = player_info['standardGivenName']
                        player.last_name = player_info['standardFamilyName']
                        player.avatar_url = player_info['avatarUrl']
                        
                        session.merge(player)
                        
                        # Create or update player season info
                        player_season = (
                            session.query(PlayerSeason)
                            .filter_by(person_id=player.person_id, season_id=season_id)
                            .first()
                        )
                        if not player_season:
                            player_season = PlayerSeason(
                                person_id=player.person_id,
                                tennis_id=player.tennis_id,
                                season_id=season_id,
                                class_year=player_info['class']
                            )
                        else:
                            player_season.tennis_id = player.tennis_id
                            player_season.class_year = player_info['class']
                        
                        session.merge(player_season)

                        # Create or update roster entry
                        roster_entry = (
                            session.query(PlayerRoster)
                            .filter_by(person_id=player.person_id, season_id=season_id)
                            .first()
                        )
                        if not roster_entry:
                            roster_entry = PlayerRoster(
                                person_id=player.person_id,
                                tennis_id=player.tennis_id,
                                season_id=season_id,
                                team_id=team_id,
                                school_id=school_id
                            )
                        else:
                            roster_entry.tennis_id = player.tennis_id
                            roster_entry.team_id = team_id
                            roster_entry.school_id = school_id
                        
                        session.merge(roster_entry)
                        
                        # Store WTN numbers
                        if player_info['worldTennisNumbers']:
                            for wtn in player_info['worldTennisNumbers']:
                                wtn_entry = (
                                    session.query(PlayerWTN)
                                    .filter_by(
                                        person_id=player.person_id,
                                        season_id=season_id,
                                        wtn_type=wtn['type']
                                    )
                                    .first()
                                )
                                if not wtn_entry:
                                    wtn_entry = PlayerWTN(
                                        person_id=player.person_id,
                                        tennis_id=player.tennis_id,
                                        season_id=season_id,
                                        wtn_type=wtn['type'],
                                        confidence=wtn['confidence'],
                                        tennis_number=wtn['tennisNumber'],
                                        is_ranked=wtn['isRanked']
                                    )
                                else:
                                    wtn_entry.tennis_id = player.tennis_id
                                    wtn_entry.confidence = wtn['confidence']
                                    wtn_entry.tennis_number = wtn['tennisNumber']
                                    wtn_entry.is_ranked = wtn['isRanked']
                                
                                session.merge(wtn_entry)
                        
                        session.commit()
                        print(f"Successfully stored player: {player.first_name} {player.last_name} for season {season_id}")
                        
                    except Exception as e:
                        print(player_info)
                        print(f"Error processing player {player_info.get('standardGivenName', '')} {player_info.get('standardFamilyName', '')}: {e}")
                        session.rollback()
                        continue
                        
        except Exception as e:
            print(f"Error processing roster: {e}")
            session.rollback()
        finally:
            session.close()

    def process_completed_not_catched_matches(self):
        """Process completed matches that were not caught by the initial fetch"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            # Get matches without scores where match date is less than today
            matches = session.query(Match).filter(
                Match.completed == True,
                Match.home_team_score == None,
                Match.away_team_score == None
            ).all()
            
            print(f"Found {len(matches)} matches without scores to process")
            
            for match in matches:
                try:
                    # Fetch match data from API
                    match_data = self.fetch_single_match(match.id)
                    if not match_data:
                        continue
                        
                    # Update match with new data
                    self.update_single_match(match, match_data)
                    
                except Exception as e:
                    print(f"Error processing match {match.id}: {e}")
                    session.rollback()
                    continue
                    
            session.commit()
            return len(matches)
            
        except Exception as e:
            print(f"Error processing matches: {e}")
            session.rollback()
            return 0
        finally:
            session.close()
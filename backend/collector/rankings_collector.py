# collector/rankings_collector.py
import requests
import json
from datetime import datetime
from sqlalchemy import create_engine, and_,func
from sqlalchemy.orm import sessionmaker
from pathlib import Path
import sys
import logging

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from models.models import (
    Base, RankingList, TeamRanking, Team, 
    PlayerRankingList, PlayerRanking, Player,DoublesRanking
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rankings_updates.log'),
        logging.StreamHandler()
    ]
)

class RankingsCollector:
    def __init__(self, database_url: str):
        self.api_url = 'https://prd-itat-kube.clubspark.pro/mesh-api/graphql'
        
        # Standard headers for API requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.5',
            'Content-Type': 'application/json',
            'Origin': 'https://www.collegetennis.com',
            'Referer': 'https://www.collegetennis.com/',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
        }
        
        # Initialize database
        try:
            self.engine = create_engine(database_url)
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)
            logging.info("Database connection established")
        except Exception as e:
            logging.error(f"Database initialization error: {e}")
            self.engine = None
            self.Session = None
    
    def fetch_ranking_lists(self, division_type="DIV1", gender="M", match_format="TEAM"):
        """Fetch available ranking lists metadata"""
        query = """
        query td_RankListsPublishDate($sort: td_SortOrder, $filter: td_RankListFilter) {
            td_rankLists(sort: $sort, filter: $filter) {
                items {
                    id
                    publishDate
                    plannedPublishDate
                    __typename
                }
                __typename
            }
        }
        """
        
        variables = {
            "filter": {
                "visible": True,
                "matchFormat": match_format,
                "divisionType": division_type,
                "gender": gender,
                "listType": "STANDING"
            },
            "sort": {
                "field": "plannedPublishDate",
                "direction": "DESC"
            }
        }
        
        try:
            logging.info(f"Fetching ranking lists for {division_type} {gender} {match_format}")
            response = requests.post(
                self.api_url,
                json={
                    'operationName': 'td_RankListsPublishDate',
                    'query': query,
                    'variables': variables
                },
                headers=self.headers,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'td_rankLists' in data['data'] and 'items' in data['data']['td_rankLists']:
                    rank_lists = data['data']['td_rankLists']['items']
                    logging.info(f"Found {len(rank_lists)} ranking lists")
                    return rank_lists
            
            logging.error(f"Failed to fetch ranking lists. Status code: {response.status_code}")
            return []
            
        except Exception as e:
            logging.error(f"Error fetching ranking lists: {e}")
            return []
    
    def fetch_ranking_details(self, ranking_id: str):
        """Fetch detailed ranking information for a specific ranking list"""
        query = """
        query td_RankListById($id: String!, $itemPageArgs: td_PaginationArgs) {
            td_rankList(id: $id) {
                id
                createdAt
                divisionType
                gender
                matchFormat
                dateRange {
                    start
                    end
                    __typename
                }
                rankingItems(itemPageArgs: $itemPageArgs) {
                    totalItems
                    items {
                        rank
                        points {
                            total
                            __typename
                        }
                        wins {
                            total
                            __typename
                        }
                        losses {
                            total
                            __typename
                        }
                        participants {
                            participantType
                            itemId
                            name
                            state
                            __typename
                        }
                        conference
                        __typename
                    }
                    __typename
                }
                __typename
            }
        }
        """
        
        variables = {
            "id": ranking_id,
            "itemPageArgs": {
                "limit": 125  # Increased to handle more players
            }
        }
        
        try:
            logging.info(f"Fetching details for ranking list {ranking_id}")
            response = requests.post(
                self.api_url,
                json={
                    'operationName': 'td_RankListById',
                    'query': query,
                    'variables': variables
                },
                headers=self.headers,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'td_rankList' in data['data']:
                    ranking_data = data['data']['td_rankList']
                    logging.info(f"Successfully fetched ranking details for {ranking_id}")
                    return ranking_data
            
            logging.error(f"Failed to fetch ranking details. Status code: {response.status_code}")
            return None
            
        except Exception as e:
            logging.error(f"Error fetching ranking details: {e}")
            return None
    

    def store_team_ranking_list(self, ranking_data):
        """Store team ranking list metadata and team rankings"""
        if not self.Session:
            logging.error("Database not initialized")
            return False
        
        session = self.Session()
        try:
            # Extract ranking list metadata
            ranking_id = ranking_data['id']
            created_at = datetime.fromisoformat(ranking_data['createdAt'].replace('Z', '+00:00'))
            division_type = ranking_data['divisionType']
            gender = ranking_data['gender']
            match_format = ranking_data['matchFormat']
            date_range_start = datetime.fromisoformat(ranking_data['dateRange']['start'].replace('Z', '+00:00'))
            date_range_end = datetime.fromisoformat(ranking_data['dateRange']['end'].replace('Z', '+00:00'))
            
            # Check if ranking list already exists
            existing_list = session.query(RankingList).get(ranking_id)
            
            if existing_list:
                logging.info(f"Ranking list {ranking_id} already exists, skipping...")
                return False
            
            # Create new ranking list
            logging.info(f"Creating new team ranking list entry for {ranking_id}")
            ranking_list = RankingList(
                id=ranking_id,
                division_type=division_type,
                gender=gender,
                match_format=match_format,
                date_range_start=date_range_start,
                date_range_end=date_range_end,
                created_at=created_at
                # The publish date will be set when we fetch the list of ranking lists
            )
            session.add(ranking_list)
            
            # Extract and store team rankings
            if 'rankingItems' in ranking_data and 'items' in ranking_data['rankingItems']:
                items = ranking_data['rankingItems']['items']
                success_count = 0
                
                for item in items:
                    try:
                        # Get team info
                        if not item['participants'] or len(item['participants']) == 0:
                            logging.warning(f"No participant data for ranking item: {item['rank']}")
                            continue
                        
                        participant = item['participants'][0]
                        team_id = participant['itemId']
                        team_name = participant['name']
                        conference = item.get('conference')
                        
                        # Find existing team with case-insensitive comparison
                        existing_team = session.query(Team).filter(
                            func.upper(Team.id) == func.upper(team_id)
                        ).first()
                        
                        if existing_team:
                            # Use the existing team's ID with correct case
                            actual_team_id = existing_team.id
                            
                            # Create team ranking entry
                            team_ranking = TeamRanking(
                                ranking_list_id=ranking_id,
                                team_id=actual_team_id,  # Use the ID with correct case
                                rank=item['rank'],
                                points=item['points']['total'],
                                wins=item['wins']['total'],
                                losses=item['losses']['total'],
                                team_name=team_name,
                                conference=conference
                            )
                            session.add(team_ranking)
                            success_count += 1
                        else:
                            logging.warning(f"Team {team_name} (ID: {team_id}) not found, skipping ranking")
                    except Exception as e:
                        logging.error(f"Error processing team ranking for {team_name}: {e}")
                
                session.commit()
                logging.info(f"Successfully stored team ranking list {ranking_id} with {success_count} team rankings")
                return True
            
        except Exception as e:
            logging.error(f"Error storing team ranking list: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def store_player_ranking_list(self, ranking_data):
        """Store player ranking list metadata and player rankings"""
        if not self.Session:
            logging.error("Database not initialized")
            return False
        
        session = self.Session()
        try:
            # Extract ranking list metadata
            ranking_id = ranking_data['id']
            created_at = datetime.fromisoformat(ranking_data['createdAt'].replace('Z', '+00:00'))
            division_type = ranking_data['divisionType']
            gender = ranking_data['gender']
            match_format = ranking_data['matchFormat']
            date_range_start = datetime.fromisoformat(ranking_data['dateRange']['start'].replace('Z', '+00:00'))
            date_range_end = datetime.fromisoformat(ranking_data['dateRange']['end'].replace('Z', '+00:00'))
            
            # Check if ranking list already exists
            existing_list = session.query(PlayerRankingList).get(ranking_id)
            
            if existing_list:
                logging.info(f"Player ranking list {ranking_id} already exists, skipping...")
                return False
            
            # Create new ranking list
            logging.info(f"Creating new player ranking list entry for {ranking_id}")
            ranking_list = PlayerRankingList(
                id=ranking_id,
                division_type=division_type,
                gender=gender,
                match_format=match_format,
                date_range_start=date_range_start,
                date_range_end=date_range_end,
                created_at=created_at
            )
            session.add(ranking_list)
            session.flush()  # Flush to get the ID
            
            # Extract and store player rankings
            success_count = 0
            
            if 'rankingItems' in ranking_data and 'items' in ranking_data['rankingItems']:
                items = ranking_data['rankingItems']['items']
                
                for item in items:
                    try:
                        # Get participants info
                        if not item['participants'] or len(item['participants']) < 2:
                            logging.warning(f"Incomplete participant data for ranking item: {item['rank']}")
                            continue
                        
                        # Find team and player participants
                        team_participant = None
                        player_participant = None
                        
                        for participant in item['participants']:
                            if participant['participantType'] == 'TEAM':
                                team_participant = participant
                            elif participant['participantType'] == 'INDIVIDUAL':
                                player_participant = participant
                        
                        if not team_participant or not player_participant:
                            logging.warning(f"Missing team or player data for ranking item: {item['rank']}")
                            continue
                        
                        team_id = team_participant['itemId']
                        team_name = team_participant['name']
                        player_id = player_participant['itemId']
                        player_name = player_participant['name']
                        conference = item.get('conference')
                        
                        # Find existing team with case-insensitive comparison
                        existing_team = session.query(Team).filter(
                            func.upper(Team.id) == func.upper(team_id)
                        ).first()
                        
                        # Find existing player with case-insensitive comparison
                        existing_player = session.query(Player).filter(
                            func.upper(Player.person_id) == func.upper(player_id)
                        ).first()
                        
                        if existing_team and existing_player:
                            # Create player ranking entry using correct case for IDs
                            player_ranking = PlayerRanking(
                                ranking_list_id=ranking_id,
                                player_id=existing_player.person_id,  # Use correct case
                                team_id=existing_team.id,  # Use correct case
                                rank=item['rank'],
                                points=item['points']['total'],
                                wins=item['wins']['total'],
                                losses=item['losses']['total'],
                                player_name=player_name,
                                team_name=team_name,
                                conference=conference
                            )
                            session.add(player_ranking)
                            success_count += 1
                        else:
                            if not existing_team:
                                logging.warning(f"Team {team_name} (ID: {team_id}) not found, skipping")
                            if not existing_player:
                                logging.warning(f"Player {player_name} (ID: {player_id}) not found, skipping")
                    except Exception as e:
                        logging.error(f"Error processing player ranking: {e}")
                        continue
                
                session.commit()
                logging.info(f"Successfully stored player ranking list {ranking_id} with {success_count} player rankings")
                return True
                
        except Exception as e:
            logging.error(f"Error storing player ranking list: {e}")
            session.rollback()
            return False
        finally:
            session.close()
    
    def update_team_ranking_publish_dates(self, ranking_lists):
        """Update publish dates for team ranking lists"""
        if not self.Session:
            logging.error("Database not initialized")
            return
        
        session = self.Session()
        try:
            for rank_list in ranking_lists:
                ranking_id = rank_list['id']
                
                # Check if publishDate is None
                if rank_list['publishDate'] is None:
                    logging.warning(f"Ranking list {ranking_id} has no publish date, skipping...")
                    continue
                    
                publish_date = datetime.fromisoformat(rank_list['publishDate'].replace('Z', '+00:00'))
                planned_date = datetime.strptime(rank_list['plannedPublishDate'], '%Y-%m-%d').date()
                
                # Get or create ranking list
                ranking = session.query(RankingList).get(ranking_id)
                if ranking:
                    ranking.publish_date = publish_date
                    ranking.planned_publish_date = planned_date
                    session.merge(ranking)
                    logging.debug(f"Updated publish dates for team ranking list {ranking_id}")
                
            session.commit()
            logging.info(f"Updated publish dates for {len(ranking_lists)} team ranking lists")
        except Exception as e:
            logging.error(f"Error updating team publish dates: {e}")
            session.rollback()
        finally:
            session.close()
    
    def update_player_ranking_publish_dates(self, ranking_lists):
        """Update publish dates for player ranking lists"""
        if not self.Session:
            logging.error("Database not initialized")
            return
        
        session = self.Session()
        try:
            for rank_list in ranking_lists:
                ranking_id = rank_list['id']
                
                # Check if publishDate is None
                if rank_list.get('publishDate') is None:
                    logging.warning(f"Ranking list {ranking_id} has no publish date, skipping...")
                    continue
                    
                publish_date = datetime.fromisoformat(rank_list['publishDate'].replace('Z', '+00:00'))
                planned_date = datetime.strptime(rank_list['plannedPublishDate'], '%Y-%m-%d').date() if rank_list.get('plannedPublishDate') else None
                
                # Get or create ranking list
                ranking = session.query(PlayerRankingList).get(ranking_id)
                if ranking:
                    ranking.publish_date = publish_date
                    if planned_date:
                        ranking.planned_publish_date = planned_date
                    session.merge(ranking)
                    logging.debug(f"Updated publish dates for player ranking list {ranking_id}")
                
            session.commit()
            logging.info(f"Updated publish dates for {len(ranking_lists)} player ranking lists")
        except Exception as e:
            logging.error(f"Error updating player publish dates: {e}")
            session.rollback()
        finally:
            session.close()
    
    def collect_team_rankings(self, max_lists_to_process=None, genders=None):
        """Collect and store team rankings
        
        Args:
            max_lists_to_process: Optional limit on number of lists to process (None for all)
            genders: List of genders to collect rankings for, defaults to ['M', 'F'] for both
        """
        if genders is None:
            genders = ['M', 'F']  # Default to both men's and women's rankings
        
        for gender in genders:
            gender_label = "men's" if gender == 'M' else "women's"
            logging.info(f"Starting collection of {gender_label} team rankings")
            
            # 1. Fetch available ranking lists for this gender
            rank_lists = self.fetch_ranking_lists(match_format="TEAM", gender=gender)
            if not rank_lists:
                logging.error(f"Failed to fetch {gender_label} team ranking lists")
                continue
            
            # 2. Update publish dates for all ranking lists
            self.update_team_ranking_publish_dates(rank_lists)
            
            # 3. Process ALL ranking lists (or max_lists_to_process if specified)
            lists_to_process = rank_lists[:max_lists_to_process] if max_lists_to_process else rank_lists
            
            processed_count = 0
            for rank_list in lists_to_process:
                ranking_id = rank_list['id']
                
                # Check if we already have this ranking list stored
                session = self.Session()
                try:
                    existing_rankings = session.query(TeamRanking).filter_by(ranking_list_id=ranking_id).count()
                    if existing_rankings > 0:
                        logging.info(f"Ranking list {ranking_id} already has team rankings, skipping...")
                        continue
                finally:
                    session.close()
                
                # Fetch and store detailed ranking information
                ranking_data = self.fetch_ranking_details(ranking_id)
                if ranking_data:
                    success = self.store_team_ranking_list(ranking_data)
                    if success:
                        processed_count += 1
                
            logging.info(f"Processed {processed_count} {gender_label} team ranking lists out of {len(lists_to_process)} total lists")
    
    def collect_singles_rankings(self, max_lists_to_process=None, genders=None):
        """Collect and store singles rankings for specified genders
        
        Args:
            max_lists_to_process: Optional limit on number of lists to process (None for all)
            genders: List of genders to collect rankings for, defaults to ['M', 'F'] for both
        """
        if genders is None:
            genders = ['M', 'F']  # Default to both men's and women's rankings
        
        for gender in genders:
            gender_label = "men's" if gender == 'M' else "women's"
            logging.info(f"Starting collection of {gender_label} singles rankings")
            
            # 1. Fetch available ranking lists for this gender
            rank_lists = self.fetch_ranking_lists(match_format="SINGLES", gender=gender)
            if not rank_lists:
                logging.error(f"Failed to fetch {gender_label} singles ranking lists")
                continue
            
            # 2. Update publish dates for all ranking lists
            self.update_player_ranking_publish_dates(rank_lists)
            
            # 3. Process ALL ranking lists (or max_lists_to_process if specified)
            lists_to_process = rank_lists
            if max_lists_to_process is not None:
                lists_to_process = rank_lists[:max_lists_to_process]
            
            processed_count = 0
            for rank_list in lists_to_process:
                ranking_id = rank_list['id']
                
                # Check if we already have this ranking list stored
                session = self.Session()
                try:
                    existing_rankings = session.query(PlayerRanking).filter_by(ranking_list_id=ranking_id).count()
                    if existing_rankings > 0:
                        logging.info(f"Ranking list {ranking_id} already has singles rankings, skipping...")
                        continue
                finally:
                    session.close()
                
                # Fetch and store detailed ranking information
                ranking_data = self.fetch_ranking_details(ranking_id)
                if ranking_data:
                    success = self.store_player_ranking_list(ranking_data)
                    if success:
                        processed_count += 1
            
            logging.info(f"Processed {processed_count} {gender_label} singles ranking lists out of {len(lists_to_process)} selected lists")
    
    def collect_doubles_rankings(self, max_lists_to_process=None, genders=None):
        """Collect and store doubles rankings for specified genders
        
        Args:
            max_lists_to_process: Optional limit on number of lists to process (None for all)
            genders: List of genders to collect rankings for, defaults to ['M', 'F'] for both
        """
        if genders is None:
            genders = ['M', 'F']  # Default to both men's and women's rankings
        
        for gender in genders:
            gender_label = "men's" if gender == 'M' else "women's"
            logging.info(f"Starting collection of {gender_label} doubles rankings")
            
            # 1. Fetch available ranking lists for this gender
            rank_lists = self.fetch_ranking_lists(match_format="DOUBLES", gender=gender)
            if not rank_lists:
                logging.error(f"Failed to fetch {gender_label} doubles ranking lists")
                continue
            
            # 2. Update publish dates for all ranking lists
            self.update_player_ranking_publish_dates(rank_lists)
            
            # 3. Process ALL ranking lists (or max_lists_to_process if specified)
            lists_to_process = rank_lists
            if max_lists_to_process is not None:
                lists_to_process = rank_lists[:max_lists_to_process]
            
            processed_count = 0
            for rank_list in lists_to_process:
                ranking_id = rank_list['id']
                
                # Check if we already have this ranking list stored
                # session = self.Session()
                # try:
                #     existing_rankings = session.query(DoublesRanking).filter_by(ranking_list_id=ranking_id).count()
                #     if existing_rankings > 0:
                #         logging.info(f"Ranking list {ranking_id} already has doubles rankings, skipping...")
                #         continue
                # finally:
                #     session.close()
                
                # Fetch and store detailed ranking information
                ranking_data = self.fetch_ranking_details(ranking_id)
                if ranking_data:
                    # Use the new store_doubles_ranking_list method instead of store_player_ranking_list
                    success = self.store_doubles_ranking_list(ranking_data)
                    if success:
                        processed_count += 1
            
            logging.info(f"Processed {processed_count} {gender_label} doubles ranking lists out of {len(lists_to_process)} selected lists")
    
    def collect_all_rankings(self, max_lists_to_process=None,genders=None):
        """Collect all types of rankings (team, singles, doubles)"""
        logging.info("Starting collection of team rankings")
        self.collect_team_rankings(max_lists_to_process, genders)
        
        logging.info("Starting collection of singles rankings")
        self.collect_singles_rankings(max_lists_to_process)
        
        logging.info("Starting collection of doubles rankings")
        self.collect_doubles_rankings(max_lists_to_process)
        
        logging.info("Completed collection of all ranking types")

    def store_doubles_ranking_list(self, ranking_data):
        """Store doubles ranking list metadata and rankings"""
        if not self.Session:
            logging.error("Database not initialized")
            return False
        
        session = self.Session()
        try:
            # Extract ranking list metadata
            ranking_id = ranking_data['id']
            created_at = datetime.fromisoformat(ranking_data['createdAt'].replace('Z', '+00:00'))
            division_type = ranking_data['divisionType']
            gender = ranking_data['gender']
            match_format = ranking_data['matchFormat']
            date_range_start = datetime.fromisoformat(ranking_data['dateRange']['start'].replace('Z', '+00:00'))
            date_range_end = datetime.fromisoformat(ranking_data['dateRange']['end'].replace('Z', '+00:00'))
            
            # Check if ranking list already exists in PlayerRankingList
            existing_list = session.query(PlayerRankingList).get(ranking_id)
            
            if existing_list:
                # Check if there are existing entries in DoublesRanking
                existing_doubles = session.query(DoublesRanking).filter_by(
                    ranking_list_id=ranking_id
                ).count()
                
                if existing_doubles > 0:
                    logging.info(f"Ranking list {ranking_id} already has doubles rankings, skipping...")
                    return False
                
                logging.info(f"Ranking list {ranking_id} exists but has no doubles rankings. Processing...")
            else:
                # Create new ranking list
                ranking_list = PlayerRankingList(
                    id=ranking_id,
                    division_type=division_type,
                    gender=gender,
                    match_format=match_format,
                    date_range_start=date_range_start,
                    date_range_end=date_range_end,
                    created_at=created_at
                )
                session.add(ranking_list)
                session.flush()
            
            # Extract and store doubles rankings
            success_count = 0
            
            if 'rankingItems' in ranking_data and 'items' in ranking_data['rankingItems']:
                items = ranking_data['rankingItems']['items']
                
                for item in items:
                    try:
                        # Get participants info
                        if not item['participants'] or len(item['participants']) < 3:
                            continue
                        
                        # Find team and player participants
                        team_participant = None
                        player_participants = []
                        
                        for participant in item['participants']:
                            if participant['participantType'] == 'TEAM':
                                team_participant = participant
                            elif participant['participantType'] == 'INDIVIDUAL':
                                player_participants.append(participant)
                        
                        if not team_participant or len(player_participants) < 2:
                            continue
                        
                        team_id = team_participant['itemId']
                        team_name = team_participant['name']
                        player1_id = player_participants[0]['itemId']
                        player1_name = player_participants[0]['name']
                        player2_id = player_participants[1]['itemId']
                        player2_name = player_participants[1]['name']
                        conference = item.get('conference')
                        
                        # Find existing team with case-insensitive comparison
                        existing_team = session.query(Team).filter(
                            func.upper(Team.id) == func.upper(team_id)
                        ).first()
                        
                        # Find existing players with case-insensitive comparison
                        existing_player1 = session.query(Player).filter(
                            func.upper(Player.person_id) == func.upper(player1_id)
                        ).first()
                        
                        existing_player2 = session.query(Player).filter(
                            func.upper(Player.person_id) == func.upper(player2_id)
                        ).first()
                        
                        if existing_team and existing_player1 and existing_player2:
                            # Create doubles ranking entry
                            doubles_ranking = DoublesRanking(
                                ranking_list_id=ranking_id,
                                team_id=existing_team.id,
                                player1_id=existing_player1.person_id,
                                player2_id=existing_player2.person_id,
                                rank=item['rank'],
                                points=item['points']['total'],
                                wins=item['wins']['total'],
                                losses=item['losses']['total'],
                                player1_name=player1_name,
                                player2_name=player2_name,
                                team_name=team_name,
                                conference=conference
                            )
                            session.add(doubles_ranking)
                            success_count += 1
                    except Exception as e:
                        logging.error(f"Error processing doubles ranking: {e}")
                        continue
                
                session.commit()
                logging.info(f"Successfully stored doubles ranking list {ranking_id} with {success_count} doubles rankings")
                return True
                
        except Exception as e:
            logging.error(f"Error storing doubles ranking list: {e}")
            session.rollback()
            return False
        finally:
            session.close()
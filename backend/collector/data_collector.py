# src/data_collector.py
import asyncio
import httpx
from datetime import datetime
from sqlalchemy import create_engine, func, distinct
from sqlalchemy.orm import sessionmaker
from models.models import (
    Base, Match, Team, MatchTeam, WebLink, TeamLogo, SchoolInfo, Season, 
    Player, PlayerRoster, PlayerWTN, PlayerSeason, PlayerMatch, PlayerMatchSet, 
    PlayerMatchParticipant, MatchLineup, MatchLineupSet
)
import requests
import traceback
from uuid import uuid4
import time


class TennisDataCollector:
    def __init__(self, database_url=None):
        self.api_url = 'https://prd-itat-kube-tournamentdesk-api.clubspark.pro/'
        
        # Only initialize database if URL is provided
        if database_url:
            try:
                self.engine = create_engine(database_url)
                Base.metadata.create_all(self.engine)
                self.Session = sessionmaker(bind=self.engine)
            except Exception as e:
                print(f"Database initialization skipped: {e}")
                self.engine = None
                self.Session = None
######### store team matches data #########     
    async def get_total_matches(self):
        query = """query dualMatchesPaginated($skip: Int!, $limit: Int!, $filter: DualMatchesFilter, $sort: DualMatchesSort) {
        dualMatchesPaginated(skip: $skip, limit: $limit, filter: $filter, sort: $sort) {
            totalItems
        }
        }"""
        
        variables = {
            "skip": 0,
            "limit": 1,
            "sort": {
                "field": "START_DATE",
                "direction": "DESCENDING"
            },
            "filter": {
                "seasonStarting": "2024",
                "isCompleted": True,
                "divisions": ["DIVISION_1"]
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                json={
                    'operationName': 'dualMatchesPaginated',
                    'query': query,
                    'variables': variables
                }
            )
            data = response.json()
            return data['data']['dualMatchesPaginated']['totalItems']

    async def fetch_all_matches(self):
        all_matches = []
        skip = 0
        limit = 100
        max_retries = 3
        
        # Define the query outside the loop
        query = """query dualMatchesPaginated($skip: Int!, $limit: Int!, $filter: DualMatchesFilter, $sort: DualMatchesSort) {
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
            __typename
        }
        }"""
        
        while True:
            retry_count = 0
            while retry_count < max_retries:
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            self.api_url,
                            json={
                                'operationName': 'dualMatchesPaginated',
                                'query': query,
                                'variables': {
                                    "skip": skip,
                                    "limit": limit,
                                    "sort": {
                                        "field": "START_DATE",
                                        "direction": "DESCENDING"
                                    },
                                    "filter": {
                                        "seasonStarting": "2023",
                                        "isCompleted": True,
                                        "divisions": ["DIVISION_1"]
                                    }
                                }
                            },
                            timeout=30.0
                        )
                        
                        if response.status_code == 200:
                            try:
                                data = response.json()
                                matches_batch = data['data']['dualMatchesPaginated']['items']
                                total_items = data['data']['dualMatchesPaginated']['totalItems']
                                
                                all_matches.extend(matches_batch)
                                print(f"Fetched matches {skip+1} to {skip+len(matches_batch)} of {total_items}")
                                
                                if len(matches_batch) < limit or len(all_matches) >= total_items:
                                    return {'data': {'dualMatchesPaginated': {'items': all_matches}}}
                                
                                skip += limit
                                await asyncio.sleep(2)
                                break
                                
                            except (KeyError, json.JSONDecodeError) as e:
                                print(f"Error parsing response: {e}")
                                retry_count += 1
                                await asyncio.sleep(5)
                        else:
                            print(f"Request failed with status code: {response.status_code}")
                            retry_count += 1
                            await asyncio.sleep(5)
                            
                except Exception as e:
                    print(f"Request error: {e}")
                    retry_count += 1
                    await asyncio.sleep(5)
            
            if retry_count == max_retries:
                print(f"Failed to fetch matches after {max_retries} retries. Saving what we have so far...")
                return {'data': {'dualMatchesPaginated': {'items': all_matches}}}

    def save_team(session, team_data):
        team = Team(
            id=team_data['id'],
            name=team_data['name'],
            abbreviation=team_data.get('abbreviation'),
            division=team_data.get('division'),
            conference=team_data.get('conference'),
            region=team_data.get('region')
        )
        session.merge(team)  # Use merge to handle duplicates gracefully
        return team

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
                    session.merge(home_team)
                    home_team_id = home_team.id
                else:
                    # If no home team specified, use first team from teams list
                    home_team_data = teams[0]
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
                        session.merge(away_team)
                        away_team_id = away_team.id

                # Validate that we have both team IDs before proceeding
                if not home_team_id or not away_team_id:
                    raise ValueError(
                        f"Invalid team IDs for match {match_data['id']}: "
                        f"home={home_team_id}, away={away_team_id}"
                    )

                # Process and store match
                start_date = datetime.fromisoformat(match_data['startDateTime']['dateTimeString'].replace('Z', '+00:00'))
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

            except Exception as e:
                print(f"Error storing match: {e}")
                session.rollback()
                raise
            finally:
                session.close()

    async def get_total_upcoming_matches(self):
        query = """query dualMatchesPaginated($skip: Int!, $limit: Int!, $filter: DualMatchesFilter, $sort: DualMatchesSort) {
        dualMatchesPaginated(skip: $skip, limit: $limit, filter: $filter, sort: $sort) {
            totalItems
        }
        }"""
        
        variables = {
            "skip": 0,
            "limit": 1,
            "sort": {
                "field": "START_DATE",
                "direction": "ASCENDING"
            },
            "filter": {
                "seasonStarting": "2024",
                "startDate": {
                    "gte": datetime.now().strftime("%Y-%m-%d")
                },
                "isCompleted": False,
                "divisions": ["DIVISION_1"]
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.api_url,
                json={
                    'operationName': 'dualMatchesPaginated',
                    'query': query,
                    'variables': variables
                }
            )
            data = response.json()
            return data['data']['dualMatchesPaginated']['totalItems']
######### end of store team matches data #########

######### logo fetching #########
    async def fetch_and_store_team_logos(self):
        """Fetch and store logos for all teams in the database"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        base_url = "https://colleges.wearecollegetennis.com/"
        session = self.Session()
        
        try:
            # Get all teams that don't have logos yet
            teams = session.query(Team).all()
            total_teams = len(teams)
            print(f"Found {total_teams} teams to process")
            
            # Track success and failure counts
            success_count = 0
            failure_count = 0
            
            async with httpx.AsyncClient(follow_redirects=True) as client:
                for i, team in enumerate(teams, 1):
                    try:
                        # Construct logo URL
                        logo_url = f"{base_url}{team.id}/Logo"
                        
                        # Fetch logo
                        response = await client.get(logo_url, timeout=30.0)
                        
                        if response.status_code == 200 and 'content-length' in response.headers:
                            content_length = int(response.headers['content-length'])
                            if content_length > 0:
                                # Save or update logo in database
                                existing_logo = session.query(TeamLogo).filter_by(team_id=team.id).first()
                                
                                if existing_logo:
                                    existing_logo.logo_data = response.content
                                    existing_logo.updated_at = datetime.utcnow()
                                else:
                                    team_logo = TeamLogo(
                                        team_id=team.id,
                                        logo_data=response.content
                                    )
                                    session.add(team_logo)
                                
                                session.commit()
                                success_count += 1
                                print(f"[{i}/{total_teams}] Successfully stored logo for {team.name} (ID: {team.id})")
                            else:
                                failure_count += 1
                                print(f"[{i}/{total_teams}] Empty response for {team.name} (ID: {team.id})")
                        else:
                            failure_count += 1
                            print(f"[{i}/{total_teams}] Failed to fetch logo for {team.name} (ID: {team.id}): Status {response.status_code}")
                        
                        # Add a small delay between requests
                        await asyncio.sleep(0.5)  # Reduced delay to 0.5 seconds since server seems responsive
                        
                    except Exception as e:
                        failure_count += 1
                        print(f"[{i}/{total_teams}] Error processing logo for {team.name} (ID: {team.id}): {e}")
                        session.rollback()
                        await asyncio.sleep(1)  # Longer delay after errors
                        continue
            
            print(f"\nLogo fetching completed!")
            print(f"Successful: {success_count}")
            print(f"Failed: {failure_count}")
            print(f"Total processed: {success_count + failure_count}")
            
        except Exception as e:
            print(f"Unexpected error during logo fetching: {e}")
            session.rollback()
        finally:
            session.close()

    def get_teams_with_logos_count(self):
        """Get count of teams with and without logos"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            total_teams = session.query(Team).count()
            teams_with_logos = session.query(TeamLogo).count()
            
            return {
                'total_teams': total_teams,
                'with_logos': teams_with_logos,
                'without_logos': total_teams - teams_with_logos
            }
        finally:
            session.close()

    async def test_single_logo_fetch(self):
        """Test fetching a single team logo"""
        test_id = "4A8E8A56-D2F0-4F62-8CB2-7AEF3A0F71D6"
        base_url = "https://colleges.wearecollegetennis.com/"
        
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                logo_url = f"{base_url}{test_id}/Logo"
                
                print(f"Attempting to fetch logo from: {logo_url}")
                response = await client.get(logo_url, timeout=30.0)
                
                print(f"Status code: {response.status_code}")
                print(f"Content type: {response.headers.get('content-type', 'No content-type header')}")
                print(f"Response headers: {dict(response.headers)}")
                print(f"Response length: {len(response.content)} bytes")
                
                # Save the response content to a file for inspection
                with open('test_logo.png', 'wb') as f:
                    f.write(response.content)
                    print("Saved response content to test_logo.png")
                
                return response

        except Exception as e:
            print(f"Error during test fetch: {e}")
            return None
        
    def test_retrieve_logo(self, team_id: str, output_path: str = 'retrieved_logo.png'):
        """
        Retrieve a team's logo from the database and save it to a file
        
        Args:
            team_id (str): The team's ID
            output_path (str): Where to save the logo file
        """
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            # Get the team info and logo
            team = session.query(Team).filter_by(id=team_id).first()
            if not team:
                print(f"No team found with ID {team_id}")
                return False
                
            logo = session.query(TeamLogo).filter_by(team_id=team_id).first()
            if not logo:
                print(f"No logo found for team {team.name} (ID: {team_id})")
                return False
                
            # Save the logo to a file
            with open(output_path, 'wb') as f:
                f.write(logo.logo_data)
            
            print(f"Successfully retrieved and saved logo for {team.name}")
            print(f"Saved to: {output_path}")
            print(f"Logo size: {len(logo.logo_data)} bytes")
            print(f"Last updated: {logo.updated_at}")
            
            return True
                
        except Exception as e:
            print(f"Error retrieving logo: {e}")
            return False
        finally:
            session.close()

    def get_random_team_with_logo(self):
        """Get a random team that has a logo"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            # Get any team that has a logo
            team = session.query(Team)\
                .join(TeamLogo)\
                .order_by(func.random())\
                .first()
                
            if team:
                return team.id, team.name
            return None, None
        finally:
            session.close()
####### end of logo fetching #######

##### Fetching school data #####
    def fetch_school_data(self, school_id: str) -> dict:
        """Fetch school data using the school GraphQL query"""
        url = "https://prd-itat-kube.clubspark.pro/mesh-api/graphql"
        query = """
        query school { 
            school(id: "%s") { 
                id 
                name 
                conference 
                itaRegion 
                rankingAwardRegion 
                ustaSection 
                manId 
                womanId 
                division 
                mailingAddress 
                city 
                state 
                zipCode 
                teamType 
            }
        }
        """ % school_id

        try:
            response = requests.post(
                url,
                json={'query': query},
                headers={'Content-Type': 'application/json'},
                verify=False
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching school data: Status {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"Error fetching school data: {e}")
            return {}
        
    def update_school_details(self):
        """Fetch and update details for all schools in school_info table"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            schools = session.query(SchoolInfo).all()
            total_schools = len(schools)
            print(f"Found {total_schools} schools to update")

            success_count = 0
            failure_count = 0

            for i, school in enumerate(schools, 1):
                try:
                    print(f"\nProcessing {i}/{total_schools}: School ID {school.id}")
                    
                    school_data = self.fetch_school_data(school.id)
                    
                    if school_data and 'data' in school_data and 'school' in school_data['data']:
                        school_info = school_data['data']['school']
                        
                        school.name = school_info['name']
                        school.conference = school_info['conference']
                        school.ita_region = school_info['itaRegion']
                        school.ranking_award_region = school_info['rankingAwardRegion']
                        school.usta_section = school_info['ustaSection']
                        school.man_id = school_info['manId']
                        school.woman_id = school_info['womanId']
                        school.division = school_info['division']
                        school.mailing_address = school_info['mailingAddress']
                        school.city = school_info['city']
                        school.state = school_info['state']
                        school.zip_code = school_info['zipCode']
                        school.team_type = school_info['teamType']
                        school.updated_at = datetime.utcnow()
                        
                        session.commit()
                        success_count += 1
                        print(f"Successfully updated: {school_info['name']}")
                    else:
                        print(f"Failed to fetch data for school ID: {school.id}")
                        failure_count += 1

                except Exception as e:
                    print(f"Error updating school {school.id}: {e}")
                    failure_count += 1
                    session.rollback()
                    continue

        except Exception as e:
            print(f"Unexpected error: {e}")
            session.rollback()
        finally:
            session.close()
            print(f"\nUpdate completed!")
            print(f"Successfully updated: {success_count}")
            print(f"Failed: {failure_count}")
            print(f"Total processed: {total_schools}")

    def fetch_seasons_data(self) -> dict:
        """Fetch seasons data using GraphQL query"""
        url = "https://prd-itat-kube.clubspark.pro/mesh-api/graphql"
        query = """
        query listSeasons {
            listSeasons(includeDeletedAndPending: false) {
                id
                name
                status
                startDate
                endDate
            }
        }
        """

        try:
            response = requests.post(
                url,
                json={
                    'query': query,
                    'operationName': 'listSeasons',
                    'variables': {}
                },
                headers={'Content-Type': 'application/json'},
                verify=False
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching seasons data: Status {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"Error fetching seasons data: {e}")
            return {}

    def store_seasons(self):
        """Fetch and store season information"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            seasons_data = self.fetch_seasons_data()
            
            if seasons_data and 'data' in seasons_data and 'listSeasons' in seasons_data['data']:
                seasons = seasons_data['data']['listSeasons']
                total_seasons = len(seasons)
                print(f"Found {total_seasons} seasons to process")

                success_count = 0
                failure_count = 0

                for i, season_info in enumerate(seasons, 1):
                    try:
                        print(f"\nProcessing {i}/{total_seasons}: Season {season_info['name']}")
                        
                        # Create or update season record
                        season = session.query(Season).get(season_info['id'])
                        if not season:
                            season = Season(id=season_info['id'])
                        
                        season.name = season_info['name']
                        season.status = season_info['status']
                        if 'startDate' in season_info and season_info['startDate']:
                            season.start_date = datetime.fromisoformat(season_info['startDate'].replace('Z', '+00:00'))
                        if 'endDate' in season_info and season_info['endDate']:
                            season.end_date = datetime.fromisoformat(season_info['endDate'].replace('Z', '+00:00'))
                        
                        session.merge(season)
                        session.commit()
                        success_count += 1
                        print(f"Successfully stored season: {season_info['name']}")

                    except Exception as e:
                        print(f"Error processing season {season_info.get('name', 'Unknown')}: {e}")
                        failure_count += 1
                        session.rollback()
                        continue

                print(f"\nUpdate completed!")
                print(f"Successfully updated: {success_count}")
                print(f"Failed: {failure_count}")
                print(f"Total processed: {total_seasons}")

            else:
                print("Failed to fetch seasons data")

        except Exception as e:
            print(f"Unexpected error: {e}")
            session.rollback()
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
                headers={'Content-Type': 'application/json'},
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
        
    def fetch_player_matches(self, person_id: str) -> dict:
        """Fetch match results for a player"""
        url = "https://prd-itat-kube.clubspark.pro/mesh-api/graphql"
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
                "end": {"before": "2025-12-30"},
                "statuses": ["DEFAULTED", "RETIRED", "WALKOVER", "COMPLETED", "ABANDONED"]
            }
        }

        try:
            response = requests.post(
                url,
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
                # print("Raw response data:")
                # print(data)
                
                # Check the structure
                if 'data' in data:
                    # print("\nFound 'data' key")
                    if 'td_matchUps' in data['data']:
                        # print("Found 'td_matchUps' key")
                        # print(f"Total items: {data['data']['td_matchUps']['totalItems']}")
                        print(f"Number of items: {len(data['data']['td_matchUps']['items'])}")
                    elif 'matchUps' in data['data']:
                        # print("Found 'matchUps' key")
                        # print(f"Total items: {data['data']['matchUps']['totalItems']}")
                        print(f"Number of items: {len(data['data']['matchUps']['items'])}")
                return data
            else:
                print(f"Error fetching matches: Status {response.status_code}")
                print(f"Response: {response.text}")
                return {}
                
        except Exception as e:
            print(f"Error fetching matches: {e}")
            return {}
        
    def create_match_identifier(self, match_data):
        """Create a unique identifier for a match based on players, date, and tournament"""
        # Sort player IDs to ensure consistent ordering
        player_ids = []
        for side in match_data['sides']:
            for player in side['players']:
                player_ids.append(player['person']['externalID'])
        player_ids.sort()
        
        # Get the date (without time) from the start time
        date = match_data['start'].split('T')[0]
        
        # Include tournament ID for additional uniqueness
        tournament_id = match_data['tournament']['providerTournamentId']
        
        # Create identifier: date-tournament-player1ID-player2ID-type
        return f"{date}-{tournament_id}-{'-'.join(player_ids)}-{match_data['type']}"

    def store_player_matches(self, matches_data):
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
                    # Create unique identifier for this match
                    match_identifier = self.create_match_identifier(match_item)
                    
                    # Check if match already exists using the identifier
                    existing_match = session.query(PlayerMatch).filter_by(match_identifier=match_identifier).first()
                    
                    if existing_match:
                        # print(f"Skipping duplicate match: {match_identifier}")
                        skipped_count += 1
                        continue
                    
                    # If we get here, this is a new match
                    start_time = datetime.fromisoformat(match_item['start'].replace('Z', '+00:00'))
                    end_time = datetime.fromisoformat(match_item['end'].replace('Z', '+00:00'))
                    
                    match = PlayerMatch(
                        match_identifier=match_identifier,  # Store the identifier
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
                        # Get team ID from extensions
                        team_id = None
                        for ext in side['extensions']:
                            if ext['name'] in ['teamId', 'schoolId']:
                                team_id = ext['value']
                                break
                        
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
                    # print(f"Successfully stored new match: {match_identifier}")
                    
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
    
    def test_store_player_matches(self, person_id: str):
        """Test storing matches for a single player"""
        if not self.Session:
            raise RuntimeError("Database not initialized")

        # First check existing matches
        session = self.Session()
        try:
            # Fetch matches data
            matches_data = self.fetch_player_matches(person_id)
            
            if matches_data and 'data' in matches_data and 'td_matchUps' in matches_data['data']:
                matches = matches_data['data']['td_matchUps']['items']
                if matches:
                    # print(f"Found {len(matches)} matches to process")
                    self.store_player_matches(matches_data)
                    
                else:
                    print("No matches found in the response")
            else:
                print("Invalid response format from fetch_player_matches")
                
        except Exception as e:
            print(f"Error in test: {e}")
            session.rollback()
            raise
        finally:
            session.close()

    def store_all_player_matches(self):
        """Fetch and store matches for all players in the database"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            # Get all unique player IDs from players table
            players = session.query(Player).all()
            total_players = len(players)
            print(f"Found {total_players} players to process")
            
            success_count = 0
            error_count = 0
            
            for idx, player in enumerate(players, 1):
                try:
                    print(f"\nProcessing player {idx}/{total_players}: {player.first_name} {player.last_name} (ID: {player.person_id})")
                    
                    # Check if player already has matches stored
                    existing_matches = session.query(PlayerMatchParticipant).filter_by(person_id=player.person_id).count()
                    print(f"Player has {existing_matches} existing matches in database")
                    
                    # Fetch and store matches for this player
                    self.test_store_player_matches(player.person_id)
                    
                    success_count += 1
                    print(f"Successfully processed player {player.first_name} {player.last_name}")
                    
                except Exception as e:
                    error_count += 1
                    print(f"Error processing player {player.person_id}: {e}")
                    continue
                    
                # Add a small delay between requests to avoid overwhelming the API
                time.sleep(1)
            
            print("\nProcessing completed!")
            print(f"Successfully processed: {success_count} players")
            print(f"Errors: {error_count} players")
            print(f"Total: {total_players} players")
            
        except Exception as e:
            print(f"Error in main process: {e}")
        finally:
            session.close()

    def fetch_dual_match(self, match_id: str) -> dict:
        """Fetch dual match details from the API"""
        url = "https://prd-itat-kube.clubspark.pro/mesh-api/graphql"
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
            response = requests.post(
                url,
                json={
                    'operationName': 'dualMatch',
                    'query': query,
                    'variables': {'id': match_id}
                },
                headers={'Content-Type': 'application/json'},
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"Successfully fetched match data for {match_id}")
                return data
            else:
                print(f"Error fetching dual match: Status {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"Error fetching dual match: {e}")
            return {}

    def store_match_lineup(self, match_id: str, match_data: dict):
        """Store lineup data for a dual match"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            match = match_data['data']['dualMatch']
            print(f"Processing match {match_id} with {len(match['tieMatchUps'])} lineups")
            
            # Check if match exists
            existing_match = session.query(Match).get(match_id)
            if not existing_match:
                print(f"Match {match_id} not found in database")
                return
                
            # Check if any lineups exist for this match
            existing_lineups = session.query(MatchLineup).filter_by(match_id=match_id).all()
            if existing_lineups:
                print(f"Match {match_id} already has lineups stored. Skipping...")
                return
                
            # Store each lineup position
            for tie_match in match['tieMatchUps']:
                try:
                    
                    # Validate required data exists
                    if not tie_match.get('side1') or not tie_match.get('side2'):
                        print(f"Missing side1 or side2 data for lineup {tie_match.get('id')}")
                        continue
                        
                    if not tie_match['side1'].get('score') or not tie_match['side2'].get('score'):
                        print(f"Missing score data for lineup {tie_match.get('id')}")
                        continue
                        
                    if not tie_match['side1'].get('participants') or not tie_match['side2'].get('participants'):
                        print(f"Missing participants data for lineup {tie_match.get('id')}")
                        continue
                        
                    if not tie_match['side1']['participants'] or not tie_match['side2']['participants']:
                        print(f"Empty participants list for lineup {tie_match.get('id')}")
                        continue

                    # Get scores safely
                    side1_score = tie_match['side1']['score'].get('scoreString')
                    side2_score = tie_match['side2']['score'].get('scoreString')
                    
                    if not side1_score or not side2_score:
                        print(f"Missing score strings for lineup {tie_match.get('id')}")
                        continue

                    # Get player IDs safely
                    try:
                        side1_player1_id = tie_match['side1']['participants'][0].get('personId')
                        side2_player1_id = tie_match['side2']['participants'][0].get('personId')
                        
                        if not side1_player1_id or not side2_player1_id:
                            print(f"Missing player IDs for lineup {tie_match.get('id')}")
                            continue
                            
                    except (IndexError, KeyError) as e:
                        print(f"Error accessing player data: {e}")
                        continue

                    # Get team names from abbreviations
                    side1_name = None
                    side2_name = None

                    if tie_match['side1'].get('teamAbbreviation'):
                        # Find the team with this abbreviation
                        for team in match['teams']:
                            if team.get('abbreviation') == tie_match['side1']['teamAbbreviation']:
                                side1_name = team.get('name')
                                break

                    if tie_match['side2'].get('teamAbbreviation'):
                        # Find the team with this abbreviation
                        for team in match['teams']:
                            if team.get('abbreviation') == tie_match['side2']['teamAbbreviation']:
                                side2_name = team.get('name')
                                break

                    # If we couldn't find names by abbreviation, try using sideNumber
                    if side1_name is None or side2_name is None:
                        for team in match['teams']:
                            if team.get('sideNumber') == 1:
                                side1_name = team.get('name')
                            elif team.get('sideNumber') == 2:
                                side2_name = team.get('name')

                    lineup = MatchLineup(
                        id=tie_match['id'],
                        match_id=match_id,
                        match_type=tie_match.get('type'),
                        position=tie_match.get('collectionPosition'),
                        collection_id=tie_match.get('collectionId'),
                        side1_player1_id=side1_player1_id,
                        side1_score=side1_score,
                        side1_won=tie_match['side1'].get('didWin', False),
                        side1_name=side1_name,  # Add the team name
                        side2_player1_id=side2_player1_id,
                        side2_score=side2_score,
                        side2_won=tie_match['side2'].get('didWin', False),
                        side2_name=side2_name  # Add the team name
                    )
                    
                    # Add doubles partners if exists
                    if tie_match.get('type') == 'DOUBLES':
                        if len(tie_match['side1']['participants']) > 1:
                            partner_id = tie_match['side1']['participants'][1].get('personId')
                            if partner_id:
                                lineup.side1_player2_id = partner_id
                        if len(tie_match['side2']['participants']) > 1:
                            partner_id = tie_match['side2']['participants'][1].get('personId')
                            if partner_id:
                                lineup.side2_player2_id = partner_id
                    
                    session.add(lineup)
                    
                    # Store set scores
                    if tie_match['side1']['score'].get('sets') and tie_match['side2']['score'].get('sets'):
                        for idx, set_data in enumerate(tie_match['side1']['score']['sets'], 1):
                            try:
                                if idx > len(tie_match['side2']['score']['sets']):
                                    print(f"Mismatched set count for lineup {tie_match.get('id')}")
                                    break
                                    
                                side1_set_score = set_data.get('setScore')
                                side2_set_score = tie_match['side2']['score']['sets'][idx-1].get('setScore')
                                
                                if side1_set_score is None or side2_set_score is None:
                                    print(f"Skipping set {idx} due to missing scores")
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
                                print(f"Error storing set {idx}: {e}")
                                continue
                                
                except Exception as e:
                    print(f"Error storing individual lineup: {e}")
                    print(f"Lineup data: {tie_match}")  # Add this to see the problematic data
                    continue
                
            session.commit()
            print(f"Successfully stored all lineup data for match {match_id}")
            
        except Exception as e:
            print(f"Error storing match lineup: {e}")
            session.rollback()
        finally:
            session.close()
    def test_store_match_lineup(self, match_id: str):
        """Test fetching and storing a match lineup"""
        print(f"Fetching match {match_id} lineup data...")
        match_data = self.fetch_dual_match(match_id)
        
        if match_data and 'data' in match_data and 'dualMatch' in match_data['data']:
            print("Successfully fetched match data")
            self.store_match_lineup(match_id, match_data)
        else:
            print("Failed to fetch match data")
    
    def store_all_match_lineups(self):
        """Fetch and store lineups for all matches in the database"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            # Get all matches from the database
            matches = session.query(Match).all()
            total_matches = len(matches)
            
            # Initialize counters
            stats = {
                'total': total_matches,
                'processed': 0,
                'successful': 0,
                'failed': 0,
                'skipped': 0,
                'missing_data': 0
            }
            
            print(f"Found {total_matches} matches to process")
            print("Starting processing...")
            
            for idx, match in enumerate(matches, 1):
                try:
                    print(f"\nProcessing match {idx}/{total_matches}: {match.id}")
                    
                    # Check if match already has lineups
                    existing_lineups = session.query(MatchLineup).filter_by(match_id=match.id).first()
                    if existing_lineups:
                        print(f"Match {match.id} already has lineups. Skipping...")
                        stats['skipped'] += 1
                        continue
                    
                    # Fetch and store lineup data
                    match_data = self.fetch_dual_match(match.id)
                    if match_data and 'data' in match_data and 'dualMatch' in match_data['data']:
                        try:
                            self.store_match_lineup(match.id, match_data)
                            stats['successful'] += 1
                            print(f"Successfully processed match {match.id}")
                        except Exception as e:
                            stats['failed'] += 1
                            print(f"Failed to store match {match.id}: {e}")
                    else:
                        print(f"No data found for match {match.id}")
                        stats['missing_data'] += 1
                    
                    stats['processed'] += 1
                    
                    # Print progress every 100 matches
                    if idx % 100 == 0:
                        print("\nProgress Update:")
                        print(f"Processed: {stats['processed']} of {stats['total']} ({(stats['processed']/stats['total']*100):.1f}%)")
                        print(f"Successful: {stats['successful']}")
                        print(f"Failed: {stats['failed']}")
                        print(f"Skipped: {stats['skipped']}")
                        print(f"Missing Data: {stats['missing_data']}")
                        print("Continuing processing...\n")
                    
                    # Add a small delay between requests to avoid overwhelming the API
                    time.sleep(1)
                    
                except Exception as e:
                    stats['failed'] += 1
                    print(f"Error processing match {match.id}: {e}")
                    continue
            
            # Print final statistics
            print("\n=== Final Processing Statistics ===")
            print(f"Total Matches: {stats['total']}")
            print(f"Total Processed: {stats['processed']}")
            print(f"Successfully Processed: {stats['successful']} ({(stats['successful']/stats['total']*100):.1f}%)")
            print(f"Failed: {stats['failed']} ({(stats['failed']/stats['total']*100):.1f}%)")
            print(f"Skipped (Already Existed): {stats['skipped']} ({(stats['skipped']/stats['total']*100):.1f}%)")
            print(f"Missing Data: {stats['missing_data']} ({(stats['missing_data']/stats['total']*100):.1f}%)")
            print("=================================")
            
        except Exception as e:
            print(f"Error in main process: {e}")
        finally:
            session.close()

    
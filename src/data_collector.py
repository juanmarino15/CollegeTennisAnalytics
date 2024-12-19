# src/data_collector.py
import asyncio
import httpx
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import Base, Match, Team, MatchTeam, WebLink, TeamLogo 
from sqlalchemy import func  # Add this import


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
            # First, process and store teams
            home_team_id = None
            away_team_id = None
            
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

            # Find and process away team
            away_team_data = next(
                (team for team in match_data['teams'] 
                if not match_data.get('homeTeam') or team['id'] != match_data['homeTeam']['id']),
                None
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
                side_numbers=len(match_data['teams']),
                completed=any(team.get('score') is not None for team in match_data['teams']),
                scheduled_time=start_date if not match_data['startDateTime']['noScheduledTime'] else None
            )
            session.merge(match)

            # Process and store match_teams relationships
            for team_data in match_data['teams']:
                is_home = match_data.get('homeTeam') and team_data['id'] == match_data['homeTeam']['id']
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
                session.add(team_match)

            # Process and store web links
            for link_data in match_data.get('webLinks', []):
                web_link = WebLink(
                    match_id=match.id,
                    name=link_data['name'],
                    url=link_data['url'],
                    typename=link_data.get('__typename')
                )
                session.add(web_link)

            session.commit()
            print(f"Successfully stored match {match.id} with teams and relationships")

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
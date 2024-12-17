# src/data_collector.py
import asyncio
import httpx
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.models import Base, Match, Team, MatchTeam, WebLink  # Changed this line

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


    def store_matches(self, matches_data):
        session = self.Session()
        try:
            for match_data in matches_data:
                try:
                    # Store home team
                    if match_data.get('homeTeam'):
                        home_team = Team(
                            id=match_data['homeTeam']['id'],
                            name=match_data['homeTeam']['name'],
                            abbreviation=match_data['homeTeam'].get('abbreviation'),
                            division=match_data['homeTeam'].get('division'),
                            conference=match_data['homeTeam'].get('conference'),
                            region=match_data['homeTeam'].get('region'),
                            typename=match_data['homeTeam'].get('__typename'),
                            gender=match_data['gender'],
                            has_abbreviation=match_data['homeTeam'].get('abbreviation') is not None,
                            conference_full=match_data['homeTeam'].get('conference'),
                            conference_normalized=match_data['homeTeam'].get('conference', '').replace('_', ' ')
                        )
                        session.merge(home_team)
                        home_team_id = home_team.id
                    
                    # Find and store away team
                    away_team_data = next((team for team in match_data['teams'] 
                                        if team['id'] != match_data['homeTeam']['id']), None)
                    if away_team_data:
                        away_team = Team(
                            id=away_team_data['id'],
                            name=away_team_data['name'],
                            abbreviation=away_team_data.get('abbreviation'),
                            division=away_team_data.get('division'),
                            conference=away_team_data.get('conference'),
                            region=away_team_data.get('region'),
                            typename=away_team_data.get('__typename'),
                            gender=match_data['gender'],
                            has_abbreviation=away_team_data.get('abbreviation') is not None,
                            conference_full=away_team_data.get('conference'),
                            conference_normalized=away_team_data.get('conference', '').replace('_', ' ')
                        )
                        session.merge(away_team)
                        away_team_id = away_team.id

                    # Store match
                    start_date = datetime.fromisoformat(match_data['startDateTime']['dateTimeString'].replace('Z', '+00:00'))
                    season = str(start_date.year if start_date.month < 7 else start_date.year + 1)
                    
                    match = Match(
                        id=match_data['id'],
                        start_date=start_date,
                        timezone=match_data['startDateTime']['timezoneName'],
                        no_scheduled_time=match_data['startDateTime']['noScheduledTime'],
                        is_conference_match=match_data['isConferenceMatch'],
                        gender=match_data['gender'],
                        typename=match_data.get('__typename'),
                        home_team_id=home_team_id if match_data.get('homeTeam') else None,
                        away_team_id=away_team_id if away_team_data else None,
                        season=season,
                        side_numbers=len(match_data['teams']),
                        completed=any(team.get('score') is not None for team in match_data['teams']),
                        scheduled_time=start_date if not match_data['startDateTime']['noScheduledTime'] else None
                    )
                    session.merge(match)

                    # Store match_teams
                    for team_data in match_data['teams']:
                        team_match = MatchTeam(
                            match_id=match.id,
                            team_id=team_data['id'],
                            score=team_data.get('score'),
                            did_win=team_data.get('didWin'),
                            side_number=team_data.get('sideNumber'),
                            is_home_team=(match_data.get('homeTeam') and 
                                        team_data['id'] == match_data['homeTeam']['id']),
                            order_of_play=team_data.get('sideNumber'),
                            team_position='home' if (match_data.get('homeTeam') and 
                                                team_data['id'] == match_data['homeTeam']['id']) 
                                        else 'away'
                        )
                        session.add(team_match)

                    # Store web links
                    for link_data in match_data.get('webLinks', []):
                        web_link = WebLink(
                            match_id=match.id,
                            name=link_data['name'],
                            url=link_data['url'],
                            typename=link_data.get('__typename')
                        )
                        session.add(web_link)

                    session.commit()

                except Exception as match_error:
                    print(f"Error processing match {match_data['id']}: {match_error}")
                    session.rollback()
                    continue

            print(f"Stored {len(matches_data)} matches successfully")

        except Exception as e:
            print(f"Unexpected error: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
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
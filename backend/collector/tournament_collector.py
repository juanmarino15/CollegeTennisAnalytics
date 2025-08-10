# collector/tournament_collector.py
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from sqlalchemy.orm import sessionmaker
from models.models import Tournament, TournamentEvent
from models.models import Base
from sqlalchemy import create_engine

class TournamentCollector:
    def __init__(self, database_url: str):
        """Initialize the tournament collector with database connection"""
        self.database_url = database_url
        self.engine = create_engine(database_url)
        Base.metadata.bind = self.engine
        self.Session = sessionmaker(bind=self.engine)
        
        # API configuration
        self.api_url = "https://prd-itat-kube.clubspark.pro/unified-search-api/api/Search/tournaments/Query"
        self.index_schema = "tournament"
        
        # Headers similar to your match updates service
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
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('tournament_updates.log'),
                logging.StreamHandler()
            ]
        )

    def create_search_payload(self, 
                            from_date: Optional[str] = None,
                            to_date: Optional[str] = None,
                            size: int = 100,
                            from_offset: int = 0,
                            sort_key: str = "date",
                            latitude: float = 0,
                            longitude: float = 0) -> Dict[str, Any]:
        """Create the search payload for tournament API"""
        
        if not from_date:
            # Default to today
            from_date = datetime.now().strftime('%Y-%m-%dT00:00:00.000Z')
        
        payload = {
            "filters": [
                {
                    "key": "date-range",
                    "operator": "Or",
                    "items": [
                        {
                            "minDate": from_date
                        }
                    ]
                }
            ],
            "options": {
                "size": size,
                "from": from_offset,
                "sortKey": sort_key,
                "latitude": latitude,
                "longitude": longitude
            }
        }
        
        # Add max date if provided
        if to_date:
            payload["filters"][0]["items"][0]["maxDate"] = to_date
        
        return payload

    def fetch_tournaments_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch tournament data from the unified search API"""
        try:
            url = f"{self.api_url}?indexSchema={self.index_schema}"
            
            logging.info(f"Fetching tournaments from: {url}")
            
            response = requests.post(
                url,
                json=payload,
                headers=self.headers,
                timeout=30,
                verify=False  # Match your pattern
            )
            
            if response.status_code == 200:
                data = response.json()
                total_results = data.get('total', 0)
                search_results = data.get('searchResults', [])
                logging.info(f"Successfully fetched {len(search_results)} tournaments out of {total_results} total")
                return data
            else:
                logging.error(f"API request failed with status {response.status_code}: {response.text}")
                return {}
                
        except Exception as e:
            logging.error(f"Error fetching tournament data: {str(e)}")
            return {}

    def classify_tournament_type(self, tournament_data: Dict[str, Any]) -> tuple[bool, str]:
        """Since we're hitting a tournaments API, everything is a tournament"""
        return False, 'TOURNAMENT'

    def store_tournament_events(self, session, tournament_id: str, events_data: list):
        """Store tournament events in separate table"""
        
        # Delete existing events for this tournament
        session.query(TournamentEvent).filter_by(tournament_id=tournament_id).delete()
        
        for event_data in events_data:
            division = event_data.get('division', {})
            
            tournament_event = TournamentEvent()
            tournament_event.event_id = event_data.get('id')
            tournament_event.tournament_id = tournament_id
            tournament_event.gender = division.get('gender')
            tournament_event.event_type = division.get('eventType')
            
            session.add(tournament_event)

    def store_tournament_data(self, tournaments_data: Dict[str, Any]) -> None:
        """Store tournament data in the database"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
        
        session = self.Session()
        try:
            search_results = tournaments_data.get('searchResults', [])
            if not search_results:
                logging.warning("No tournament results found in data")
                return
            
            stored_count = 0
            updated_count = 0
            skipped_count = 0
            
            for result in search_results:
                try:
                    tournament_item = result.get('item', {})
                    tournament_id = tournament_item.get('id')
                    
                    if not tournament_id:
                        logging.warning("Tournament missing ID, skipping")
                        continue
                    
                    # Check if tournament already exists
                    existing_tournament = session.query(Tournament).filter_by(tournament_id=tournament_id).first()
                    
                    # Classify tournament type
                    is_dual_match, tournament_type = self.classify_tournament_type(tournament_item)
                    
                    # Parse dates
                    start_datetime = None
                    end_datetime = None
                    if tournament_item.get('startDateTime'):
                        start_datetime = datetime.fromisoformat(tournament_item['startDateTime'].replace('Z', '+00:00'))
                    if tournament_item.get('endDateTime'):
                        end_datetime = datetime.fromisoformat(tournament_item['endDateTime'].replace('Z', '+00:00'))
                    
                    # Parse registration dates
                    reg_restrictions = tournament_item.get('registrationRestrictions', {})
                    entries_open_dt = None
                    entries_close_dt = None
                    if reg_restrictions.get('entriesOpenDateTime'):
                        entries_open_dt = datetime.fromisoformat(reg_restrictions['entriesOpenDateTime'].replace('Z', '+00:00'))
                    if reg_restrictions.get('entriesCloseDateTime'):
                        entries_close_dt = datetime.fromisoformat(reg_restrictions['entriesCloseDateTime'].replace('Z', '+00:00'))
                    
                    # Extract location info
                    location = tournament_item.get('location', {})
                    primary_location = tournament_item.get('primaryLocation', {})
                    geo = location.get('geo', {})
                    
                    # Extract level info
                    level = tournament_item.get('level', {})
                    
                    # Extract organization info
                    organization = tournament_item.get('organization', {})

                    # Extract additional tournament info
                    gender = None
                    event_types = []

                    # Extract from events
                    for event in tournament_item.get('events', []):
                        division = event.get('division', {})
                        if division.get('gender') and not gender:
                            gender = division['gender']
                        if division.get('eventType'):
                            event_types.append(division['eventType'])

                    # Extract level category
                    level_category = None
                    level_categories = tournament_item.get('levelCategories', [])
                    if level_categories:
                        level_category = level_categories[0].get('name')

                    # Calculate registration status
                    registration_status = 'CLOSED'  # Default
                    seconds_until_close = reg_restrictions.get('secondsUntilEntriesClose')
                    seconds_until_open = reg_restrictions.get('secondsUntilEntriesOpen')

                    if seconds_until_open is not None and seconds_until_open > 0:
                        registration_status = 'UPCOMING'
                    elif seconds_until_close is not None and seconds_until_close > 0:
                        registration_status = 'OPEN'
                    else:
                        registration_status = 'CLOSED'
                    
                    if existing_tournament:
                        # Update existing tournament (similar to your team update pattern)
                        logging.info(f"Updating existing tournament: {tournament_id}")
                        
                        existing_tournament.name = tournament_item.get('name')
                        existing_tournament.image = tournament_item.get('image')
                        existing_tournament.is_cancelled = tournament_item.get('isCancelled', False)
                        existing_tournament.start_date_time = start_datetime
                        existing_tournament.end_date_time = end_datetime
                        existing_tournament.is_dual_match = is_dual_match
                        existing_tournament.tournament_type = tournament_type
                        existing_tournament.updated_at = datetime.utcnow()
                        
                        # Update other fields only if they have values (following your pattern)
                        if tournament_item.get('timeZone'):
                            existing_tournament.time_zone = tournament_item['timeZone']
                        if tournament_item.get('url'):
                            existing_tournament.url = tournament_item['url']
                        if location.get('name'):
                            existing_tournament.location_name = location['name']
                        if primary_location.get('town'):
                            existing_tournament.primary_location_town = primary_location['town']
                        if primary_location.get('county'):
                            existing_tournament.primary_location_county = primary_location['county']
                        if organization.get('name'):
                            existing_tournament.organization_name = organization['name']
                        if organization.get('conference'):
                            existing_tournament.organization_conference = organization['conference']
                        if organization.get('division'):
                            existing_tournament.organization_division = organization['division']
                        
                        existing_tournament.gender = gender
                        existing_tournament.event_types = ','.join(set(event_types)) if event_types else None
                        existing_tournament.level_category = level_category
                        existing_tournament.registration_status = registration_status

                        session.merge(existing_tournament)
                        updated_count += 1
                        tournament = existing_tournament
                    else:
                        # Create new tournament
                        logging.info(f"Creating new tournament: {tournament_id}")
                        
                        tournament = Tournament(
                            tournament_id=tournament_id,
                            identification_code=tournament_item.get('identificationCode'),
                            name=tournament_item.get('name'),
                            image=tournament_item.get('image'),
                            is_cancelled=tournament_item.get('isCancelled', False),
                            start_date_time=start_datetime,
                            end_date_time=end_datetime,
                            time_zone=tournament_item.get('timeZone'),
                            time_zone_start_date_time=datetime.fromisoformat(tournament_item['timeZoneStartDateTime'].replace('Z', '+00:00')) if tournament_item.get('timeZoneStartDateTime') else None,
                            time_zone_end_date_time=datetime.fromisoformat(tournament_item['timeZoneEndDateTime'].replace('Z', '+00:00')) if tournament_item.get('timeZoneEndDateTime') else None,
                            url=tournament_item.get('url'),
                            root_provider_id=tournament_item.get('rootProviderId'), 
                            
                            # Location
                            location_id=location.get('id'),
                            location_name=location.get('name'),
                            primary_location_town=primary_location.get('town'),
                            primary_location_county=primary_location.get('county'),
                            primary_location_address1=primary_location.get('address1'),
                            primary_location_address2=primary_location.get('address2'),
                            primary_location_address3=primary_location.get('address3'),
                            primary_location_postcode=primary_location.get('postcode'),
                            geo_latitude=geo.get('latitude', 0),
                            geo_longitude=geo.get('longitude', 0),
                            
                            # Level
                            level_id=level.get('id'),
                            level_name=level.get('name'),
                            level_branding=level.get('branding'),
                            
                            # Organization
                            organization_id=organization.get('id'),
                            organization_name=organization.get('name'),
                            organization_conference=organization.get('conference'),
                            organization_division=organization.get('division'),
                            organization_url_segment=organization.get('urlSegment'),
                            organization_parent_region_id=organization.get('parentRegionId'),
                            organization_region_id=organization.get('regionId'),
                            
                            # Registration
                            entries_open_date_time=entries_open_dt,
                            entries_close_date_time=entries_close_dt,
                            seconds_until_entries_close=reg_restrictions.get('secondsUntilEntriesClose'),
                            seconds_until_entries_open=reg_restrictions.get('secondsUntilEntriesOpen'),
                            registration_time_zone=reg_restrictions.get('timeZone'),
                            
                            # Classification
                            is_dual_match=is_dual_match,
                            tournament_type=tournament_type,

                            gender=gender,
                            event_types=','.join(set(event_types)) if event_types else None,
                            level_category=level_category,
                            registration_status=registration_status
                        )
                        
                        session.add(tournament)
                        session.flush()  # Get the ID
                        stored_count += 1
                    
                    # Store tournament events
                    if tournament_item.get('events'):
                        self.store_tournament_events(session, tournament_id, tournament_item['events'])
                    
                except Exception as e:
                    logging.error(f"Error processing tournament {tournament_id}: {str(e)}")
                    session.rollback()  # Rollback this tournament only
                    continue
            
            session.commit()
            logging.info(f"Tournament data processing complete: {stored_count} new, {updated_count} updated, {skipped_count} skipped")
            
        except Exception as e:
            session.rollback()
            logging.error(f"Error storing tournament data: {str(e)}")
            raise
        finally:
            session.close()

    def collect_tournaments_range(self, 
                                start_date: Optional[str] = None,
                                end_date: Optional[str] = None,
                                batch_size: int = 100):
        """Collect tournaments for a specific date range with pagination"""
        
        if not start_date:
            # Default to collecting from today onwards
            start_date = datetime.now().strftime('%Y-%m-%dT00:00:00.000Z')
        
        offset = 0
        total_processed = 0
        
        while True:
            payload = self.create_search_payload(
                from_date=start_date,
                to_date=end_date,
                size=batch_size,
                from_offset=offset
            )
            
            logging.info(f"Fetching batch starting at offset {offset}")
            
            data = self.fetch_tournaments_data(payload)
            if not data or not data.get('searchResults'):
                logging.info("No more data to fetch")
                break
            
            # Store the data
            self.store_tournament_data(data)
            
            # Check if we've reached the end
            search_results = data.get('searchResults', [])
            total_available = data.get('total', 0)
            
            total_processed += len(search_results)
            offset += batch_size
            
            logging.info(f"Processed {total_processed} out of {total_available} tournaments")
            
            # If we've processed all available data, stop
            if total_processed >= total_available or len(search_results) < batch_size:
                break
        
        logging.info(f"Tournament collection complete. Total processed: {total_processed}")

    def collect_weekly_tournaments(self):
        """Collect tournaments for the next week (weekly scheduled run)"""
        today = datetime.now()
        next_week = today + timedelta(days=7)
        
        start_date = today.strftime('%Y-%m-%dT00:00:00.000Z')
        end_date = next_week.strftime('%Y-%m-%dT23:59:59.000Z')
        
        logging.info(f"Starting weekly tournament collection from {start_date} to {end_date}")
        self.collect_tournaments_range(start_date, end_date)

    def collect_all_future_tournaments(self):
        """Collect all tournaments from today onwards"""
        today = datetime.now()
        start_date = today.strftime('%Y-%m-%dT00:00:00.000Z')
        
        logging.info(f"Starting collection of all future tournaments from {start_date}")
        self.collect_tournaments_range(start_date=start_date)

# Example usage
if __name__ == "__main__":
    # Initialize collector
    DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
    collector = TournamentCollector(DATABASE_URL)
    
    # Run weekly collection
    collector.collect_weekly_tournaments()
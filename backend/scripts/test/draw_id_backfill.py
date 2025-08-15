#!/usr/bin/env python3
"""
Backfill script to add draw_id to player_matches table and populate it from API data
Run this from the backend directory: python migrations/backfill_player_matches_draw_id.py
"""

import os
import sys
import logging
import requests
import json
from pathlib import Path
from sqlalchemy import create_engine, text, Column, String
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Add the backend directory to Python path
backend_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_root))

# Import your models
from models.models import PlayerMatch, PlayerMatchParticipant

def setup_logging():
    """Set up logging for the backfill script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('player_matches_draw_id_backfill.log'),
            logging.StreamHandler()
        ]
    )

class PlayerMatchesDrawIdBackfill:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # API configuration (same as your collector)
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

    def add_draw_id_column(self):
        """Add draw_id column to player_matches table if it doesn't exist"""
        try:
            logging.info("Adding draw_id column to player_matches table...")
            
            with self.engine.connect() as conn:
                # Check if column already exists
                check_sql = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'player_matches' 
                    AND column_name = 'draw_id'
                """)
                
                result = conn.execute(check_sql).fetchone()
                
                if result:
                    logging.info("draw_id column already exists, skipping creation")
                    return
                
                # Add the column
                add_column_sql = text("""
                    ALTER TABLE player_matches 
                    ADD COLUMN draw_id VARCHAR
                """)
                
                conn.execute(add_column_sql)
                conn.commit()
                
                logging.info("Successfully added draw_id column to player_matches table")
                
        except Exception as e:
            logging.error(f"Error adding draw_id column: {str(e)}")
            raise

    def get_total_matches_needing_draw_id(self) -> int:
        """Get the total count of matches that need draw_id"""
        session = self.Session()
        try:
            # Check if draw_id column exists
            check_column = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'player_matches' 
                AND column_name = 'draw_id'
            """)
            
            column_exists = session.execute(check_column).fetchone()
            
            if not column_exists:
                # Count all matches if column doesn't exist
                query = text("""
                    SELECT COUNT(*)
                    FROM player_matches pm
                    WHERE pm.start_time >= CURRENT_DATE - INTERVAL '365 days'
                """)
            else:
                # Count matches without draw_id
                query = text("""
                    SELECT COUNT(*)
                    FROM player_matches pm
                    WHERE (pm.draw_id IS NULL OR pm.draw_id = '')
                    AND pm.start_time >= CURRENT_DATE - INTERVAL '365 days'
                """)
            
            count = session.execute(query).scalar()
            return count or 0
            
        except Exception as e:
            logging.error(f"Error getting total count: {str(e)}")
            return 0
        finally:
            session.close()

    def get_player_matches_without_draw_id(self, limit: int = 1000, offset: int = 0) -> List[Dict]:
        """Get player matches that don't have draw_id populated"""
        session = self.Session()
        try:
            # First check if draw_id column exists
            check_column = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'player_matches' 
                AND column_name = 'draw_id'
            """)
            
            column_exists = session.execute(check_column).fetchone()
            
            if not column_exists:
                logging.info("draw_id column doesn't exist yet, getting all matches for backfill")
                # Get all matches if column doesn't exist - use a subquery to get match IDs first
                base_query = text("""
                    SELECT pm.id
                    FROM player_matches pm
                    WHERE pm.start_time >= CURRENT_DATE - INTERVAL '365 days'
                    ORDER BY pm.id
                    LIMIT :limit OFFSET :offset
                """)
            else:
                # Get matches without draw_id
                base_query = text("""
                    SELECT pm.id
                    FROM player_matches pm
                    WHERE (pm.draw_id IS NULL OR pm.draw_id = '')
                    AND pm.start_time >= CURRENT_DATE - INTERVAL '365 days'
                    ORDER BY pm.id
                    LIMIT :limit OFFSET :offset
                """)
            
            # Get the match IDs first
            match_ids_result = session.execute(base_query, {"limit": limit, "offset": offset}).fetchall()
            match_ids = [row[0] for row in match_ids_result]
            
            if not match_ids:
                return []
            
            logging.info(f"Got {len(match_ids)} match IDs to process")
            
            # Now get full match data including participants
            full_query = text("""
                SELECT 
                    pm.id,
                    pm.match_identifier,
                    pm.tournament_id,
                    pm.start_time,
                    pm.match_type,
                    pmp.person_id
                FROM player_matches pm
                JOIN player_match_participants pmp ON pm.id = pmp.match_id
                WHERE pm.id = ANY(:match_ids)
                ORDER BY pm.id
            """)
            
            result = session.execute(full_query, {"match_ids": match_ids}).fetchall()
            
            # Group by match to get all participants per match
            matches_dict = {}
            for row in result:
                match_id = row[0]
                if match_id not in matches_dict:
                    matches_dict[match_id] = {
                        'id': row[0],
                        'match_identifier': row[1],
                        'tournament_id': row[2],
                        'start_time': row[3],
                        'match_type': row[4],
                        'participants': []
                    }
                matches_dict[match_id]['participants'].append(row[5])
            
            matches_list = list(matches_dict.values())
            
            logging.info(f"Found {len(matches_list)} matches without draw_id (offset: {offset})")
            return matches_list
            
        except Exception as e:
            logging.error(f"Error getting matches without draw_id: {str(e)}")
            return []
        finally:
            session.close()

    def fetch_player_matches_from_api(self, person_id: str, days_back: int = 730) -> Dict:
        """Fetch player matches from the API to get draw_id information"""
        
        query = """query matchUps($personFilter: [td_PersonFilterOptions], $filter: td_MatchUpFilterOptions) {
            td_matchUps(personFilter: $personFilter, filter: $filter) {
                totalItems
                items {
                    drawId
                    tournament {
                        providerTournamentId
                    }
                    start
                    type
                    sides {
                        players {
                            person {
                                externalID
                            }
                        }
                    }
                    matchUpFormat
                    status
                    collectionPosition
                }
            }
        }"""

        # Calculate date range
        today = datetime.now()
        start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')

        variables = {
            "personFilter": {
                "ids": [{
                    "type": "ExternalID",
                    "identifier": person_id.lower()  # API requires lowercase
                }]
            },
            "filter": {
                "start": {"after": start_date},
                "end": {"before": end_date},
                "statuses": ["DEFAULTED", "RETIRED", "WALKOVER", "COMPLETED", "ABANDONED"]
            }
        }

        try:
            response = requests.post(
                self.api_url,
                json={
                    'operationName': 'matchUps',
                    'query': query,
                    'variables': variables
                },
                headers=self.headers,
                verify=False,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and data['data'] and 'td_matchUps' in data['data'] and data['data']['td_matchUps']:
                    return data
                    
            logging.warning(f"No data returned for player {person_id}")
            return {}
                
        except Exception as e:
            logging.error(f"Error fetching matches for player {person_id}: {str(e)}")
            return {}

    def create_match_identifier_from_api_data(self, match_data: Dict) -> str:
        """Create the same match identifier used by the collector"""
        try:
            # Extract all player IDs from both sides
            player_ids = []
            for side in match_data.get('sides', []):
                if not side:
                    continue
                for player in side.get('players', []):
                    if player and 'person' in player and player['person'] and 'externalID' in player['person']:
                        player_ids.append(player['person']['externalID'])
            
            # Sort player IDs for consistency
            player_ids.sort()
            
            # Extract date information
            date_str = match_data.get('start', '').split('T')[0] if match_data.get('start') else 'unknown_date'
            
            # Extract tournament ID
            tournament_data = match_data.get('tournament', {})
            tournament_id = tournament_data.get('providerTournamentId', 'unknown_tournament') if tournament_data else 'unknown_tournament'
            
            # Extract match type
            match_type = match_data.get('type', 'unknown_type')
            
            # Extract collection position (for dual matches)
            collection_pos = match_data.get('collectionPosition', 'np')
            
            # Create identifier matching collector logic
            identifier = f"{date_str}-{tournament_id}-{match_type}-{collection_pos}-{'-'.join(player_ids)}"
            
            return identifier
        except Exception as e:
            logging.error(f"Error creating match identifier: {e}")
            return None

    def update_match_draw_id(self, match_id: int, draw_id: str):
        """Update a single match with its draw_id"""
        session = self.Session()
        try:
            session.execute(
                text("UPDATE player_matches SET draw_id = :draw_id WHERE id = :match_id"),
                {"draw_id": draw_id, "match_id": match_id}
            )
            session.commit()
            logging.debug(f"Updated match {match_id} with draw_id {draw_id}")
            return True
        except Exception as e:
            logging.error(f"Error updating match {match_id} with draw_id {draw_id}: {str(e)}")
            session.rollback()
            return False
        finally:
            session.close()

    def process_matches_batch(self, matches: List[Dict]) -> Dict[str, int]:
        """Process a batch of matches to get their draw_id"""
        stats = {
            'processed': 0,
            'updated': 0,
            'no_api_data': 0,
            'no_draw_id': 0,
            'api_errors': 0
        }
        
        # Group matches by participant to minimize API calls
        participants_matches = {}
        for match in matches:
            for participant_id in match['participants']:
                if participant_id not in participants_matches:
                    participants_matches[participant_id] = []
                participants_matches[participant_id].append(match)
        
        for participant_id, participant_matches in participants_matches.items():
            try:
                logging.info(f"Fetching API data for participant {participant_id} ({len(participant_matches)} matches)")
                
                # Fetch API data for this participant
                api_data = self.fetch_player_matches_from_api(participant_id)
                
                if not api_data or 'data' not in api_data or not api_data['data']:
                    stats['no_api_data'] += len(participant_matches)
                    continue
                
                api_matches = api_data['data']['td_matchUps']['items']
                if not api_matches:
                    stats['no_api_data'] += len(participant_matches)
                    continue
                
                # Create lookup dictionary for API matches by identifier
                api_matches_lookup = {}
                for api_match in api_matches:
                    identifier = self.create_match_identifier_from_api_data(api_match)
                    if identifier and api_match.get('drawId'):
                        api_matches_lookup[identifier] = api_match['drawId']
                
                # Match our database matches with API matches
                for db_match in participant_matches:
                    stats['processed'] += 1
                    
                    # Try to find matching API match
                    if db_match['match_identifier'] in api_matches_lookup:
                        draw_id = api_matches_lookup[db_match['match_identifier']]
                        
                        if self.update_match_draw_id(db_match['id'], draw_id):
                            stats['updated'] += 1
                            logging.info(f"✅ Updated match {db_match['id']} with draw_id {draw_id}")
                        else:
                            logging.error(f"❌ Failed to update match {db_match['id']}")
                    else:
                        stats['no_draw_id'] += 1
                        logging.warning(f"⚠️  No draw_id found for match {db_match['id']} ({db_match['match_identifier']})")
                
                # Add small delay between API calls
                import time
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Error processing participant {participant_id}: {str(e)}")
                stats['api_errors'] += len(participant_matches)
                continue
        
        return stats

    def run_backfill(self, batch_size: int = 500, max_batches: int = None):
        """Run the complete backfill process"""
        try:
            logging.info("Starting player_matches draw_id backfill...")
            
            # Step 1: Add draw_id column if needed
            self.add_draw_id_column()
            
            # Step 2: Get total count for progress tracking
            total_matches = self.get_total_matches_needing_draw_id()
            logging.info(f"Total matches needing draw_id: {total_matches:,}")
            
            # Step 3: Process matches in batches
            total_stats = {
                'processed': 0,
                'updated': 0,
                'no_api_data': 0,
                'no_draw_id': 0,
                'api_errors': 0
            }
            
            batch_count = 0
            offset = 0
            while True:
                if max_batches and batch_count >= max_batches:
                    logging.info(f"Reached maximum batch limit ({max_batches})")
                    break
                
                # Get next batch of matches
                matches = self.get_player_matches_without_draw_id(batch_size, offset)
                
                if not matches:
                    logging.info("No more matches to process")
                    break
                
                batch_count += 1
                progress = (offset + len(matches)) / total_matches * 100 if total_matches > 0 else 0
                logging.info(f"Processing batch {batch_count} ({len(matches)} matches, offset: {offset:,}, progress: {progress:.1f}%)...")
                
                # Process this batch
                batch_stats = self.process_matches_batch(matches)
                
                # Update totals
                for key in total_stats:
                    total_stats[key] += batch_stats[key]
                
                # Log batch results
                logging.info(f"Batch {batch_count} completed: {batch_stats}")
                
                # Update offset for next batch
                offset += len(matches)
                
                # If we got fewer matches than requested, we're done
                if len(matches) < batch_size:
                    logging.info(f"Got {len(matches)} matches (less than batch size {batch_size}), this was the last batch")
                    break
            
            # Final summary
            logging.info("🎉 Backfill completed!")
            logging.info(f"📊 Final Statistics:")
            logging.info(f"   • Processed: {total_stats['processed']}")
            logging.info(f"   • Updated: {total_stats['updated']}")
            logging.info(f"   • No API data: {total_stats['no_api_data']}")
            logging.info(f"   • No draw_id in API: {total_stats['no_draw_id']}")
            logging.info(f"   • API errors: {total_stats['api_errors']}")
            
            success_rate = (total_stats['updated'] / total_stats['processed']) * 100 if total_stats['processed'] > 0 else 0
            logging.info(f"   • Success rate: {success_rate:.1f}%")
            
        except Exception as e:
            logging.error(f"Backfill failed: {str(e)}")
            raise

def main():
    """Main function to run the backfill"""
    setup_logging()
    
    # Database URL
    database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
    
    if not database_url:
        raise ValueError("Database URL not found")
    
    # Initialize backfill processor
    backfill = PlayerMatchesDrawIdBackfill(database_url)
    
    # For initial testing, you can start small:
    # backfill.run_backfill(batch_size=50, max_batches=2)
    
    # For full backfill (remove max_batches or set to None):
    backfill.run_backfill(
        batch_size=100,  # Reasonable batch size
        max_batches=None  # Remove this limit for full backfill
    )

if __name__ == "__main__":
    main()
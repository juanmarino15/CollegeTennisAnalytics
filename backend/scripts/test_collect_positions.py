#!/usr/bin/env python
# update_match_positions.py
# Script to update collection_position in player_matches table

import os
import sys
from pathlib import Path
import asyncio
import logging
import requests
from datetime import datetime, timedelta
import argparse
from sqlalchemy import create_engine, func, or_, text
from sqlalchemy.orm import sessionmaker
import time

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from models.models import (
    Base, 
    PlayerMatch, 
    PlayerMatchParticipant
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('match_positions_update.log'),
        logging.StreamHandler()
    ]
)

# Get DATABASE_URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    # Fallback for local development
    DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

class MatchPositionUpdater:
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
            logging.error(f"Database initialization error: {e}")
            self.engine = None
            self.Session = None

    def fetch_match_details(self, person_id: str):
        """Fetch match results for a player"""
        query = """query matchUps($personFilter: [td_PersonFilterOptions], $filter: td_MatchUpFilterOptions) {
            td_matchUps(personFilter: $personFilter, filter: $filter) {
                totalItems
                items {
                    score {
                        scoreString
                    }
                    sides {
                        sideNumber
                        players {
                            person {
                                externalID
                            }
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
                "start": {"after": "2024-06-01"},
                "end": {"before": "2025-12-31"},
                "statuses": ["DEFAULTED", "RETIRED", "WALKOVER", "COMPLETED", "ABANDONED"]
            }
        }

        try:
            # Use a shorter version of the log to prevent flooding logs
            logging.debug(f"Fetching matches for player {person_id}...")
            
            # Add retry logic for API resilience
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
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
                        timeout=10  # Add timeout to prevent hanging requests
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if 'data' in data and 'td_matchUps' in data['data']:
                            items = data['data']['td_matchUps']['items']
                            # Make this a debug level log to reduce console output
                            logging.debug(f"Found {len(items)} matches for player {person_id}")
                            return data
                        else:
                            logging.warning(f"Invalid response format for {person_id}")
                            return {}
                    elif response.status_code == 429:  # Too many requests
                        retry_count += 1
                        wait_time = 5 * retry_count  # Progressive backoff
                        logging.warning(f"Rate limited. Waiting {wait_time}s before retry {retry_count}/{max_retries}")
                        time.sleep(wait_time)
                    else:
                        logging.error(f"Error fetching matches (Status {response.status_code}): {response.text[:100]}")
                        return {}
                
                except requests.exceptions.Timeout:
                    retry_count += 1
                    logging.warning(f"Request timeout. Retry {retry_count}/{max_retries}")
                    time.sleep(2)
                except requests.exceptions.RequestException as req_err:
                    logging.error(f"Request failed: {req_err}")
                    return {}
            
            logging.error(f"Exceeded maximum retries for player {person_id}")
            return {}
                
        except Exception as e:
            logging.error(f"Error fetching matches for {person_id}: {e}")
            return {}

    def create_match_identifier(self, match_data):
        """Create a unique identifier for a match"""
        player_ids = []
        for side in match_data['sides']:
            for player in side['players']:
                player_ids.append(player['person']['externalID'])
        player_ids.sort()
        
        date = match_data['start'].split('T')[0]
        tournament_id = match_data['tournament']['providerTournamentId']
        
        return f"{date}-{tournament_id}-{'-'.join(player_ids)}-{match_data['type']}"

    def update_match_positions(self, player_matches_data):
        """Update position data in player_matches table"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            if not player_matches_data.get('data', {}).get('td_matchUps', {}).get('items'):
                return 0
                
            matches = player_matches_data['data']['td_matchUps']['items']
            updated_count = 0
            skipped_count = 0
            
            for match_item in matches:
                try:
                    # Only process if collection_position exists
                    if match_item.get('collectionPosition') is None:
                        skipped_count += 1
                        continue
                    
                    # Create unique identifier
                    match_identifier = self.create_match_identifier(match_item)
                    
                    # Find the match in the database
                    db_match = session.query(PlayerMatch).filter_by(match_identifier=match_identifier).first()
                    if not db_match:
                        logging.warning(f"Match not found: {match_identifier}")
                        skipped_count += 1
                        continue
                    
                    # Update the collection_position
                    if db_match.collection_position != match_item.get('collectionPosition'):
                        db_match.collection_position = match_item.get('collectionPosition')
                        session.add(db_match)
                        updated_count += 1
                        logging.info(f"Updated position for match {db_match.id} to {match_item.get('collectionPosition')}")
                    else:
                        skipped_count += 1
                
                except Exception as e:
                    logging.error(f"Error updating match: {e}")
                    continue
            
            session.commit()
            return updated_count, skipped_count
            
        except Exception as e:
            logging.error(f"Error updating matches: {e}")
            session.rollback()
            return 0, 0
        finally:
            session.close()

    def update_all_match_positions(self):
        """Update positions for all matches in the database"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            # Find players with matches in 2025
            query = """
            SELECT DISTINCT pp.person_id 
            FROM player_match_participants pp
            JOIN player_matches pm ON pp.match_id = pm.id
            WHERE pm.start_time >= '2025-01-01'
            """
            
            result = session.execute(query).fetchall()
            players = [r[0] for r in result if r[0]]
            
            logging.info(f"Found {len(players)} players with matches in 2025")
            
            total_updated = 0
            total_skipped = 0
            
            for idx, player_id in enumerate(players):
                try:
                    logging.info(f"Processing player {idx+1}/{len(players)}: {player_id}")
                    
                    # Fetch match data for player
                    player_matches = self.fetch_match_details(player_id)
                    
                    # Update match positions
                    updated, skipped = self.update_match_positions(player_matches)
                    total_updated += updated
                    total_skipped += skipped
                    
                    if (idx + 1) % 10 == 0:
                        logging.info(f"Progress: {idx+1}/{len(players)} players processed")
                    
                    # Sleep to avoid rate limiting
                    time.sleep(0.5)
                    
                except Exception as e:
                    logging.error(f"Error processing player {player_id}: {e}")
                    continue
            
            logging.info(f"Position update summary:")
            logging.info(f"Total matches updated: {total_updated}")
            logging.info(f"Total matches skipped: {total_skipped}")
            
            return total_updated, total_skipped
            
        except Exception as e:
            logging.error(f"Error updating match positions: {e}")
            return 0, 0
        finally:
            session.close()

    def update_null_positions(self):
        """Only update matches with NULL collection_position"""
        if not self.Session:
            raise RuntimeError("Database not initialized")
            
        session = self.Session()
        try:
            # Use command line argument for batch size or default to 100
            import sys
            batch_size = 100
            if len(sys.argv) > 1 and sys.argv[1].isdigit():
                batch_size = int(sys.argv[1])
        
            # Find matches with NULL collection_position more efficiently
            # Use a direct SQL query for better performance with large datasets
            logging.info("Counting matches with NULL collection_position...")
            count_query = text("""
            SELECT COUNT(*) FROM player_matches 
            WHERE collection_position IS NULL 
            AND start_time >= '2025-01-01'
            """)
            null_count = session.execute(count_query).scalar()
            logging.info(f"Found {null_count} matches with NULL collection_position")
            
            # Get unique player IDs using SQL for better performance
            logging.info(f"Finding players with NULL position matches (this may take a few minutes)...")
            player_query = text(f"""
            SELECT DISTINCT pmp.person_id 
            FROM player_match_participants pmp
            JOIN player_matches pm ON pmp.match_id = pm.id
            WHERE pm.collection_position IS NULL
            AND pm.start_time >= '2025-01-01'
            AND pmp.person_id IS NOT NULL
            LIMIT {batch_size}  -- Process in smaller batches
            """)
            
            player_results = session.execute(player_query).fetchall()
            player_ids = [result[0] for result in player_results if result[0]]
            player_count = len(player_ids)
            
            logging.info(f"Processing the first {player_count} players with NULL position matches")
            
            # Add a counter for players with no matches found
            total_updated = 0
            no_matches_count = 0
            start_time = datetime.now()
            
            for idx, player_id in enumerate(player_ids):
                try:
                    current_time = datetime.now()
                    elapsed = (current_time - start_time).total_seconds() / 60.0
                    
                    if idx > 0:  # After first player, we can calculate rate
                        remaining = player_count - (idx + 1)
                        rate = idx / elapsed if elapsed > 0 else 0
                        estimated_mins = remaining / rate if rate > 0 else 0
                        
                        logging.info(f"Processing player {idx+1}/{player_count}: {player_id} " +
                                    f"({elapsed:.1f} mins elapsed, ~{estimated_mins:.1f} mins remaining)")
                    else:
                        logging.info(f"Processing player {idx+1}/{player_count}: {player_id}")
                    
                    # Fetch match data for player
                    player_matches = self.fetch_match_details(player_id)
                    match_count = len(player_matches.get('data', {}).get('td_matchUps', {}).get('items', []))
                    
                    if match_count == 0:
                        no_matches_count += 1
                        # Skip the wait time for players with no matches
                        continue
                    
                    logging.info(f"Found {match_count} matches for player {player_id}")
                    
                    # Update match positions
                    updated, skipped = self.update_match_positions(player_matches)
                    total_updated += updated
                    
                    logging.info(f"Updated {updated} matches for player {player_id}")
                    
                    # Sleep to avoid rate limiting
                    time.sleep(0.5)
                    
                except Exception as e:
                    logging.error(f"Error processing player {player_id}: {e}")
                    continue
            
            # Provide detailed summary
            end_time = datetime.now()
            total_duration = (end_time - start_time).total_seconds() / 60.0
            
            logging.info(f"Position update summary:")
            logging.info(f"Total players processed: {player_count}")
            logging.info(f"Players with no matches found: {no_matches_count}")
            logging.info(f"Total matches updated: {total_updated}")
            logging.info(f"Total duration: {total_duration:.1f} minutes")
            
            return total_updated
            
        except Exception as e:
            logging.error(f"Error updating match positions: {e}")
            return 0
        finally:
            session.close()

async def main():
    """Main function to run the update"""
    try:
        # Parse command line arguments
        import argparse
        parser = argparse.ArgumentParser(description="Update match positions in player_matches table")
        parser.add_argument('--batch-size', type=int, default=100, help='Number of players per batch')
        parser.add_argument('--max-batches', type=int, default=0, help='Maximum number of batches to process (0 for all)')
        parser.add_argument('--sleep', type=int, default=5, help='Seconds to sleep between batches')
        args = parser.parse_args()
        
        logging.info("Starting match position update")
        start_time = datetime.now()
        
        updater = MatchPositionUpdater(DATABASE_URL)
        
        # Get initial count of NULL positions
        session = updater.Session()
        count_query = text("""
        SELECT COUNT(*) FROM player_matches 
        WHERE collection_position IS NULL 
        AND start_time >= '2024-06-01'
        """)
        initial_count = session.execute(count_query).scalar() or 0
        session.close()
        
        logging.info(f"Found {initial_count} matches with NULL collection_position")
        
        # Process batches until all matches are updated or max batches reached
        batch_num = 0
        total_updated = 0
        remaining = initial_count
        zero_updates_count = 0
        
        while remaining > 0:
            batch_num += 1
            batch_start = datetime.now()
            
            # Check if we've reached the maximum number of batches
            if args.max_batches > 0 and batch_num > args.max_batches:
                logging.info(f"Reached maximum batch limit of {args.max_batches}")
                break
                
            logging.info(f"Processing batch {batch_num} (remaining: {remaining})")
            
            # Process a batch with the specified batch size
            try:
                # Override sys.argv temporarily for the batch size
                original_argv = sys.argv.copy()
                sys.argv = [sys.argv[0], str(args.batch_size)]
                
                updated = updater.update_null_positions()
                
                # Restore original argv
                sys.argv = original_argv
                
                total_updated += updated
                
                # Check if we're making progress
                if updated == 0:
                    zero_updates_count += 1
                    if zero_updates_count >= 3:
                        logging.warning(f"No updates in {zero_updates_count} consecutive batches")
                        if zero_updates_count >= 5:
                            logging.error("Stopping after 5 consecutive zero-update batches")
                            break
                else:
                    zero_updates_count = 0
                
            except Exception as e:
                logging.error(f"Error processing batch {batch_num}: {e}")
                continue
            
            # Check remaining NULL positions
            try:
                session = updater.Session()
                current_count = session.execute(count_query).scalar() or 0
                session.close()
                
                remaining = current_count
                
                batch_duration = (datetime.now() - batch_start).total_seconds() / 60.0
                total_duration = (datetime.now() - start_time).total_seconds() / 60.0
                
                # Calculate estimated remaining time
                if batch_num > 1 and total_updated > 0:
                    rate = total_updated / total_duration  # updates per minute
                    remaining_est = remaining / rate if rate > 0 else float('inf')
                    
                    logging.info(f"Batch {batch_num} completed in {batch_duration:.1f} minutes")
                    logging.info(f"Updated {updated} matches in this batch")
                    logging.info(f"Progress: {total_updated}/{initial_count} ({(total_updated/initial_count*100):.1f}%) updated")
                    logging.info(f"Estimated time remaining: {remaining_est:.1f} minutes")
                else:
                    logging.info(f"Batch {batch_num} completed in {batch_duration:.1f} minutes")
                    logging.info(f"Updated {updated} matches in this batch")
                
            except Exception as e:
                logging.error(f"Error checking remaining matches: {e}")
                # Continue with the loop even if the check fails
            
            # Break if no matches remain to be updated
            if remaining == 0:
                logging.info("All matches have been updated! 🎉")
                break
                
            # Sleep between batches
            if args.sleep > 0 and remaining > 0:
                logging.info(f"Sleeping for {args.sleep} seconds before next batch...")
                await asyncio.sleep(args.sleep)
        
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds() / 60.0
        
        logging.info(f"Process completed after {batch_num} batches")
        logging.info(f"Total matches updated: {total_updated}")
        logging.info(f"Remaining matches with NULL positions: {remaining}")
        logging.info(f"Total duration: {total_duration:.1f} minutes")
        
    except Exception as e:
        logging.error(f"Error in match position update: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
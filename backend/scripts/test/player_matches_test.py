import urllib.request
import urllib.parse
import json
from datetime import datetime, timedelta
from typing import Dict
import ssl
import gzip
import logging
import os

def setup_logging():
    """Setup logging to both file and console"""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/enhanced_match_query_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    
    return log_filename

class EnhancedTennisMatchFetcher:
    def __init__(self):
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
        self.logger = logging.getLogger(__name__)

    def fetch_player_matches_comprehensive(self, person_id: str, days_back: int = 360) -> Dict:
        """Fetch match results with comprehensive field selection"""
        
        # Let's try to include ALL potential fields we might want
        query = """query matchUps($personFilter: [td_PersonFilterOptions], $filter: td_MatchUpFilterOptions) {
            td_matchUps(personFilter: $personFilter, filter: $filter) {
                totalItems
                items {
                    id
                    matchUpId
                    matchId
                    eventId
                    drawId
                    collectionId
                    matchUpStatus
                    matchUpFormat
                    matchUpType
                    venue
                    court
                    surface
                    weather
                    notes
                    discipline
                    category
                    gender
                    winnerMatchUpId
                    loserMatchUpId
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
                        notes
                        players {
                            playerNumber
                            person {
                                externalID
                                nativeFamilyName
                                nativeGivenName
                                tennisId
                                utrId
                                ranking
                            }
                        }
                        extensions {
                            name
                            value
                            description
                        }
                    }
                    winningSide
                    start
                    end
                    type
                    matchUpFormat
                    status
                    tournament {
                        id
                        providerTournamentId
                        tournamentName
                        tournamentId
                        name
                        startDate
                        endDate
                        venue
                        surface
                        category
                    }
                    event {
                        id
                        eventId
                        eventName
                        name
                        eventType
                        gender
                        category
                    }
                    extensions {
                        name
                        value
                        description
                    }
                    roundName
                    roundNumber
                    roundPosition
                    collectionPosition
                    drawId
                    drawPosition
                    structureId
                    bye
                    walkover
                    defaulted
                    retired
                    scheduledDate
                    scheduledTime
                    actualStartTime
                    actualEndTime
                    duration
                    umpire
                    referee
                    courtNumber
                    courtName
                    matchNumber
                }
            }
        }"""

        # Calculate date range
        today = datetime.now()
        days_ago = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
        current_date = today.strftime('%Y-%m-%d')

        variables = {
            "personFilter": {
                "ids": [{
                    "type": "ExternalID",
                    "identifier": person_id
                }]
            },
            "filter": {
                "start": {"after": days_ago},
                "end": {"before": current_date},
                "statuses": ["DEFAULTED", "RETIRED", "WALKOVER", "COMPLETED", "ABANDONED"]
            }
        }

        return self._make_request(query, variables, "comprehensive")

    def fetch_player_matches_minimal_test(self, person_id: str, days_back: int = 360) -> Dict:
        """Test with just the basic fields we know work plus ID"""
        
        query = """query matchUps($personFilter: [td_PersonFilterOptions], $filter: td_MatchUpFilterOptions) {
            td_matchUps(personFilter: $personFilter, filter: $filter) {
                totalItems
                items {
                    id
                    score {
                        scoreString
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
                    drawId
                }
            }
        }"""

        # Calculate date range
        today = datetime.now()
        days_ago = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
        current_date = today.strftime('%Y-%m-%d')

        variables = {
            "personFilter": {
                "ids": [{
                    "type": "ExternalID",
                    "identifier": person_id
                }]
            },
            "filter": {
                "start": {"after": days_ago},
                "end": {"before": current_date},
                "statuses": ["DEFAULTED", "RETIRED", "WALKOVER", "COMPLETED", "ABANDONED"]
            }
        }

        return self._make_request(query, variables, "minimal with ID")

    def _make_request(self, query, variables, query_type):
        """Make the GraphQL request"""
        try:
            self.logger.info(f"Testing {query_type} query...")
            
            request_data = {
                'operationName': 'matchUps',
                'query': query,
                'variables': variables
            }
            
            json_data = json.dumps(request_data).encode('utf-8')
            
            req = urllib.request.Request(
                self.api_url,
                data=json_data,
                headers=self.headers,
                method='POST'
            )
            
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            with urllib.request.urlopen(req, context=ssl_context) as response:
                if response.status == 200:
                    raw_data = response.read()
                    
                    if raw_data.startswith(b'\x1f\x8b'):
                        response_data = gzip.decompress(raw_data).decode('utf-8')
                        self.logger.info("Response was gzipped, decompressed successfully")
                    else:
                        response_data = raw_data.decode('utf-8')
                    
                    data = json.loads(response_data)
                    
                    # Check for GraphQL errors
                    if 'errors' in data:
                        self.logger.error(f"GraphQL errors in {query_type}: {data['errors']}")
                        
                        # Try to identify which fields caused errors
                        for error in data['errors']:
                            if 'message' in error:
                                self.logger.error(f"Error message: {error['message']}")
                            if 'path' in error:
                                self.logger.error(f"Error path: {error['path']}")
                    
                    if data and 'data' in data and data['data'] and 'td_matchUps' in data['data']:
                        match_ups = data['data']['td_matchUps']
                        if match_ups:
                            items = match_ups.get('items', [])
                            total_items = match_ups.get('totalItems', 0)
                            self.logger.info(f"Found {len(items)} matches out of {total_items} total for player")
                            
                            # If we have matches, show the structure of the first one
                            if items:
                                self.logger.info(f"Sample match structure (first match):")
                                self.logger.info(f"Available fields: {list(items[0].keys())}")
                                
                                # Log any ID-like fields
                                id_fields = {}
                                for key, value in items[0].items():
                                    if 'id' in key.lower() or key in ['id', 'matchUpId', 'drawId']:
                                        id_fields[key] = value
                                
                                if id_fields:
                                    self.logger.info(f"ID-like fields found: {id_fields}")
                        else:
                            self.logger.info("td_matchUps is None or empty")
                    
                    return data
                else:
                    self.logger.error(f"Error fetching matches: Status {response.status}")
                    return {}
                
        except Exception as e:
            self.logger.error(f"Error fetching matches ({query_type}): {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {}

def main():
    log_filename = setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting enhanced match query test. Log file: {log_filename}")
    
    fetcher = EnhancedTennisMatchFetcher()
    player_id = "3cb8ad7f-e8e3-4866-aeca-4419adda0297"
    days_back = 1095
    
    # First try the minimal query with ID
    logger.info("="*60)
    logger.info("TESTING MINIMAL QUERY WITH ID FIELD")
    logger.info("="*60)
    
    result_minimal = fetcher.fetch_player_matches_minimal_test(player_id, days_back)
    
    if result_minimal:
        json_filename = f'logs/minimal_with_id_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(result_minimal, f, indent=2, ensure_ascii=False)
        logger.info(f"Minimal query result saved to: {json_filename}")
    
    # Then try the comprehensive query
    logger.info("="*60)
    logger.info("TESTING COMPREHENSIVE QUERY")
    logger.info("="*60)
    
    result_comprehensive = fetcher.fetch_player_matches_comprehensive(player_id, days_back)
    
    if result_comprehensive:
        json_filename = f'logs/comprehensive_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(result_comprehensive, f, indent=2, ensure_ascii=False)
        logger.info(f"Comprehensive query result saved to: {json_filename}")
    
    logger.info(f"Enhanced query testing completed. Check log file: {log_filename}")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Test script to examine the API response structure for player matches
This will help us understand what data is available for linking with tournament draws
"""

import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

class PlayerMatchesAPITester:
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

    def test_player_matches_api(self, person_id: str = None, days_back: int = 365):
        """
        Test the player matches API with a sample player ID
        Try both uppercase and lowercase versions
        """
        
        # Use a sample player ID if none provided
        if not person_id:
            # Use the specific player ID you want to test
            person_id = "57663d67-a8a7-4e50-b3c8-5a324c25cba2"
        
        # Try both cases
        for case_name, test_id in [("UPPERCASE", person_id.upper()), ("lowercase", person_id.lower())]:
            print(f"\n🔍 Testing Player Matches API with {case_name} ID")
            print(f"Player ID (original): {person_id}")
            print(f"Player ID (testing): {test_id}")
            print(f"Date Range: Last {days_back} days")
            print(f"API URL: {self.api_url}")
            print("-" * 80)
            
            result = self._make_api_request(test_id, days_back, case_name)
            if result:
                print(f"✅ SUCCESS with {case_name} ID!")
                return result
            else:
                print(f"❌ No results with {case_name} ID")
        
        print("\n💡 Neither uppercase nor lowercase worked. This could mean:")
        print("   - Player ID doesn't exist in the API")
        print("   - Player has no matches in the last year")
        print("   - API endpoint or authentication issue")
        return None

    def _make_api_request(self, person_id: str, days_back: int, case_label: str):
        """Make the actual API request"""
        
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
                        name
                        start
                        end
                    }
                    extensions {
                        name
                        value
                    }
                    roundName
                    collectionPosition
                    tieMatchUpId
                    drawId
                    venueId
                    roundNumber
                    roundPosition
                    drawName
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
                    "identifier": person_id  # Use the ID as provided (already upper or lower)
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
            
            print(f"Response Status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    print(f"❌ Failed to parse JSON response: {e}")
                    return None
                
                print(f"📄 Response structure: {list(data.keys()) if data else 'Empty response'}")
                
                # Check for errors
                if 'errors' in data:
                    print(f"❌ GraphQL Errors with {case_label}:")
                    for error in data['errors']:
                        print(f"   {error}")
                    
                    # Continue anyway to see if we got partial data
                    if 'data' not in data:
                        return None
                
                # Extract match data
                if 'data' in data:
                    if data['data'] is None:
                        print(f"❌ GraphQL data field is null with {case_label}")
                        return None
                        
                    if 'td_matchUps' not in data['data']:
                        print(f"❌ No 'td_matchUps' field in data with {case_label}. Available fields: {list(data['data'].keys())}")
                        return None
                        
                    match_data = data['data']['td_matchUps']
                    if match_data is None:
                        print(f"❌ td_matchUps field is null with {case_label}")
                        return None
                        
                    total_items = match_data.get('totalItems', 0)
                    items = match_data.get('items', [])
                    
                    print(f"✅ Found {total_items} total matches, returned {len(items)} items with {case_label}")
                    
                    if len(items) > 0:
                        print(f"📋 SAMPLE MATCH DATA STRUCTURE ({case_label}):")
                        print("-" * 80)
                        
                        # Show first match in detail
                        sample_match = items[0]
                        self.print_match_structure(sample_match, indent=0)
                        
                        print("\n" + "=" * 80)
                        print("🔗 POTENTIAL LINKING FIELDS:")
                        print("=" * 80)
                        
                        # Analyze all matches for potential linking fields
                        self.analyze_linking_fields(items)
                        
                        return data
                    else:
                        print(f"⚠️  No matches found with {case_label} in the date range")
                        return None
                else:
                    print(f"❌ No data field in response with {case_label}")
                    return None
            else:
                print(f"❌ API request failed with status {response.status_code}")
                print(f"Response: {response.text[:500]}")
                return None
                
        except Exception as e:
            print(f"❌ Error making API request with {case_label}: {str(e)}")
            return None

    def print_match_structure(self, match_data, indent=0):
        """Recursively print the structure of match data"""
        spaces = "  " * indent
        
        if isinstance(match_data, dict):
            for key, value in match_data.items():
                if isinstance(value, (dict, list)) and len(str(value)) > 100:
                    print(f"{spaces}{key}: {type(value).__name__}")
                    if isinstance(value, list) and len(value) > 0:
                        print(f"{spaces}  └─ [0]: {type(value[0]).__name__}")
                        if isinstance(value[0], dict):
                            self.print_match_structure(value[0], indent + 2)
                    elif isinstance(value, dict):
                        self.print_match_structure(value, indent + 1)
                else:
                    print(f"{spaces}{key}: {value}")
        elif isinstance(match_data, list):
            for i, item in enumerate(match_data[:2]):  # Show first 2 items
                print(f"{spaces}[{i}]: {type(item).__name__}")
                if isinstance(item, dict):
                    self.print_match_structure(item, indent + 1)

    def analyze_linking_fields(self, matches):
        """Analyze matches to identify fields that could link to tournament draws"""
        
        linking_fields = {}
        tournament_fields = {}
        draw_fields = {}
        
        for match in matches:
            # Tournament-related fields
            if 'tournament' in match and match['tournament']:
                for key, value in match['tournament'].items():
                    if key not in tournament_fields:
                        tournament_fields[key] = set()
                    tournament_fields[key].add(str(value)[:50])  # Limit length for display
            
            # Direct linking fields
            potential_fields = [
                'tieMatchUpId', 'drawId', 'venueId', 'roundNumber', 'roundPosition', 
                'roundName', 'collectionPosition', 'drawName'
            ]
            
            for field in potential_fields:
                if field in match and match[field] is not None:
                    if field not in linking_fields:
                        linking_fields[field] = set()
                    linking_fields[field].add(str(match[field])[:50])

        # Print tournament fields
        print("🏆 TOURNAMENT FIELDS:")
        for field, values in tournament_fields.items():
            sample_values = list(values)[:3]
            print(f"   {field}: {sample_values}")
        
        print("\n🔗 POTENTIAL LINKING FIELDS:")
        for field, values in linking_fields.items():
            sample_values = list(values)[:3]
            print(f"   {field}: {sample_values}")
            
        # Specific analysis for tournament draw linking
        print("\n💡 LINKING ANALYSIS:")
        if 'drawId' in linking_fields:
            print("   ✅ drawId available - Could directly link to tournament_draws.draw_id")
        if 'venueId' in linking_fields:
            print("   ✅ venueId available - Could link via venue/event")
        if 'roundPosition' in linking_fields:
            print("   ✅ roundPosition available - Could match tournament_bracket_positions.draw_position")
        if 'collectionPosition' in linking_fields:
            print("   ✅ collectionPosition available - Could use for match ordering")
        if 'roundName' in linking_fields:
            print("   ✅ roundName available - Could derive round info")

def get_sample_player_id():
    """
    Try to get a sample player ID from the database
    """
    try:
        database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
        
        from sqlalchemy import create_engine, text
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            # Try to get player IDs from recent matches with more details
            result = conn.execute(text("""
                SELECT DISTINCT pmp.person_id, pm.tournament_id, pm.start_time, pm.round_name
                FROM player_match_participants pmp
                JOIN player_matches pm ON pmp.match_id = pm.id
                WHERE pm.start_time >= NOW() - INTERVAL '120 days'
                AND pmp.person_id IS NOT NULL
                ORDER BY pm.start_time DESC
                LIMIT 10
            """)).fetchall()
            
            if result:
                print("🎯 Found sample player IDs from recent matches:")
                for i, row in enumerate(result):
                    print(f"   {i+1}. {row[0]} (Tournament: {row[1][:8]}..., Date: {row[2].strftime('%Y-%m-%d')}, Round: {row[3]})")
                return result[0][0]  # Return first player ID
            else:
                # Try tournament bracket positions as backup
                result2 = conn.execute(text("""
                    SELECT DISTINCT participant_id, draw_id
                    FROM tournament_bracket_positions
                    WHERE created_at >= NOW() - INTERVAL '30 days'
                    AND participant_id IS NOT NULL
                    LIMIT 10
                """)).fetchall()
                
                if result2:
                    print("🎯 Found player IDs from tournament bracket positions:")
                    for i, row in enumerate(result2):
                        print(f"   {i+1}. {row[0]} (Draw: {row[1][:8]}...)")
                    return result2[0][0]  # Return first player ID
                else:
                    print("⚠️  No recent player data found in database")
                    return None
                
    except Exception as e:
        print(f"⚠️  Could not get sample player ID from database: {e}")
        return None

if __name__ == "__main__":
    print("🚀 Player Matches API Test Script")
    print("=" * 80)
    
    # Try to get a sample player ID from the database
    sample_player_id = get_sample_player_id()
    
    if not sample_player_id:
        # Fallback to manual input
        sample_player_id = input("\n📝 Enter a player ID to test (or press Enter to use default): ").strip()
        if not sample_player_id:
            sample_player_id = "57663d67-a8a7-4e50-b3c8-5a324c25cba2"  # Updated default
            print(f"Using default player ID: {sample_player_id}")
    
    # Run the test
    tester = PlayerMatchesAPITester()
    result = tester.test_player_matches_api(sample_player_id, days_back=365)
    
    if result:
        print("\n" + "=" * 80)
        print("✅ TEST COMPLETED SUCCESSFULLY!")
        print("💾 Full response data available in 'result' variable")
        print("🔍 Check the linking fields above to determine best approach for connecting to tournament draws")
    else:
        print("\n❌ Test failed - check the output above for details")
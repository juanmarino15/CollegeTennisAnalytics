# scripts/test_api_response.py - FIXED VERSION with consistent ID case handling
import requests
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
import sys
from pathlib import Path

# Add the parent directory to the Python path
backend_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_root))

from models.models import (
    Base, PlayerMatch, PlayerMatchSet, PlayerMatchParticipant,
    Player, PlayerRoster, Season, Tournament, TournamentEvent
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

def normalize_id(id_value: str) -> str:
    """Normalize ID to lowercase for consistent comparison - same as collector"""
    if id_value is None:
        return None
    return str(id_value).lower()

def get_tournament_events_from_db():
    """Get tournament and event IDs from the database"""
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Get OLDEST tournaments with events using case-insensitive joins
        # Older tournaments are more likely to have complete draw data
        results = session.query(
            Tournament.tournament_id,
            Tournament.name.label('tournament_name'),
            Tournament.start_date_time,
            TournamentEvent.event_id,
            TournamentEvent.gender,
            TournamentEvent.event_type
        ).join(
            TournamentEvent, 
            func.lower(Tournament.tournament_id) == func.lower(TournamentEvent.tournament_id)  # Case-insensitive join
        ).filter(
            Tournament.is_cancelled == False,
            Tournament.start_date_time.is_not(None)  # Ensure we have valid dates
        ).order_by(Tournament.start_date_time.asc()).limit(15).all()  # Get oldest first, increased limit
        
        session.close()
        
        test_cases = []
        for result in results:
            # Format the start date for display
            date_str = result.start_date_time.strftime('%Y-%m-%d') if result.start_date_time else 'No Date'
            
            test_cases.append({
                # Store original case IDs (don't force uppercase)
                "tournament_id": result.tournament_id,
                "event_id": result.event_id,
                "description": f"{result.tournament_name} ({date_str}) - {result.gender} {result.event_type}",
                "tournament_name": result.tournament_name,
                "start_date": date_str,
                "gender": result.gender,
                "event_type": result.event_type,
                # Add normalized versions for debugging
                "tournament_id_normalized": normalize_id(result.tournament_id),
                "event_id_normalized": normalize_id(result.event_id)
            })
        
        return test_cases
        
    except Exception as e:
        logging.error(f"Error getting tournament events from database: {str(e)}")
        return []

def test_tournament_draws_api():
    """Test the tournament draws API and show the exact response structure"""
    
    # API configuration
    api_url = "https://prd-itat-kube-tournamentevent-api.clubspark.pro/"
    
    # Headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Origin': 'https://www.collegetennis.com',
        'Referer': 'https://www.collegetennis.com/',
    }
    
    # Get test cases from database
    print("Getting OLDEST tournament events from database (older tournaments more likely to have complete draws)...")
    test_cases = get_tournament_events_from_db()
    
    if not test_cases:
        print("No tournament events found in database!")
        return
    
    print(f"Found {len(test_cases)} tournament/event combinations to test (oldest first)")
    
    # Let user choose which ones to test
    print("\nAvailable test cases (OLDEST tournaments first):")
    for i, test_case in enumerate(test_cases, 1):
        print(f"{i:2d}. {test_case['description']}")
        print(f"     Tournament ID: {test_case['tournament_id']} (normalized: {test_case['tournament_id_normalized']})")
        print(f"     Event ID: {test_case['event_id']} (normalized: {test_case['event_id_normalized']})")
        print(f"     Date: {test_case['start_date']}")
        print()
    
    user_input = input("Enter test case numbers to run (e.g., '1,2,3' or 'all' for all): ").strip()
    
    if user_input.lower() == 'all':
        selected_cases = test_cases
    else:
        try:
            indices = [int(x.strip()) - 1 for x in user_input.split(',')]
            selected_cases = [test_cases[i] for i in indices if 0 <= i < len(test_cases)]
        except ValueError:
            print("Invalid input, testing first case only")
            selected_cases = [test_cases[0]]
    
    for i, test_case in enumerate(selected_cases, 1):
        print(f"\n{'='*80}")
        print(f"TEST CASE {i}/{len(selected_cases)}: {test_case['description']}")
        print(f"Tournament ID: {test_case['tournament_id']}")
        print(f"Event ID: {test_case['event_id']}")
        print(f"Tournament Date: {test_case['start_date']}")
        print(f"Gender: {test_case['gender']}")
        print(f"Event Type: {test_case['event_type']}")
        print(f"Normalized Tournament ID: {test_case['tournament_id_normalized']}")
        print(f"Normalized Event ID: {test_case['event_id_normalized']}")
        print(f"{'='*80}")
        
        # Test with both original case and different case variations
        test_variations = [
            {
                "name": "Original Case",
                "tournament_id": test_case["tournament_id"],
                "event_id": test_case["event_id"]
            },
            {
                "name": "Uppercase",
                "tournament_id": test_case["tournament_id"].upper(),
                "event_id": test_case["event_id"].upper()
            },
            {
                "name": "Lowercase", 
                "tournament_id": test_case["tournament_id"].lower(),
                "event_id": test_case["event_id"].lower()
            }
        ]
        
        for variation in test_variations:
            print(f"\n--- Testing {variation['name']} ---")
            print(f"Tournament ID: {variation['tournament_id']}")
            print(f"Event ID: {variation['event_id']}")
            
            # Create payload - API returns JSON string, not structured data
            payload = {
                "operationName": "TournamentPublicEventData",
                "query": """
                    query TournamentPublicEventData($eventId: ID!, $tournamentId: ID!) {
                        tournamentPublicEventData(eventId: $eventId, tournamentId: $tournamentId)
                    }
                """,
                "variables": {
                    "eventId": variation["event_id"],
                    "tournamentId": variation["tournament_id"]
                }
            }
            
            try:
                print(f"Making API request...")
                response = requests.post(
                    api_url,
                    json=payload,
                    headers=headers,
                    timeout=30,
                    verify=False
                )
                
                print(f"Response Status: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        print(f"\n--- RAW RESPONSE STRUCTURE ({variation['name']}) ---")
                        print(f"Response type: {type(data)}")
                        
                        if isinstance(data, dict):
                            print(f"Top-level keys: {list(data.keys())}")
                            
                            if 'data' in data:
                                print(f"data type: {type(data['data'])}")
                                print(f"data keys: {list(data['data'].keys()) if isinstance(data['data'], dict) else 'Not a dict'}")
                                
                                if 'tournamentPublicEventData' in data['data']:
                                    event_data = data['data']['tournamentPublicEventData']
                                    print(f"tournamentPublicEventData type: {type(event_data)}")
                                    
                                    if event_data is None:
                                        print(f"❌ tournamentPublicEventData is NULL for {variation['name']}!")
                                    elif isinstance(event_data, str):
                                        print(f"✅ tournamentPublicEventData is JSON string (expected!)")
                                        print(f"String length: {len(event_data)}")
                                        print(f"First 200 chars: {event_data[:200]}...")
                                        
                                        try:
                                            parsed = json.loads(event_data)
                                            print(f"✅ Parsed JSON successfully!")
                                            print(f"Parsed data type: {type(parsed)}")
                                            
                                            if isinstance(parsed, dict):
                                                print(f"✅ Parsed JSON keys: {list(parsed.keys())}")
                                                
                                                # Look for the structure we expect
                                                if 'eventData' in parsed:
                                                    event_data_inner = parsed['eventData']
                                                    print(f"✅ eventData found, type: {type(event_data_inner)}")
                                                    
                                                    if isinstance(event_data_inner, dict):
                                                        print(f"✅ eventData keys: {list(event_data_inner.keys())}")
                                                        
                                                        # Check for drawsData
                                                        if 'drawsData' in event_data_inner:
                                                            draws_data = event_data_inner['drawsData']
                                                            print(f"✅ drawsData found! Type: {type(draws_data)}")
                                                            if isinstance(draws_data, list):
                                                                print(f"✅ drawsData length: {len(draws_data)}")
                                                                if draws_data:
                                                                    first_draw = draws_data[0]
                                                                    print(f"✅ First draw keys: {list(first_draw.keys()) if isinstance(first_draw, dict) else 'Not a dict'}")
                                                                    if isinstance(first_draw, dict):
                                                                        print(f"✅ Draw ID: {first_draw.get('drawId')}")
                                                                        print(f"✅ Draw Name: {first_draw.get('drawName')}")
                                                                        positions = first_draw.get('positionAssignments', [])
                                                                        print(f"✅ Position Assignments: {len(positions)}")
                                                                else:
                                                                    print("⚠️  drawsData is empty list")
                                                            else:
                                                                print(f"❌ drawsData is not a list: {type(draws_data)}")
                                                        else:
                                                            print("❌ No 'drawsData' in eventData")
                                                        
                                                        # Check for participants
                                                        if 'participants' in event_data_inner:
                                                            participants = event_data_inner['participants']
                                                            print(f"✅ participants found! Type: {type(participants)}")
                                                            if isinstance(participants, list):
                                                                print(f"✅ participants length: {len(participants)}")
                                                                if participants:
                                                                    first_participant = participants[0]
                                                                    print(f"✅ First participant keys: {list(first_participant.keys()) if isinstance(first_participant, dict) else 'Not a dict'}")
                                                            else:
                                                                print(f"❌ participants is not a list: {type(participants)}")
                                                        else:
                                                            print("❌ No 'participants' in eventData")
                                                    else:
                                                        print(f"❌ eventData is not a dict: {type(event_data_inner)}")
                                                else:
                                                    print("❌ No 'eventData' key in parsed JSON")
                                                    print(f"Available keys: {list(parsed.keys())}")
                                            else:
                                                print(f"❌ Parsed JSON is not a dict: {type(parsed)}")
                                                
                                        except json.JSONDecodeError as e:
                                            print(f"❌ Failed to parse JSON string: {str(e)}")
                                            print(f"Raw string first 500 chars: {event_data[:500]}")
                                    elif isinstance(event_data, dict):
                                        print(f"✅ tournamentPublicEventData is dict with keys: {list(event_data.keys())}")
                                        # This shouldn't happen based on the error, but handle it anyway
                                        print("⚠️  Unexpected: API returned structured data instead of JSON string")
                                    else:
                                        print(f"❌ tournamentPublicEventData is unexpected type: {type(event_data)}")
                                else:
                                    print("❌ No 'tournamentPublicEventData' key in data")
                            else:
                                print("❌ No 'data' key in response")
                            
                            if 'errors' in data:
                                print(f"❌ GraphQL Errors: {data['errors']}")
                                
                                # If this variation fails but others might work, continue
                                if variation['name'] != 'Original Case':
                                    print(f"⚠️  {variation['name']} failed, but continuing with other variations...")
                                    continue
                        
                        # Save response only for successful cases or original case
                        if (event_data and event_data is not None) or variation['name'] == 'Original Case':
                            safe_filename = "".join(c for c in test_case['description'] if c.isalnum() or c in (' ', '-', '_')).rstrip()
                            filename = f"api_response_{i}_{variation['name'].lower().replace(' ', '_')}_{safe_filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                            with open(filename, 'w') as f:
                                json.dump(data, f, indent=2, default=str)
                            print(f"\n💾 Full response saved to: {filename}")
                        
                        # If we got data, no need to test other variations
                        if event_data and event_data is not None:
                            print(f"✅ SUCCESS with {variation['name']} - Found working case!")
                            break
                        
                    except json.JSONDecodeError as e:
                        print(f"❌ Failed to parse JSON response: {str(e)}")
                        print(f"Raw response: {response.text[:500]}...")
                else:
                    print(f"❌ API request failed: {response.status_code}")
                    print(f"Response text: {response.text}")
                    
            except Exception as e:
                print(f"❌ Error making request: {str(e)}")
                import traceback
                print(f"Traceback: {traceback.format_exc()}")
            
            print(f"\n{'-'*60}")
        
        print(f"\n{'='*80}")
        if i < len(selected_cases):
            input("Press Enter to continue to next test case...")

def test_database_id_consistency():
    """Test to check ID case consistency in the database"""
    print("\n" + "="*80)
    print("DATABASE ID CASE CONSISTENCY CHECK")
    print("="*80)
    
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Check Tournament IDs (get both oldest and newest for comparison)
        print("\n--- Tournament ID Case Analysis (Oldest vs Newest) ---")
        oldest_tournaments = session.query(Tournament.tournament_id, Tournament.start_date_time)\
            .filter(Tournament.start_date_time.is_not(None))\
            .order_by(Tournament.start_date_time.asc()).limit(3).all()
        newest_tournaments = session.query(Tournament.tournament_id, Tournament.start_date_time)\
            .filter(Tournament.start_date_time.is_not(None))\
            .order_by(Tournament.start_date_time.desc()).limit(3).all()
        
        print("OLDEST Tournaments:")
        for t in oldest_tournaments:
            original = t.tournament_id
            normalized = normalize_id(original)
            date_str = t.start_date_time.strftime('%Y-%m-%d') if t.start_date_time else 'No Date'
            print(f"  {date_str}: '{original}' -> '{normalized}' -> Same? {original.lower() == normalized}")
        
        print("\nNEWEST Tournaments:")
        for t in newest_tournaments:
            original = t.tournament_id
            normalized = normalize_id(original)
            date_str = t.start_date_time.strftime('%Y-%m-%d') if t.start_date_time else 'No Date'
            print(f"  {date_str}: '{original}' -> '{normalized}' -> Same? {original.lower() == normalized}")
        
        # Check Event IDs  
        print("\n--- Event ID Case Analysis ---")
        events = session.query(TournamentEvent.event_id, TournamentEvent.tournament_id).limit(5).all()
        for e in events:
            event_original = e.event_id
            event_normalized = normalize_id(event_original)
            tournament_original = e.tournament_id
            tournament_normalized = normalize_id(tournament_original)
            print(f"Event: '{event_original}' -> '{event_normalized}'")
            print(f"Tournament: '{tournament_original}' -> '{tournament_normalized}'")
            print()
        
        # Check for potential mismatches
        print("\n--- Checking for Tournament/Event ID Mismatches ---")
        mismatched_query = session.query(
            Tournament.tournament_id.label('t_id'),
            TournamentEvent.tournament_id.label('e_tournament_id'),
            TournamentEvent.event_id
        ).outerjoin(
            TournamentEvent,
            Tournament.tournament_id == TournamentEvent.tournament_id  # Exact match
        ).filter(TournamentEvent.tournament_id.is_(None)).limit(5)
        
        mismatches = mismatched_query.all()
        if mismatches:
            print(f"Found {len(mismatches)} potential mismatches with exact case matching:")
            for m in mismatches[:3]:
                print(f"Tournament '{m.t_id}' has no exact case match in events")
        else:
            print("✅ No mismatches found with exact case matching")
        
        # Check with case-insensitive join
        print("\n--- Checking with Case-Insensitive Join ---")
        case_insensitive_query = session.query(
            Tournament.tournament_id.label('t_id'),
            TournamentEvent.tournament_id.label('e_tournament_id'),
            TournamentEvent.event_id
        ).outerjoin(
            TournamentEvent,
            func.lower(Tournament.tournament_id) == func.lower(TournamentEvent.tournament_id)  # Case-insensitive
        ).filter(TournamentEvent.tournament_id.is_(None)).limit(5)
        
        case_insensitive_mismatches = case_insensitive_query.all()
        if case_insensitive_mismatches:
            print(f"Found {len(case_insensitive_mismatches)} mismatches even with case-insensitive matching:")
            for m in case_insensitive_mismatches[:3]:
                print(f"Tournament '{m.t_id}' has no match in events (case-insensitive)")
        else:
            print("✅ All tournaments have matching events with case-insensitive join")
        
        session.close()
        
    except Exception as e:
        print(f"❌ Error checking database consistency: {str(e)}")

if __name__ == "__main__":
    print("Tournament Draws API Response Test Script - FIXED VERSION")
    print("This will test the API using OLDEST tournament/event IDs from your database")
    print("(Older tournaments are more likely to have complete draw data)")
    print("and show you the exact response structure to help debug the collector")
    print("Now includes ID case consistency testing!")
    
    # First check database ID consistency
    test_database_id_consistency()
    
    print("\nStarting API tests with OLDEST tournaments...")
    test_tournament_draws_api()
    
    print("\nTest completed! Check the JSON files for full response details.")
    print("Look for ❌ markers to see what's causing the errors.")
    print("Look for ✅ markers to see what's working correctly.")
    print("The script now tests multiple case variations to find what works!")
    print("Tested OLDEST tournaments first since they're more likely to have complete draw data.")
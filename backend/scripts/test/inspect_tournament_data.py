# backend/scripts/inspect_tournament_data.py
"""
Script to inspect tournament data from the API before inserting to database
This will help us understand what we're getting and filter properly
"""

import os
import sys
import json
import requests
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add backend to path
backend_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_root))

class TournamentDataInspector:
    def __init__(self):
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

    def create_search_payload(self, 
                            from_date: str = None,
                            to_date: str = None,
                            size: int = 50) -> Dict[str, Any]:
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
                "from": 0,
                "sortKey": "date",
                "latitude": 0,
                "longitude": 0
            }
        }
        
        # Add max date if provided
        if to_date:
            payload["filters"][0]["items"][0]["maxDate"] = to_date
        
        return payload

    def fetch_sample_data(self, size: int = 50) -> Dict[str, Any]:
        """Fetch sample tournament data from the API"""
        try:
            today = datetime.now()
            next_month = today + timedelta(days=30)
            
            payload = self.create_search_payload(
                from_date=today.strftime('%Y-%m-%dT00:00:00.000Z'),
                # to_date=next_month.strftime('%Y-%m-%dT23:59:59.000Z'),
                size=size
            )
            
            print("=== API REQUEST ===")
            print(f"URL: {self.api_url}?indexSchema={self.index_schema}")
            print(f"Payload: {json.dumps(payload, indent=2)}")
            print("\n")
            
            url = f"{self.api_url}?indexSchema={self.index_schema}"
            
            response = requests.post(
                url,
                json=payload,
                headers=self.headers,
                timeout=30,
                verify=False
            )
            
            if response.status_code == 200:
                data = response.json()
                print("=== API RESPONSE STATUS ===")
                print(f"Status Code: {response.status_code}")
                print(f"Total Results: {data.get('total', 0)}")
                print(f"Returned Results: {len(data.get('searchResults', []))}")
                print("\n")
                return data
            else:
                print(f"API request failed with status {response.status_code}: {response.text}")
                return {}
                
        except Exception as e:
            print(f"Error fetching tournament data: {str(e)}")
            return {}

    def classify_tournament_type(self, tournament_data: Dict[str, Any]) -> tuple[bool, str, str]:
        """Classify if this is a dual match or tournament based on events"""
        events = tournament_data.get('events', [])
        
        if not events:
            return False, 'TOURNAMENT', 'No events found'
        
        # Check if this looks like a dual match
        has_singles = any(event.get('division', {}).get('eventType') == 'singles' for event in events)
        has_doubles = any(event.get('division', {}).get('eventType') == 'doubles' for event in events)
        
        # Look at organization structure - dual matches often have simpler org structure
        org_name = tournament_data.get('organization', {}).get('name', '')
        tournament_name = tournament_data.get('name', '')
        
        # Enhanced heuristics for dual match detection
        dual_match_indicators = []
        
        # Check if it has both singles and doubles (common in dual matches)
        if has_singles and has_doubles:
            dual_match_indicators.append("has both singles and doubles")
        
        # Check organization name for team indicators
        if '(' in org_name and ')' in org_name:
            dual_match_indicators.append("org name has parentheses (likely team designation)")
        
        # Check tournament name for "vs" or dual match patterns
        vs_patterns = [' vs ', ' v ', ' @ ', ' at ']
        if any(pattern in tournament_name.lower() for pattern in vs_patterns):
            dual_match_indicators.append("tournament name contains vs/at patterns")
        
        # Check for specific dual match keywords
        dual_keywords = ['dual', 'match', 'versus']
        if any(keyword in tournament_name.lower() for keyword in dual_keywords):
            dual_match_indicators.append("tournament name contains dual match keywords")
        
        # Check number of events - dual matches typically have fewer events
        if len(events) <= 10:  # Most dual matches have 6-7 events (3 singles, 3 doubles, maybe 1 extra)
            dual_match_indicators.append(f"small number of events ({len(events)})")
        
        reason = "; ".join(dual_match_indicators) if dual_match_indicators else "no dual match indicators found"
        
        # Decision logic
        if len(dual_match_indicators) >= 2:  # Need at least 2 indicators to classify as dual match
            return True, 'DUAL_MATCH', reason
        else:
            return False, 'TOURNAMENT', reason

    def analyze_sample_data(self, data: Dict[str, Any]):
        """Analyze the sample data and show classification results"""
        search_results = data.get('searchResults', [])
        
        if not search_results:
            print("No search results to analyze")
            return
        
        print("=== TOURNAMENT ANALYSIS ===")
        
        dual_matches = []
        tournaments = []
        
        for i, result in enumerate(search_results):
            tournament_item = result.get('item', {})
            
            # Classify the tournament
            is_dual_match, tournament_type, reason = self.classify_tournament_type(tournament_item)
            
            # Store classification
            classification_data = {
                'index': i + 1,
                'id': tournament_item.get('id'),
                'name': tournament_item.get('name'),
                'organization': tournament_item.get('organization', {}).get('name', 'Unknown'),
                'events_count': len(tournament_item.get('events', [])),
                'events': [
                    {
                        'type': event.get('division', {}).get('eventType'),
                        'gender': event.get('division', {}).get('gender')
                    } for event in tournament_item.get('events', [])
                ],
                'start_date': tournament_item.get('startDateTime'),
                'is_dual_match': is_dual_match,
                'tournament_type': tournament_type,
                'reason': reason
            }
            
            if is_dual_match:
                dual_matches.append(classification_data)
            else:
                tournaments.append(classification_data)
        
        # Print summary
        print(f"CLASSIFICATION SUMMARY:")
        print(f"Total items analyzed: {len(search_results)}")
        print(f"Classified as DUAL MATCHES: {len(dual_matches)}")
        print(f"Classified as TOURNAMENTS: {len(tournaments)}")
        print("\n")
        
        # Show dual matches (these would be filtered OUT)
        if dual_matches:
            print("=== DUAL MATCHES (WOULD BE FILTERED OUT) ===")
            for dm in dual_matches:
                print(f"{dm['index']}. {dm['name']}")
                print(f"   Organization: {dm['organization']}")
                print(f"   Events: {dm['events_count']} ({[e['type'] for e in dm['events']]})")
                print(f"   Reason: {dm['reason']}")
                print(f"   Start Date: {dm['start_date']}")
                print()
        
        # Show tournaments (these would be kept)
        if tournaments:
            print("=== TOURNAMENTS (WOULD BE KEPT) ===")
            for t in tournaments:
                print(f"{t['index']}. {t['name']}")
                print(f"   Organization: {t['organization']}")
                print(f"   Events: {t['events_count']} ({[e['type'] for e in t['events']]})")
                print(f"   Reason: {t['reason']}")
                print(f"   Start Date: {t['start_date']}")
                print()

    def show_raw_sample(self, data: Dict[str, Any], num_samples: int = 3):
        """Show raw JSON data for a few samples"""
        search_results = data.get('searchResults', [])
        
        if not search_results:
            print("No search results to show")
            return
        
        print(f"=== RAW JSON SAMPLES (First {min(num_samples, len(search_results))} items) ===")
        
        for i in range(min(num_samples, len(search_results))):
            result = search_results[i]
            tournament_item = result.get('item', {})
            
            print(f"\n--- SAMPLE {i + 1}: {tournament_item.get('name', 'Unknown')} ---")
            print(json.dumps(tournament_item, indent=2))
            print("-" * 80)

    def run_inspection(self, sample_size: int = 50, show_raw: bool = False, raw_samples: int = 3):
        """Run the complete inspection"""
        print("TOURNAMENT DATA INSPECTION")
        print("=" * 50)
        
        # Fetch sample data
        data = self.fetch_sample_data(sample_size)
        
        if not data:
            print("Failed to fetch data. Exiting.")
            return
        
        # Show raw samples if requested
        if show_raw:
            self.show_raw_sample(data, raw_samples)
        
        # Analyze and classify
        self.analyze_sample_data(data)
        
        print("\n=== NEXT STEPS ===")
        print("1. Review the classification results above")
        print("2. If the dual match filtering looks correct, proceed with the migration")
        print("3. If you need to adjust the classification logic, modify the classify_tournament_type method")
        print("4. Run this script again until you're satisfied with the filtering")

if __name__ == "__main__":
    inspector = TournamentDataInspector()
    
    # Run inspection with default settings
    inspector.run_inspection(
        sample_size=50,      # Number of tournaments to analyze
        show_raw=True,       # Show raw JSON samples
        raw_samples=2        # Number of raw samples to show
    )
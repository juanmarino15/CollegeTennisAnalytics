import requests
import json
import urllib3
from collections import defaultdict
import csv

# Disable warnings for unverified HTTPS requests
urllib3.disable_warnings()

# API URL
PLAYER_API_URL = 'https://prd-itat-kube.clubspark.pro/mesh-api/graphql'

def fetch_player_matches(player_id):
    """Fetch matches for a specific player"""
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
                "identifier": player_id
            }]
        },
        "filter": {
            "start": {"after": "2023-01-01"},  # Expand date range to get more matches
            "end": {"before": "2025-12-31"},
            "statuses": ["DEFAULTED", "RETIRED", "WALKOVER", "COMPLETED", "ABANDONED"]
        }
    }
    
    try:
        response = requests.post(
            PLAYER_API_URL,
            json={
                'operationName': 'matchUps',
                'query': query,
                'variables': variables
            },
            headers={'Content-Type': 'application/json'},
            verify=False
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching player matches: Status {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching player matches: {e}")
        return None

def analyze_round_names_and_sources(player_ids):
    """Analyze relationship between round names and source values"""
    # Data structures for analysis
    round_name_by_source = defaultdict(set)
    source_by_round_name = defaultdict(set)
    collection_pos_by_source = defaultdict(set)
    match_details = []
    
    print(f"Analyzing {len(player_ids)} players...")
    
    for player_id in player_ids:
        print(f"Fetching matches for player {player_id}...")
        player_data = fetch_player_matches(player_id)
        
        if not player_data or 'data' not in player_data or 'td_matchUps' not in player_data['data']:
            print(f"No match data found for player {player_id}")
            continue
            
        matches = player_data['data']['td_matchUps']['items']
        print(f"Found {len(matches)} matches for player {player_id}")
        
        for match in matches:
            # Extract source value
            source_value = "Unknown"
            dual_match_id = None
            
            for ext in match.get('extensions', []):
                if ext.get('name') == 'SOURCE':
                    source_value = ext.get('value')
                if ext.get('name') == 'dualMatchId':
                    dual_match_id = ext.get('value')
            
            round_name = match.get('roundName')
            collection_position = match.get('collectionPosition')
            
            # Add to our tracking
            if round_name:
                round_name_by_source[source_value].add(round_name)
                source_by_round_name[round_name].add(source_value)
            
            if collection_position:
                collection_pos_by_source[source_value].add(collection_position)
            
            # Add to detailed match info
            match_details.append({
                'player_id': player_id,
                'source': source_value,
                'round_name': round_name if round_name else 'None',
                'collection_position': collection_position if collection_position else 'None',
                'match_type': match.get('type'),
                'format': match.get('matchUpFormat'),
                'dual_match_id': dual_match_id if dual_match_id else 'None',
                'status': match.get('status'),
                'has_tournament_round': bool(round_name and round_name.startswith('R'))
            })
    
    # Write detailed results to CSV
    if match_details:
        output_file = 'round_name_source_analysis.csv'
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = list(match_details[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for detail in match_details:
                writer.writerow(detail)
        
        print(f"Detailed analysis written to {output_file}")
    
    # Print summarized findings
    print("\n=== SOURCES FOUND ===")
    for source, round_names in round_name_by_source.items():
        print(f"Source: {source}")
        print(f"  Round names: {', '.join(sorted(round_names))}")
        print(f"  Collection positions: {', '.join(str(p) for p in sorted(collection_pos_by_source[source]))}")
    
    print("\n=== ROUND NAMES FOUND ===")
    for round_name, sources in source_by_round_name.items():
        print(f"Round name: {round_name}")
        print(f"  Sources: {', '.join(sorted(sources))}")
    
    print("\n=== STATISTICS ===")
    total_matches = len(match_details)
    tournament_matches = sum(1 for m in match_details if m['has_tournament_round'])
    dual_matches = sum(1 for m in match_details if not m['has_tournament_round'] and m['collection_position'] != 'None')
    unknown_matches = total_matches - tournament_matches - dual_matches
    
    print(f"Total matches analyzed: {total_matches}")
    print(f"Likely tournament matches: {tournament_matches} ({tournament_matches/total_matches*100:.1f}%)")
    print(f"Likely dual matches: {dual_matches} ({dual_matches/total_matches*100:.1f}%)")
    print(f"Unknown match types: {unknown_matches} ({unknown_matches/total_matches*100:.1f}%)")
    
    # Analyze sources
    sources_count = defaultdict(int)
    sources_tournament = defaultdict(int)
    sources_dual = defaultdict(int)
    
    for match in match_details:
        sources_count[match['source']] += 1
        if match['has_tournament_round']:
            sources_tournament[match['source']] += 1
        elif match['collection_position'] != 'None':
            sources_dual[match['source']] += 1
    
    print("\n=== SOURCE STATISTICS ===")
    for source, count in sources_count.items():
        tournament_pct = sources_tournament[source] / count * 100 if count > 0 else 0
        dual_pct = sources_dual[source] / count * 100 if count > 0 else 0
        unknown_pct = 100 - tournament_pct - dual_pct
        
        print(f"Source: {source} - {count} matches")
        print(f"  Tournament: {sources_tournament[source]} ({tournament_pct:.1f}%)")
        print(f"  Dual: {sources_dual[source]} ({dual_pct:.1f}%)")
        print(f"  Unknown: {count - sources_tournament[source] - sources_dual[source]} ({unknown_pct:.1f}%)")
    
    # Return the summary
    return {
        'total_matches': total_matches,
        'tournament_matches': tournament_matches,
        'dual_matches': dual_matches,
        'unknown_matches': unknown_matches,
        'sources': dict(sources_count),
        'sources_tournament': dict(sources_tournament),
        'sources_dual': dict(sources_dual)
    }

if __name__ == "__main__":
    # Use multiple players to get a better sample
    player_ids = [
        "04d4294f-c181-4199-9b9b-239b4af22e37",  # From your example
        "3bcecc8e-aa26-4ef7-af6c-fd06329c0e68",  # Another player from your example
        "ada318ae-32f4-417e-a348-b305c3adab91",  # Another player from your example
        # Add more player IDs if needed
    ]
    
    print("ROUND NAME AND SOURCE ANALYSIS")
    print("==============================")
    
    results = analyze_round_names_and_sources(player_ids)
    
    print("\n=== CONCLUSION ===")
    if results['total_matches'] > 0:
        tournament_confidence = results['tournament_matches'] / results['total_matches'] * 100
        print(f"Based on the analysis of {results['total_matches']} matches:")
        print(f"- Round names starting with 'R' (R32, R64, etc.) appear to be {tournament_confidence:.1f}% reliable")
        print(f"  as indicators of tournament matches across all source types.")
        
        # Check if TOURNAMENTDESK is the dominant source for tournament matches
        if 'TOURNAMENTDESK' in results['sources_tournament']:
            td_tournament_pct = results['sources_tournament']['TOURNAMENTDESK'] / results['tournament_matches'] * 100 if results['tournament_matches'] > 0 else 0
            print(f"- TOURNAMENTDESK source is associated with {td_tournament_pct:.1f}% of tournament matches.")
        
        # Make final recommendation
        print("\nRECOMMENDATION:")
        print("Based on this analysis, you should identify tournament matches using:")
        print("1. Round name pattern check (starts with 'R')")
        print("2. Presence of a collection position (for dual matches)")
        print("\nImplement this logic in your player_matches_collector.py when storing matches:")
        print("""
        # Add this field to your PlayerMatch model
        is_tournament_match = Column(Boolean, default=False)
        
        # Then in store_player_matches method:
        match = PlayerMatch(
            # ... existing fields ...
            is_tournament_match=bool(match_item.get('roundName') and match_item.get('roundName').startswith('R')),
        )
        """)
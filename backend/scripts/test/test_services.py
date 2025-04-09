# Existing imports
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from models.models import (Base, Match, Team, TeamLogo, PlayerRoster, 
                           Player, PlayerWTN, PlayerSeason, PlayerMatch,SchoolInfo)
from api.services.match_service import MatchService
from api.services.team_service import TeamService
from api.services.player_service import PlayerService


# Database setup
engine = create_engine("postgresql://juanmarino@localhost:5432/college_tennis_db")
Session = sessionmaker(bind=engine)
db = Session()

print("\nMatch Service Tests:")
match_service = MatchService(db)
matches = match_service.get_matches()
print(f"Total matches: {len(matches)}")
today_matches = match_service.get_matches(date="2024-01-02")
print(f"Today's matches: {len(today_matches)}")

print("\nTeam Service Tests:")
team_service = TeamService(db)

# Test get_teams with and without conference filter
all_teams = team_service.get_teams()
print(f"Total teams: {len(all_teams)}")
sec_teams = team_service.get_teams(conference="Southeastern_Conference")
print(f"Southeastern_Conference teams: {len(sec_teams)}")

# Test specific team
test_team_id = "E71BCE99-4132-47AE-8006-5372D54FFDA9"
team = team_service.get_team(test_team_id)
print(f"\nTesting specific team: {team}")

# Test logo
logo = team_service.get_team_logo(test_team_id)
print(f"Has logo: {logo is not None}")

# Test roster
roster = team_service.get_roster(test_team_id, "2024")
print(f"Roster size: {len(roster)}")

# Debug roster data
raw_roster = db.query(PlayerRoster).filter(
   func.upper(PlayerRoster.team_id) == test_team_id.upper()
).all()
print(f"Raw roster entries: {len(raw_roster)}")

if raw_roster:
   print("\nSample roster entry:")
   print(f"Team ID: {raw_roster[0].team_id}")
   print(f"Player ID: {raw_roster[0].person_id}")

# Add after team service tests
print("\nPlayer Service Tests:")
player_service = PlayerService(db)

# Test get_players (with and without team filter)
all_players = player_service.get_players()
print(f"Total players: {len(all_players)}")

# Use the team_id we know works from previous test
team_players = player_service.get_players(team_id="3e251ad0-bb8a-454c-858c-0b9078381da4")
print(f"Players for specific team: {len(team_players)}")

if team_players:
    test_player = team_players[0]
    print(f"\nTesting player: {test_player.first_name} {test_player.last_name}")
    
    # Test individual player lookup
    player = player_service.get_player(test_player.person_id)
    print(f"Found player: {player.first_name} {player.last_name}")
    
    # Test WTN ratings
    wtn_ratings = player_service.get_player_wtn(player.person_id)
    print(f"WTN ratings found: {len(wtn_ratings)}")
    
    # Test seasons
    seasons = player_service.get_player_seasons(player.person_id)
    print(f"Seasons found: {len(seasons)}")
    
    # Test matches
    matches = player_service.get_player_matches(player.person_id)
    print(f"Matches found: {len(matches)}")

# Add to imports
from api.services.school_service import SchoolService

# Add after player service tests
print("\nSchool Service Tests:")
school_service = SchoolService(db)

# Test get_schools with and without conference filter
all_schools = school_service.get_schools()
print(f"Total schools: {len(all_schools)}")

sec_schools = school_service.get_schools(conference="Southeastern_Conference")
print(f"Southeastern_Conference schools: {len(sec_schools)}")

# Test specific school (using texas as example)
test_school_id = "5f3ec6521de4a073ac089dbb" 
school = school_service.get_school(test_school_id)
if school:
    print(f"\nTesting school: {school.name}")
    print(f"Conference: {school.conference}")
    
    # Test teams for school
    teams = school_service.get_school_teams(school.id)
    print(f"Teams found: {len(teams)}")
    for team in teams:
        print(f"- {team.name} ({team.gender})")

# Add to imports
from api.services.stats_service import StatsService

# Add after school service tests
print("\nStats Service Tests:")
stats_service = StatsService(db)

# Test player stats (using a player we know exists)
if team_players:  # Using player from earlier player service test
    test_player = team_players[0]
    print(f"\nPlayer Stats for {test_player.first_name} {test_player.last_name}:")
    
    # Get overall stats
    player_stats = stats_service.get_player_stats(test_player.person_id)
    if player_stats:
        print("Overall record:")
        print(f"Singles: {player_stats['singles_wins']}-{player_stats['singles_losses']}")
        print(f"Doubles: {player_stats['doubles_wins']}-{player_stats['doubles_losses']}")
    
    # Get stats for specific season
    season_stats = stats_service.get_player_stats(test_player.person_id, season="2024")
    if season_stats:
        print("\n2024 Season:")
        print(f"Singles: {season_stats['singles_wins']}-{season_stats['singles_losses']}")
        print(f"Doubles: {season_stats['doubles_wins']}-{season_stats['doubles_losses']}")

# Test team stats (using texas's ID from earlier)
test_team_id = "e71bce99-4132-47ae-8006-5372d54ffda9"
print(f"\nTeam Stats:")
team_stats = stats_service.get_team_stats(test_team_id)
if team_stats:
    print(f"Overall: {team_stats['total_wins']}-{team_stats['total_losses']}")
    print(f"Home: {team_stats['home_wins']}-{team_stats['home_losses']}")
    print(f"Away: {team_stats['away_wins']}-{team_stats['away_losses']}")

# Test team stats for specific season
season_team_stats = stats_service.get_team_stats(test_team_id, season="2023")
if season_team_stats:
    print(f"\n2024 Season:")
    print(f"Overall: {season_team_stats['total_wins']}-{season_team_stats['total_losses']}")
    print(f"Home: {season_team_stats['home_wins']}-{season_team_stats['home_losses']}")
    print(f"Away: {season_team_stats['away_wins']}-{season_team_stats['away_losses']}")
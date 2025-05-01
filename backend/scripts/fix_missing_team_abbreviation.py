# fix_lineup_team_names.py
import os
import sys
from pathlib import Path
from sqlalchemy import create_engine, func, or_
from sqlalchemy.orm import sessionmaker

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from models.models import Match, MatchLineup, Team, MatchTeam

def fix_match_lineups_team_abbrevs(database_url):
    """Fix missing side1_name and side2_name in match lineups"""
    # Create database connection
    print("Connecting to database...")
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Find all match lineups with null side names
        print("Searching for match lineups with missing team abbreviations...")
        lineups_with_null_sides = session.query(MatchLineup).filter(
            or_(
                MatchLineup.side1_name == None,
                MatchLineup.side2_name == None
            )
        ).all()
        
        print(f"Found {len(lineups_with_null_sides)} match lineups with null team abbreviations")
        
        fixed_count = 0
        processed_count = 0
        
        for lineup in lineups_with_null_sides:
            processed_count += 1
            if processed_count % 100 == 0:
                print(f"Processing lineup {processed_count} of {len(lineups_with_null_sides)}")
                
            # Get match teams
            match_teams = session.query(MatchTeam).filter(
                MatchTeam.match_id == lineup.match_id
            ).all()
            
            if not match_teams:
                print(f"Warning: No teams found for match {lineup.match_id}")
                continue
            
            # Get teams data 
            teams_data = {}
            for mt in match_teams:
                team = session.query(Team).filter(Team.id == mt.team_id).first()
                if team:
                    side_num = mt.side_number if mt.side_number else (1 if mt.is_home_team else 2)
                    teams_data[side_num] = {
                        'id': team.id,
                        'abbreviation': team.abbreviation or team.name[:3].upper()  # Fallback to first 3 letters
                    }
            
            # Update side1_name and side2_name based on team information
            updated = False
            
            if lineup.side1_name is None and 1 in teams_data:
                lineup.side1_name = teams_data[1]['abbreviation']
                updated = True
                
            if lineup.side2_name is None and 2 in teams_data:
                lineup.side2_name = teams_data[2]['abbreviation']
                updated = True
                
            if updated:
                fixed_count += 1
                
            # Commit changes in batches to avoid memory issues
            if fixed_count % 100 == 0:
                print(f"Committing batch of {fixed_count} updates")
                session.commit()
                
        # Final commit for remaining changes
        session.commit()
        print(f"Successfully updated {fixed_count} team abbreviations in match lineups")
        print(f"Process complete: fixed {fixed_count} out of {len(lineups_with_null_sides)} lineups")
        
    except Exception as e:
        print(f"Error fixing match lineups: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    # You can modify this to use command line arguments if needed
    DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

    
    print("Starting script to fix match lineup team abbreviations...")
    fix_match_lineups_team_abbrevs(DATABASE_URL)
    print("Script completed.")
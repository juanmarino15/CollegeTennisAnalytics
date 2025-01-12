import os
import sys
from pathlib import Path

# Add the parent directory to the Python path
current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

from models.models import Match, MatchTeam, Team
from sqlalchemy import create_engine, or_
from sqlalchemy.orm import sessionmaker

def fix_null_team_ids(database_url):
    # Create database connection
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Find all matches with null team IDs
        matches_with_null = session.query(Match).filter(
            or_(
                Match.home_team_id == None,
                Match.away_team_id == None
            )
        ).all()
        
        print(f"Found {len(matches_with_null)} matches with null team IDs")
        
        for match in matches_with_null:
            # Get all teams for this match from match_teams table
            match_teams = session.query(MatchTeam).filter(
                MatchTeam.match_id == match.id
            ).order_by(MatchTeam.side_number).all()
            
            if not match_teams:
                print(f"Warning: No teams found for match {match.id}")
                continue
                
            # Update home team if it's null
            if match.home_team_id is None:
                home_team = next(
                    (mt for mt in match_teams if mt.is_home_team), 
                    match_teams[0]  # Default to first team if no home team is marked
                )
                match.home_team_id = home_team.team_id
                print(f"Updated home team for match {match.id} to {home_team.team_id}")
            
            # Update away team if it's null
            if match.away_team_id is None:
                away_team = next(
                    (mt for mt in match_teams if not mt.is_home_team),
                    match_teams[1] if len(match_teams) > 1 else None
                )
                if away_team:
                    match.away_team_id = away_team.team_id
                    print(f"Updated away team for match {match.id} to {away_team.team_id}")
                else:
                    print(f"Warning: Could not find away team for match {match.id}")
        
        # Commit changes
        session.commit()
        print("Successfully updated all matches with null team IDs")
        
    except Exception as e:
        print(f"Error fixing null team IDs: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    DATABASE_URL = "postgresql://juanmarino@localhost:5432/college_tennis_db"
    fix_null_team_ids(DATABASE_URL)
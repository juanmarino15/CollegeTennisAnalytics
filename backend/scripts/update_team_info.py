import sys
from pathlib import Path

current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
sys.path.append(str(parent_dir))

import logging
from sqlalchemy import create_engine, or_, func
from sqlalchemy.orm import sessionmaker

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('team_update.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Import your models - update the import path as needed
from models.models import Team, SchoolInfo

# Database connection string - replace with your actual connection string
DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

def update_teams_with_school_info():
    """Update teams with missing conference or region from school_info table"""
    # Create database connection
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Find teams with missing conference or region
        teams_to_update = session.query(Team).filter(
            or_(
                Team.conference == None,
                Team.region == None
            )
        ).all()
        
        logging.info(f"Found {len(teams_to_update)} teams with missing conference or region")
        
        # Track statistics
        update_count = 0
        not_found_count = 0
        
        # Process each team
        for team in teams_to_update:
            # Convert team ID to uppercase for comparison
            team_id_upper = team.id.upper()
            
            # Try to find corresponding school in school_info
            # Check both man_id and woman_id, case-insensitive
            school = session.query(SchoolInfo).filter(
                or_(
                    func.upper(SchoolInfo.man_id) == team_id_upper,
                    func.upper(SchoolInfo.woman_id) == team_id_upper
                )
            ).first()
            
            if school:
                # Determine if this is men's or women's team
                is_mens = func.upper(school.man_id) == team_id_upper
                
                # Log which team we're processing
                logging.info(f"Found matching school: {school.name} for team {team.name} (ID: {team.id})")
                
                # Update team fields
                if team.conference is None and school.conference:
                    team.conference = school.conference
                    logging.info(f"Updated conference to: {school.conference}")
                
                if team.region is None and school.ita_region:
                    team.region = school.ita_region
                    logging.info(f"Updated region to: {school.ita_region}")
                
                # Commit changes for this team
                session.commit()
                update_count += 1
            else:
                logging.warning(f"No matching school found for team: {team.name} (ID: {team.id})")
                not_found_count += 1
        
        # Log final statistics
        logging.info(f"Update completed. Updated {update_count} teams.")
        logging.info(f"Could not find school info for {not_found_count} teams.")
        
    except Exception as e:
        logging.error(f"Error updating teams: {e}")
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    logging.info("Starting team update process")
    update_teams_with_school_info()
    logging.info("Team update process completed")
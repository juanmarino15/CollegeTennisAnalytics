#!/usr/bin/env python3
"""
Script to recalculate historical class years for all players based on their current class.

This script:
1. Finds the active season (2025-2026)
2. For each player with a class year in the active season
3. Calculates what their class would have been in previous seasons
4. Updates those historical class years
"""

import logging
import sys
from pathlib import Path
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

backend_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_root))
from models.models import Season, PlayerSeason

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database connection
DATABASE_URL = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"

# Class mappings
CLASS_MAP = {
    'Freshman': 1,
    'Sophomore': 2,
    'Junior': 3,
    'Senior': 4,
    'Fifth Year': 5,
    'Graduate': 6
}

REVERSE_MAP = {
    1: 'Freshman',
    2: 'Sophomore',
    3: 'Junior',
    4: 'Senior',
    5: 'Fifth Year',
    6: 'Graduate'
}


def recalculate_historical_classes(database_url: str, dry_run: bool = True):
    """
    Recalculate historical class years for all players.
    
    Args:
        database_url: PostgreSQL connection string
        dry_run: If True, only show what would be changed without committing
    """
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Get the active season
        active_season = session.query(Season).filter(
            Season.status == 'ACTIVE'
        ).first()
        
        if not active_season:
            logging.error("No active season found!")
            return
        
        logging.info(f"Active season: {active_season.name}")
        active_season_year = int(active_season.name.split('-')[0])
        
        # Get all historical seasons
        historical_seasons = session.query(Season).filter(
            Season.id != active_season.id
        ).order_by(Season.name).all()
        
        logging.info(f"Found {len(historical_seasons)} historical seasons")
        
        # Get all players with a class year in the active season
        active_season_players = session.query(PlayerSeason).filter(
            PlayerSeason.season_id == active_season.id,
            PlayerSeason.class_year.isnot(None)
        ).all()
        
        logging.info(f"Found {len(active_season_players)} players with class years in active season")
        
        total_updated = 0
        total_skipped = 0
        
        # For each player in the active season
        for active_player_season in active_season_players:
            current_class = active_player_season.class_year
            person_id = active_player_season.person_id
            
            if current_class not in CLASS_MAP:
                logging.warning(f"Unknown class '{current_class}' for player {person_id}, skipping")
                continue
            
            current_class_num = CLASS_MAP[current_class]
            
            # Process each historical season for this player
            for hist_season in historical_seasons:
                hist_season_year = int(hist_season.name.split('-')[0])
                year_diff = active_season_year - hist_season_year
                
                # Calculate what class they would have been
                historical_class_num = max(1, current_class_num - year_diff)
                historical_class = REVERSE_MAP.get(historical_class_num)
                
                if not historical_class:
                    continue
                
                # Check if this player has an entry for this historical season
                hist_player_season = session.query(PlayerSeason).filter(
                    PlayerSeason.person_id == person_id,
                    PlayerSeason.season_id == hist_season.id
                ).first()
                
                if hist_player_season:
                    old_class = hist_player_season.class_year
                    
                    # Only update if different
                    if old_class != historical_class:
                        logging.info(
                            f"Player {person_id} - Season {hist_season.name}: "
                            f"{old_class or 'NULL'} → {historical_class} "
                            f"(current: {current_class}, {year_diff} years back)"
                        )
                        
                        if not dry_run:
                            hist_player_season.class_year = historical_class
                            session.merge(hist_player_season)
                        
                        total_updated += 1
                    else:
                        total_skipped += 1
        
        if not dry_run:
            session.commit()
            logging.info(f"✅ Committed changes to database")
        else:
            logging.info(f"🔍 DRY RUN - No changes committed")
        
        logging.info(f"\nSummary:")
        logging.info(f"  Updated: {total_updated} class years")
        logging.info(f"  Skipped (already correct): {total_skipped}")
        
    except Exception as e:
        logging.error(f"Error: {e}")
        import traceback
        logging.error(traceback.format_exc())
        session.rollback()
    finally:
        session.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Recalculate historical class years for all players'
    )
    parser.add_argument(
        '--database-url',
        default=DATABASE_URL,
        help='PostgreSQL connection string (uses hardcoded URL if not provided)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Show what would be changed without committing (default: True)'
    )
    parser.add_argument(
        '--commit',
        action='store_true',
        help='Actually commit the changes to the database'
    )
    
    args = parser.parse_args()
    
    # If --commit is specified, turn off dry_run
    dry_run = not args.commit
    
    if dry_run:
        logging.info("=" * 60)
        logging.info("DRY RUN MODE - No changes will be committed")
        logging.info("Use --commit flag to actually update the database")
        logging.info("=" * 60)
    else:
        logging.info("=" * 60)
        logging.info("COMMIT MODE - Changes will be written to database")
        logging.info("=" * 60)
    
    recalculate_historical_classes(args.database_url, dry_run=dry_run)


if __name__ == '__main__':
    main()
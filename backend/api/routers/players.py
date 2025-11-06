# api/routers/players.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from api.services.player_service import PlayerService
from api.schemas.player import PlayerResponse
from api.database import get_db
from api.schemas.player import PlayerTeamInfo, PlayerStatsInfo, PlayerPositions, PlayerMatchResult,PlayerSearchResult
from models.models import Season


router = APIRouter()

@router.get("/", response_model=List[PlayerResponse])
def get_players(
    team_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = PlayerService(db)
    return service.get_players(team_id=team_id)

@router.get("/search", response_model=List[PlayerSearchResult])
def search_players(
    query: Optional[str] = None,
    gender: Optional[str] = None,
    season_name: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Search for players across all teams and seasons without artificial limits"""
    print(f"DEBUG: Route hit with params: query={query}, gender={gender}, season_name={season_name}")
    service = PlayerService(db)
    try:
        result = service.search_all_players(query, gender, season_name)
        print(f"DEBUG: Route returned {len(result)} results")
        return result
    except Exception as e:
        print(f"ERROR: Exception in search_players route: {str(e)}")
        raise
    
@router.get("/view-test")
def view_test(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("SELECT COUNT(*) FROM player_search_view")).scalar()
        return {"count": result}
    except Exception as e:
        return {"error": str(e)}

@router.get("/{player_id}", response_model=PlayerResponse)
def get_player(player_id: str, db: Session = Depends(get_db)):
    service = PlayerService(db)
    player = service.get_player(player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player

@router.get("/{player_id}/wtn")
def get_player_wtn(
    player_id: str, 
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get a player's World Tennis Number (WTN) ratings, optionally filtered by season"""
    service = PlayerService(db)
    return service.get_player_wtn(player_id, season=season)

@router.get("/{player_id}/matches")
def get_player_matches(player_id: str, db: Session = Depends(get_db)):
    service = PlayerService(db)
    return service.get_player_matches(player_id)    


@router.get("/{player_id}/team", response_model=PlayerTeamInfo)
def get_player_team(
    player_id: str,
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = PlayerService(db)
    
    # If no season is provided, get the active season from the database
    if not season:
        # First try to get the active season from the database
        active_season = db.query(Season).filter(Season.status == 'ACTIVE').first()
        
        if active_season:
            # Use the active season
            season = active_season.name
            print(f"Using active season from database: {season}")
        else:
            # Fallback to determining season based on current date
            from datetime import datetime
            current_date = datetime.now()
            current_year = current_date.year
            current_month = current_date.month
            
            # If month is between January and July, we're in the second half of the academic year
            if 1 <= current_month <= 7:
                season = str(current_year - 1)
            else:
                season = str(current_year)
            print(f"No active season found, using calculated season: {season}")
    
    team = service.get_player_team(player_id, season=season)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found for player")
    return team

@router.get("/{player_id}/stats", response_model=PlayerStatsInfo)
def get_player_stats(
    player_id: str,
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = PlayerService(db)
    stats = service.get_player_stats(player_id, season=season)
    if not stats:
        raise HTTPException(status_code=404, detail="Stats not found for player")
    return stats

@router.get("/{player_id}/positions", response_model=PlayerPositions)
def get_player_positions(
    player_id: str,
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = PlayerService(db)
    positions = service.get_player_positions(player_id, season=season)
    return positions

@router.get("/{player_id}/match-results", response_model=List[PlayerMatchResult])
def get_player_match_results(
    player_id: str,
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Enhanced version of match results with more details"""
    service = PlayerService(db)
    return service.get_player_match_results(player_id, season=season)

@router.get("/{player_id}/seasons")
def get_player_seasons(
    player_id: str,
    include_current: bool = True,
    db: Session = Depends(get_db)
):
    """
    Get seasons where the player has data (roster, WTN, or matches).
    Always includes the current/active season by default.
    
    Args:
        player_id: The player's person_id
        include_current: If True (default), always include the current/active season
    
    Returns:
        List of seasons where player has data
    """
    service = PlayerService(db)
    return service.get_player_seasons(player_id, include_current=include_current)


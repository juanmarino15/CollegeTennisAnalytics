# routers/tournament_players.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from database import get_db
from models.models import TournamentPlayer, Tournament
from services.tournament_players_service import TournamentPlayersService
from pydantic import BaseModel

router = APIRouter()

# Response models
class TournamentPlayerResponse(BaseModel):
    id: str
    tournament_id: str
    player_id: str
    first_name: str
    last_name: str
    player_name: str
    gender: str
    city: Optional[str] = None
    state: Optional[str] = None
    events_participating: str
    singles_event_id: Optional[str] = None
    doubles_event_id: Optional[str] = None
    player2_id: Optional[str] = None
    player2_first_name: Optional[str] = None
    player2_last_name: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class TournamentPlayersListResponse(BaseModel):
    total_items: int
    tournament_id: str
    tournament_name: str
    players: List[TournamentPlayerResponse]

class PlayerStats(BaseModel):
    total_registrations: int
    unique_players: int
    singles_players: int
    doubles_players: int
    both_events_players: int
    gender_breakdown: dict
    state_breakdown: dict

@router.get("/tournament/{tournament_id}/players", response_model=TournamentPlayersListResponse)
async def get_tournament_players(
    tournament_id: str,
    gender: Optional[str] = Query(None, description="Filter by gender (MALE/FEMALE)"),
    event_type: Optional[str] = Query(None, description="Filter by event type (singles/doubles)"),
    state: Optional[str] = Query(None, description="Filter by player state"),
    limit: int = Query(100, ge=1, le=500, description="Number of players to return"),
    offset: int = Query(0, ge=0, description="Number of players to skip"),
    db: Session = Depends(get_db)
):
    """
    Get all players registered for a specific tournament.
    """
    try:
        service = TournamentPlayersService(db)
        result = service.get_tournament_players(
            tournament_id=tournament_id,
            gender=gender,
            event_type=event_type,
            state=state,
            limit=limit,
            offset=offset
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="Tournament not found or no players registered")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting tournament players: {str(e)}")

@router.get("/tournament/{tournament_id}/players/stats", response_model=PlayerStats)
async def get_tournament_player_stats(
    tournament_id: str,
    db: Session = Depends(get_db)
):
    """
    Get statistics about players registered for a tournament.
    """
    try:
        service = TournamentPlayersService(db)
        stats = service.get_tournament_player_stats(tournament_id)
        
        if not stats:
            raise HTTPException(status_code=404, detail="Tournament not found")
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting tournament player stats: {str(e)}")

@router.get("/player/{player_id}/tournaments")
async def get_player_tournaments(
    player_id: str,
    from_date: Optional[str] = Query(None, description="Start date filter (ISO format)"),
    to_date: Optional[str] = Query(None, description="End date filter (ISO format)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get all tournaments a specific player is registered for.
    """
    try:
        # Parse dates
        parsed_from_date = None
        parsed_to_date = None
        
        if from_date:
            parsed_from_date = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
        if to_date:
            parsed_to_date = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
        
        service = TournamentPlayersService(db)
        result = service.get_player_tournaments(
            player_id=player_id,
            from_date=parsed_from_date,
            to_date=parsed_to_date,
            limit=limit,
            offset=offset
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting player tournaments: {str(e)}")

@router.get("/tournaments/search/players")
async def search_tournament_players(
    player_name: Optional[str] = Query(None, description="Search by player name"),
    state: Optional[str] = Query(None, description="Filter by state"),
    gender: Optional[str] = Query(None, description="Filter by gender"),
    tournament_name: Optional[str] = Query(None, description="Filter by tournament name"),
    from_date: Optional[str] = Query(None, description="Tournament start date filter"),
    to_date: Optional[str] = Query(None, description="Tournament end date filter"),
    event_type: Optional[str] = Query(None, description="Filter by event type (singles/doubles)"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Search for tournament players across all tournaments.
    """
    try:
        # Parse dates
        parsed_from_date = None
        parsed_to_date = None
        
        if from_date:
            parsed_from_date = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
        if to_date:
            parsed_to_date = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
        
        service = TournamentPlayersService(db)
        result = service.search_tournament_players(
            player_name=player_name,
            state=state,
            gender=gender,
            tournament_name=tournament_name,
            from_date=parsed_from_date,
            to_date=parsed_to_date,
            event_type=event_type,
            limit=limit,
            offset=offset
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching tournament players: {str(e)}")

@router.get("/tournaments/doubles/partnerships")
async def get_doubles_partnerships(
    tournament_id: Optional[str] = Query(None, description="Filter by tournament ID"),
    player_id: Optional[str] = Query(None, description="Find partnerships for specific player"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get doubles partnerships (player1/player2 pairs) from tournament registrations.
    """
    try:
        service = TournamentPlayersService(db)
        result = service.get_doubles_partnerships(
            tournament_id=tournament_id,
            player_id=player_id,
            limit=limit,
            offset=offset
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting doubles partnerships: {str(e)}")

@router.post("/tournament/{tournament_id}/collect-players")
async def collect_tournament_players(
    tournament_id: str,
    db: Session = Depends(get_db)
):
    """
    Manually trigger collection of players for a specific tournament.
    This endpoint calls the collector to fetch fresh data from the external API.
    """
    try:
        from collector.tournament_players_collector import TournamentPlayersCollector
        import os
        
        # Get database URL (you might want to pass this differently in production)
        database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
        
        collector = TournamentPlayersCollector(database_url)
        collector.collect_players_for_tournament(tournament_id)
        
        return {"message": f"Successfully triggered player collection for tournament {tournament_id}"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error collecting tournament players: {str(e)}")

@router.post("/tournaments/collect-all-players")
async def collect_all_tournament_players(
    from_date: Optional[str] = Query(None, description="Start date for tournaments (ISO format)"),
    to_date: Optional[str] = Query(None, description="End date for tournaments (ISO format)"),
    db: Session = Depends(get_db)
):
    """
    Manually trigger collection of players for all tournaments within date range.
    """
    try:
        from collector.tournament_players_collector import TournamentPlayersCollector
        import os
        
        database_url = "postgresql://dev-college-analyticis-db:AVNS_hhOdMVbRJmDYoEn6Q9z@app-1cef99df-53b2-41c6-8604-aa6d278bdd7d-do-user-18766687-0.j.db.ondigitalocean.com:25060/dev-college-analyticis-db?sslmode=require"
        
        collector = TournamentPlayersCollector(database_url)
        collector.collect_players_for_all_tournaments(from_date=from_date, to_date=to_date)
        
        date_range = ""
        if from_date or to_date:
            date_range = f" from {from_date or 'beginning'} to {to_date or 'end'}"
        
        return {"message": f"Successfully triggered player collection for all tournaments{date_range}"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error collecting all tournament players: {str(e)}")
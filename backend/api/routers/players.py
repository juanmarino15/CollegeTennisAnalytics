# api/routers/players.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from api.services.player_service import PlayerService
from api.schemas.player import PlayerResponse
from api.database import get_db
from api.schemas.player import PlayerTeamInfo, PlayerStatsInfo, PlayerPositions, PlayerMatchResult


router = APIRouter()

@router.get("/", response_model=List[PlayerResponse])
def get_players(
    team_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = PlayerService(db)
    return service.get_players(team_id=team_id)

@router.get("/{player_id}", response_model=PlayerResponse)
def get_player(player_id: str, db: Session = Depends(get_db)):
    service = PlayerService(db)
    player = service.get_player(player_id)
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player

@router.get("/{player_id}/wtn")
def get_player_wtn(player_id: str, db: Session = Depends(get_db)):
    service = PlayerService(db)
    return service.get_player_wtn(player_id)

@router.get("/{player_id}/matches")
def get_player_matches(player_id: str, db: Session = Depends(get_db)):
    service = PlayerService(db)
    return service.get_player_matches(player_id)    

# new
@router.get("/{player_id}/team", response_model=PlayerTeamInfo)
def get_player_team(
    player_id: str,
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = PlayerService(db)
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

# Optionally enhance the existing matches endpoint if you want more detailed info
@router.get("/{player_id}/match-results", response_model=List[PlayerMatchResult])
def get_player_match_results(
    player_id: str,
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Enhanced version of match results with more details"""
    service = PlayerService(db)
    return service.get_player_match_results(player_id, season=season)
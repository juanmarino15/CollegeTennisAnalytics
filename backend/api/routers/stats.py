from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Optional
from api.services.stats_service import StatsService
from api.database import get_db

router = APIRouter()

@router.get("/players/{player_id}")
def get_player_stats(
    player_id: str,
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = StatsService(db)
    stats = service.get_player_stats(player_id, season)
    if not stats:
        raise HTTPException(status_code=404, detail="Player stats not found")
    return stats

@router.get("/teams/{team_id}")
def get_team_stats(
    team_id: str,
    season: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = StatsService(db)
    stats = service.get_team_stats(team_id, season)
    if not stats:
        raise HTTPException(status_code=404, detail="Team stats not found")
    return stats
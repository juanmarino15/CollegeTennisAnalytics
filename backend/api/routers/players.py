# api/routers/players.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from api.services.player_service import PlayerService
from api.schemas.player import PlayerResponse
from api.database import get_db

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
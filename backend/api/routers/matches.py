from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date

from api.services.match_service import MatchService
from api.schemas.match import MatchBase, MatchResponse
from api.database import get_db

router = APIRouter()

@router.get("/", response_model=List[MatchResponse])
def get_matches(
    date: Optional[date] = None,
    team_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = MatchService(db)
    return service.get_matches(date=date, team_id=team_id)

@router.get("/{match_id}", response_model=MatchResponse)
def get_match(match_id: str, db: Session = Depends(get_db)):
    service = MatchService(db)
    match = service.get_match(match_id)
    if match is None:
        raise HTTPException(status_code=404, detail="Match not found")
    return match

@router.get("/{match_id}/lineup")
def get_match_lineup(match_id: str, db: Session = Depends(get_db)):
    service = MatchService(db)
    lineup = service.get_match_lineup(match_id)
    if lineup is None:
        raise HTTPException(status_code=404, detail="Lineup not found")
    return lineup

@router.get("/{match_id}/score")
def get_match_score(match_id: str, db: Session = Depends(get_db)):
    service = MatchService(db)
    score = service.get_match_score(match_id)
    if score is None:
        raise HTTPException(status_code=404, detail="Match score not found")
    return score
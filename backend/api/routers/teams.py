# api/routers/teams.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response 
from sqlalchemy.orm import Session
from typing import List, Optional
from api.services.team_service import TeamService
from api.schemas.team import TeamResponse, TeamLogo
from api.database import get_db

router = APIRouter()

@router.get("/", response_model=List[TeamResponse])
def get_teams(
    conference: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = TeamService(db)
    teams = service.get_teams(conference=conference)
    if not teams:
        return []
    return teams

@router.get("/{team_id}", response_model=TeamResponse)
def get_team(team_id: str, db: Session = Depends(get_db)):
    service = TeamService(db)
    team = service.get_team(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    return team

@router.get("/{team_id}/logo")
def get_team_logo(team_id: str, db: Session = Depends(get_db)):
    service = TeamService(db)
    logo = service.get_team_logo(team_id)
    if not logo:
        raise HTTPException(status_code=404, detail="Logo not found")
    
    return Response(
        content=logo["logo_data"],
        media_type="image/png"  # Adjust based on your logo format
    )

@router.get("/{team_id}/roster")
def get_team_roster(
    team_id: str,
    year: Optional[str] = None,
    db: Session = Depends(get_db)
):
    service = TeamService(db)
    roster = service.get_roster(team_id, year)
    if not roster:
        return []
    return roster
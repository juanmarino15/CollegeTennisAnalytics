# api/routers/teams.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response 
from sqlalchemy.orm import Session
from typing import List, Optional
from api.services.team_service import TeamService
from api.schemas.team import TeamResponse, TeamLogo, TeamBatchRequest, TeamLogoBatchResponse
from api.database import get_db
import logging
import base64

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/", response_model=List[TeamResponse])
def get_teams(
    conference: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get all teams, optionally filtered by conference"""
    service = TeamService(db)
    teams = service.get_teams(conference=conference)
    if not teams:
        return []
    return teams

@router.get("/{team_id}", response_model=TeamResponse)
def get_team(team_id: str, db: Session = Depends(get_db)):
    """Get a single team by ID"""
    service = TeamService(db)
    team = service.get_team(team_id)
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")
    return team

@router.post("/batch", response_model=List[TeamResponse])
def get_teams_batch(
    request: TeamBatchRequest,
    db: Session = Depends(get_db)
):
    """
    Get multiple teams in a single request.
    This is much more efficient than multiple individual requests.
    """
    service = TeamService(db)
    teams = service.get_teams_batch(request.team_ids)
    return teams

@router.get("/{team_id}/logo")
def get_team_logo(team_id: str, db: Session = Depends(get_db)):
    """Get a team's logo"""
    service = TeamService(db)
    logo = service.get_team_logo(team_id)
    if not logo:
        raise HTTPException(status_code=404, detail="Logo not found")
    
    return Response(
        content=logo["logo_data"],
        media_type="image/png"
    )

@router.post("/logos/batch", response_model=TeamLogoBatchResponse)
def get_team_logos_batch(
    request: TeamBatchRequest,
    db: Session = Depends(get_db)
):
    """
    Get multiple team logos in a single request.
    Returns a dictionary mapping team_id to base64 encoded logo data.
    """
    service = TeamService(db)
    logos = service.get_logos_batch(request.team_ids)
    
    # Convert binary data to base64 for JSON response
    logos_b64 = {
        team_id: base64.b64encode(logo_data).decode('utf-8')
        for team_id, logo_data in logos.items()
    }
    
    return TeamLogoBatchResponse(logos=logos_b64)

@router.get("/{team_id}/roster")
def get_team_roster(
    team_id: str,
    year: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get a team's roster for a specific year"""
    service = TeamService(db)
    roster = service.get_roster(team_id, year)
    if not roster:
        return []
    return roster
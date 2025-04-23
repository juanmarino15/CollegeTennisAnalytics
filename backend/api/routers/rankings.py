# api/routers/rankings.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date

from api.services.ranking_service import RankingService
from api.schemas.ranking import (
    RankingListResponse, TeamRankingResponse, 
    PlayerRankingListResponse, PlayerRankingResponse,DoublesRankingResponse
)
from api.database import get_db

router = APIRouter()

# Team rankings endpoints
@router.get("/teams/lists", response_model=List[RankingListResponse])
def get_team_ranking_lists(
    division_type: Optional[str] = None,
    gender: Optional[str] = None,
    limit: Optional[int] = None, 
    db: Session = Depends(get_db)
):
    """Get available team ranking lists with optional filters, always sorted by publish_date desc"""
    service = RankingService(db)
    return service.get_team_ranking_lists(division_type=division_type, gender=gender, limit=limit)

@router.get("/teams/lists/{ranking_id}", response_model=List[TeamRankingResponse])
def get_team_ranking_details(
    ranking_id: str,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get detailed team rankings for a specific ranking list"""
    service = RankingService(db)
    return service.get_team_rankings(ranking_id=ranking_id, limit=limit)

@router.get("/teams/latest")
def get_latest_team_rankings(
    division_type: str = "DIV1",
    gender: str = "M",
    limit: int = 25,
    db: Session = Depends(get_db)
):
    """Get the latest team rankings for the specified division and gender"""
    service = RankingService(db)
    latest_list = service.get_latest_team_ranking_list(division_type=division_type, gender=gender)
    
    if not latest_list:
        raise HTTPException(status_code=404, detail="No team ranking list found")
        
    return service.get_team_rankings(ranking_id=latest_list.id, limit=limit)

@router.get("/teams/{team_id}/history")
def get_team_ranking_history(
    team_id: str,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """Get ranking history for a specific team"""
    service = RankingService(db)
    return service.get_team_ranking_history(team_id=team_id, limit=limit)

# Player rankings endpoints (Singles)
@router.get("/singles/lists", response_model=List[PlayerRankingListResponse])
def get_singles_ranking_lists(
    division_type: Optional[str] = None,
    gender: Optional[str] = None,
    limit: Optional[int] = None,  # Make limit optional
    db: Session = Depends(get_db)
):
    """Get available singles ranking lists with optional filters, always sorted by publish_date desc"""
    service = RankingService(db)
    return service.get_player_ranking_lists(
        division_type=division_type, 
        gender=gender, 
        match_format="SINGLES",
        limit=limit
    )

@router.get("/singles/lists/{ranking_id}", response_model=List[PlayerRankingResponse])
def get_singles_ranking_details(
    ranking_id: str,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get detailed singles rankings for a specific ranking list"""
    service = RankingService(db)
    return service.get_player_rankings(ranking_id=ranking_id, limit=limit)

@router.get("/singles/latest")
def get_latest_singles_rankings(
    division_type: str = "DIV1",
    gender: str = "M",
    limit: int = 25,
    db: Session = Depends(get_db)
):
    """Get the latest singles rankings for the specified division and gender"""
    service = RankingService(db)
    latest_list = service.get_latest_player_ranking_list(
        division_type=division_type, 
        gender=gender,
        match_format="SINGLES"
    )
    
    if not latest_list:
        raise HTTPException(status_code=404, detail="No singles ranking list found")
        
    return service.get_player_rankings(ranking_id=latest_list.id, limit=limit)

@router.get("/singles/players/{player_id}/history")
def get_player_singles_history(
    player_id: str,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """Get singles ranking history for a specific player"""
    service = RankingService(db)
    return service.get_player_ranking_history(
        player_id=player_id, 
        match_format="SINGLES",
        limit=limit
    )

# Player rankings endpoints (Doubles)
@router.get("/doubles/lists", response_model=List[PlayerRankingListResponse])
def get_doubles_ranking_lists(
    division_type: Optional[str] = None,
    gender: Optional[str] = None,
    limit: Optional[int] = None,  # Make limit optional
    db: Session = Depends(get_db)
):
    """Get available doubles ranking lists with optional filters, always sorted by publish_date desc"""
    service = RankingService(db)
    return service.get_player_ranking_lists(
        division_type=division_type, 
        gender=gender, 
        match_format="DOUBLES",
        limit=limit
    )


@router.get("/doubles/latest")
def get_latest_doubles_rankings(
    division_type: str = "DIV1",
    gender: str = "M",
    limit: int = 25,
    db: Session = Depends(get_db)
):
    """Get the latest doubles rankings for the specified division and gender"""
    service = RankingService(db)
    latest_list = service.get_latest_player_ranking_list(
        division_type=division_type, 
        gender=gender,
        match_format="DOUBLES"
    )
    
    if not latest_list:
        raise HTTPException(status_code=404, detail="No doubles ranking list found")
        
    return service.get_player_rankings(ranking_id=latest_list.id, limit=limit)

@router.get("/doubles/players/{player_id}/history")
def get_player_doubles_history(
    player_id: str,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """Get doubles ranking history for a specific player"""
    service = RankingService(db)
    return service.get_player_ranking_history(
        player_id=player_id, 
        match_format="DOUBLES",
        limit=limit
    )

@router.get("/doubles/lists/{ranking_id}", response_model=List[DoublesRankingResponse])
def get_doubles_ranking_details(
    ranking_id: str,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get detailed doubles rankings for a specific ranking list"""
    service = RankingService(db)
    return service.get_doubles_rankings(ranking_id=ranking_id, limit=limit)

@router.get("/doubles/players/{player_id}/history")
def get_player_doubles_history(
    player_id: str,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """Get doubles ranking history for a specific player"""
    service = RankingService(db)
    return service.get_player_doubles_history(
        player_id=player_id, 
        limit=limit
    )
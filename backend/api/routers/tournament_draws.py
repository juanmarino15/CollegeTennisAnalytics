# api/routers/tournament_draws.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from api.services.tournament_draw_service import TournamentDrawService
from api.schemas.tournament_draw import (
    TournamentDrawResponse, TournamentWithDraws, TournamentDrawDetails,
    TournamentListItem, TournamentSearchResponse, TournamentSearchFilters,
    TournamentBracket
)
from api.database import get_db

router = APIRouter()

@router.get("/tournaments", response_model=TournamentSearchResponse)
def get_tournaments(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    sort_by: str = Query("start_date_time", description="Sort field"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order"),
    date_from: Optional[datetime] = Query(None, description="Filter tournaments from this date"),
    date_to: Optional[datetime] = Query(None, description="Filter tournaments until this date"),
    tournament_type: Optional[str] = Query(None, description="Filter by tournament type"),
    location: Optional[str] = Query(None, description="Filter by location"),
    organization: Optional[str] = Query(None, description="Filter by organization"),
    division: Optional[str] = Query(None, description="Filter by division (DIV_I, DIV_II, DIV_III)"),  # ADD THIS
    status: Optional[str] = Query(None, regex="^(upcoming|current|completed)$", description="Filter by status"),
    db: Session = Depends(get_db)
):
    """
    Get a paginated list of tournaments with their basic information and draw counts.
    Supports filtering by date range, type, location, organization, division, and status.
    """
    service = TournamentDrawService(db)
    
    filters = TournamentSearchFilters(
        date_from=date_from,
        date_to=date_to,
        tournament_type=tournament_type,
        location=location,
        organization=organization,
        division=division,  # ADD THIS
        status=status
    )
    
    return service.get_tournaments_list(
        filters=filters,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_order=sort_order
    )

@router.get("/tournaments/search", response_model=TournamentSearchResponse)
def search_tournaments(
    query: Optional[str] = Query(None, description="Search query for tournament name, location, or organization"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Number of items per page"),
    date_from: Optional[datetime] = Query(None, description="Filter tournaments from this date"),
    date_to: Optional[datetime] = Query(None, description="Filter tournaments until this date"),
    tournament_type: Optional[str] = Query(None, description="Filter by tournament type"),
    status: Optional[str] = Query(None, regex="^(upcoming|current|completed)$", description="Filter by status"),
    db: Session = Depends(get_db)
):
    """
    Search tournaments by name, location, or organization with additional filters.
    """
    service = TournamentDrawService(db)
    
    filters = TournamentSearchFilters(
        date_from=date_from,
        date_to=date_to,
        tournament_type=tournament_type,
        status=status
    )
    
    return service.search_tournaments(
        query=query,
        filters=filters,
        page=page,
        page_size=page_size
    )

@router.get("/tournaments/{tournament_id}", response_model=TournamentWithDraws)
def get_tournament_with_draws(
    tournament_id: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed tournament information including all its draws.
    This is the main endpoint for displaying tournament details on the frontend.
    """
    service = TournamentDrawService(db)
    tournament = service.get_tournament_with_draws(tournament_id)
    
    if not tournament:
        raise HTTPException(status_code=404, detail="Tournament not found")
    
    return tournament

@router.get("/tournaments/{tournament_id}/draws", response_model=List[TournamentDrawResponse])
def get_tournament_draws(
    tournament_id: str,
    gender: Optional[str] = Query(None, description="Filter draws by gender (MALE, FEMALE, MIXED)"),
    event_type: Optional[str] = Query(None, description="Filter draws by event type (SINGLES, DOUBLES)"),
    db: Session = Depends(get_db)
):
    """
    Get all draws for a specific tournament with optional filtering by gender and event type.
    """
    service = TournamentDrawService(db)
    draws = service.get_tournament_draws(tournament_id)
    
    if not draws:
        raise HTTPException(status_code=404, detail="No draws found for this tournament")
    
    # Apply filters
    if gender:
        draws = [draw for draw in draws if draw.gender == gender]
    if event_type:
        draws = [draw for draw in draws if draw.event_type == event_type]
    
    return draws

@router.get("/draws/{draw_id}", response_model=TournamentDrawDetails)
def get_draw_details(
    draw_id: str,
    db: Session = Depends(get_db)
):
    """
    Get detailed information about a specific draw including all matches and statistics.
    This endpoint provides complete data for displaying a tournament draw with matches.
    """
    service = TournamentDrawService(db)
    draw_details = service.get_draw_details(draw_id)
    
    if not draw_details:
        raise HTTPException(status_code=404, detail="Draw not found")
    
    return draw_details

@router.get("/draws/{draw_id}/bracket", response_model=TournamentBracket)
def get_draw_bracket(
    draw_id: str,
    db: Session = Depends(get_db)
):
    """
    Get bracket visualization data for a specific draw.
    This endpoint provides structured data for rendering tournament brackets.
    """
    service = TournamentDrawService(db)
    bracket = service.get_draw_bracket(draw_id)
    
    if not bracket:
        raise HTTPException(status_code=404, detail="Draw not found")
    
    return bracket

# Convenience endpoints for common use cases
@router.get("/tournaments/upcoming", response_model=TournamentSearchResponse)
def get_upcoming_tournaments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Get upcoming tournaments (start date in the future).
    """
    service = TournamentDrawService(db)
    
    filters = TournamentSearchFilters(
        status="upcoming",
        date_from=datetime.utcnow()
    )
    
    return service.get_tournaments_list(
        filters=filters,
        page=page,
        page_size=page_size,
        sort_by="start_date_time",
        sort_order="asc"  # Upcoming tournaments should be sorted chronologically
    )

@router.get("/tournaments/current", response_model=TournamentSearchResponse)
def get_current_tournaments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Get currently running tournaments (start date <= now <= end date).
    """
    service = TournamentDrawService(db)
    
    filters = TournamentSearchFilters(status="current")
    
    return service.get_tournaments_list(
        filters=filters,
        page=page,
        page_size=page_size,
        sort_by="start_date_time",
        sort_order="desc"
    )

@router.get("/tournaments/recent", response_model=TournamentSearchResponse)
def get_recent_tournaments(
    days: int = Query(30, ge=1, le=365, description="Number of days to look back"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Get tournaments from the last N days.
    """
    service = TournamentDrawService(db)
    
    from_date = datetime.utcnow() - timedelta(days=days)
    
    filters = TournamentSearchFilters(date_from=from_date)
    
    return service.get_tournaments_list(
        filters=filters,
        page=page,
        page_size=page_size,
        sort_by="start_date_time",
        sort_order="desc"
    )

# Health check endpoint
@router.get("/health")
def health_check():
    """
    Health check endpoint for the tournament draws API.
    """
    return {"status": "healthy", "service": "tournament_draws", "timestamp": datetime.utcnow()}
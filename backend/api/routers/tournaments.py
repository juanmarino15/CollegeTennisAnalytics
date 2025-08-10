# api/routers/tournaments.py
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from typing import Optional, Literal, List
from datetime import datetime
from api.database import get_db
from api.services.tournament_service import TournamentService
from api.schemas.tournament import (
    TournamentSearchResponse, 
    TournamentStatsResponse, 
    TournamentEventResponse,
    TournamentWithEvents,
    TournamentEventStats
)

router = APIRouter()

@router.post("/search", response_model=TournamentSearchResponse)
async def search_tournaments_and_matches(
    match_type: Literal["all", "dual", "tournaments"] = "all",
    from_date: Optional[str] = Query(None, description="Start date in ISO format"),
    to_date: Optional[str] = Query(None, description="End date in ISO format"),
    size: int = Query(25, ge=1, le=100, description="Number of results per page"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    latitude: float = Query(0, description="Latitude for location-based sorting"),
    longitude: float = Query(0, description="Longitude for location-based sorting"),
    sort_key: str = Query("date", description="Sort key (date, distance, etc.)"),
    db: Session = Depends(get_db)
):
    """
    Search for tournaments and dual matches with filtering options.
    
    - **match_type**: Filter by type - "all" (both), "dual" (team vs team), "tournaments" (individual tournaments)
    - **from_date**: Start date filter (ISO format: 2025-07-28T00:00:00.000Z)
    - **to_date**: End date filter (ISO format)
    - **size**: Number of results to return (1-100)
    - **offset**: Pagination offset
    - **latitude/longitude**: Geographic coordinates for distance-based sorting
    - **sort_key**: Sort criteria
    """
    
    try:
        # Parse date strings if provided
        parsed_from_date = None
        parsed_to_date = None
        
        if from_date:
            try:
                parsed_from_date = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid from_date format. Use ISO format.")
        
        if to_date:
            try:
                parsed_to_date = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid to_date format. Use ISO format.")
        
        # Initialize service
        service = TournamentService(db)
        
        # Get results
        results = service.get_tournaments_and_matches(
            match_type=match_type,
            from_date=parsed_from_date,
            to_date=parsed_to_date,
            size=size,
            offset=offset,
            latitude=latitude,
            longitude=longitude,
            sort_key=sort_key
        )
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching tournaments: {str(e)}")

@router.get("/dual-matches")
async def get_dual_matches(
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    size: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get only dual matches (team vs team matches).
    """
    
    try:
        # Parse dates
        parsed_from_date = None
        parsed_to_date = None
        
        if from_date:
            parsed_from_date = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
        if to_date:
            parsed_to_date = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
        
        service = TournamentService(db)
        results = service.search_by_type(
            "dual",
            from_date=parsed_from_date,
            to_date=parsed_to_date,
            size=size,
            offset=offset
        )
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting dual matches: {str(e)}")

@router.get("/tournaments")
async def get_tournaments(
    from_date: Optional[str] = Query(None),
    to_date: Optional[str] = Query(None),
    size: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Get only tournaments (individual competition tournaments).
    """
    
    try:
        # Parse dates
        parsed_from_date = None
        parsed_to_date = None
        
        if from_date:
            parsed_from_date = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
        if to_date:
            parsed_to_date = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
        
        service = TournamentService(db)
        results = service.search_by_type(
            "tournaments",
            from_date=parsed_from_date,
            to_date=parsed_to_date,
            size=size,
            offset=offset
        )
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting tournaments: {str(e)}")

@router.get("/stats", response_model=TournamentStatsResponse)
async def get_tournament_statistics(db: Session = Depends(get_db)):
    """
    Get statistics about dual matches vs tournaments.
    """
    
    try:
        service = TournamentService(db)
        stats = service.get_statistics()
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting statistics: {str(e)}")

# New endpoints for tournament events
@router.get("/{tournament_id}/events", response_model=List[TournamentEventResponse])
async def get_tournament_events(
    tournament_id: str = Path(..., description="Tournament ID"),
    gender: Optional[str] = Query(None, description="Filter by gender (boys, girls, mixed)"),
    event_type: Optional[str] = Query(None, description="Filter by event type (singles, doubles)"),
    db: Session = Depends(get_db)
):
    """
    Get events for a specific tournament, optionally filtered by gender and/or event type.
    """
    
    try:
        service = TournamentService(db)
        events = service.get_tournament_events_by_type(tournament_id, gender, event_type)
        
        # Convert to response format
        event_responses = []
        for event in events:
            event_responses.append(TournamentEventResponse(
                event_id=event.event_id,
                tournament_id=event.tournament_id,
                gender=event.gender,
                event_type=event.event_type,
                created_at=event.created_at,
                updated_at=event.updated_at
            ))
        
        return event_responses
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting tournament events: {str(e)}")

@router.get("/{tournament_id}", response_model=TournamentWithEvents)
async def get_tournament_with_events(
    tournament_id: str = Path(..., description="Tournament ID"),
    db: Session = Depends(get_db)
):
    """
    Get a tournament with all its events.
    """
    
    try:
        service = TournamentService(db)
        tournament_data = service.get_tournament_with_events(tournament_id)
        
        if not tournament_data:
            raise HTTPException(status_code=404, detail="Tournament not found")
        
        return tournament_data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting tournament: {str(e)}")

@router.get("/events/stats", response_model=TournamentEventStats)
async def get_tournament_event_statistics(db: Session = Depends(get_db)):
    """
    Get detailed statistics about tournament events.
    """
    
    try:
        service = TournamentService(db)
        stats = service.get_event_statistics()
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting event statistics: {str(e)}")

@router.get("/events/search")
async def search_tournament_events(
    gender: Optional[str] = Query(None, description="Filter by gender"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    tournament_name: Optional[str] = Query(None, description="Filter by tournament name"),
    from_date: Optional[str] = Query(None, description="Start date filter"),
    to_date: Optional[str] = Query(None, description="End date filter"),
    size: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Search for tournament events across all tournaments.
    """
    
    try:
        # Parse dates
        parsed_from_date = None
        parsed_to_date = None
        
        if from_date:
            parsed_from_date = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
        if to_date:
            parsed_to_date = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
        
        service = TournamentService(db)
        results = service.search_events(
            gender=gender,
            event_type=event_type,
            tournament_name=tournament_name,
            from_date=parsed_from_date,
            to_date=parsed_to_date,
            size=size,
            offset=offset
        )
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching events: {str(e)}")

# Legacy endpoint to maintain compatibility with existing unified search API
@router.post("/query")
async def tournament_query_legacy(
    filters: list = [],
    options: dict = {},
    db: Session = Depends(get_db)
):
    """
    Legacy endpoint that mimics the original unified search API structure.
    This maintains compatibility with existing frontend code.
    """
    
    try:
        # Extract parameters from the legacy format
        size = options.get("size", 25)
        offset = options.get("from", 0)
        sort_key = options.get("sortKey", "date")
        latitude = options.get("latitude", 0)
        longitude = options.get("longitude", 0)
        
        # Extract date filter
        from_date = None
        to_date = None
        
        for filter_item in filters:
            if filter_item.get("key") == "date-range":
                items = filter_item.get("items", [])
                if items:
                    item = items[0]
                    if "minDate" in item:
                        from_date = datetime.fromisoformat(item["minDate"].replace('Z', '+00:00'))
                    if "maxDate" in item:
                        to_date = datetime.fromisoformat(item["maxDate"].replace('Z', '+00:00'))
        
        # Default behavior: return all (dual matches + tournaments)
        service = TournamentService(db)
        results = service.get_tournaments_and_matches(
            match_type="all",
            from_date=from_date,
            to_date=to_date,
            size=size,
            offset=offset,
            latitude=latitude,
            longitude=longitude,
            sort_key=sort_key
        )
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in legacy query: {str(e)}")

@router.get("/health")
async def health_check():
    """Health check endpoint for tournament service"""
    return {"status": "healthy", "service": "tournaments"}
# api/schemas/tournament_draw.py
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

# Base schemas
class TournamentDrawBase(BaseModel):
    draw_id: str
    tournament_id: str
    event_id: str
    draw_name: Optional[str] = None
    draw_type: Optional[str] = None
    draw_size: Optional[int] = None
    event_type: Optional[str] = None
    gender: Optional[str] = None
    draw_completed: Optional[bool] = False
    draw_active: Optional[bool] = False
    match_up_format: Optional[str] = None

class TournamentDrawResponse(TournamentDrawBase):
    updated_at_api: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    stage: Optional[str] = None 
    
    class Config:
        orm_mode = True

# Tournament match schemas
class TournamentMatchBase(BaseModel):
    match_up_id: str
    draw_id: Optional[str] = None
    tournament_id: Optional[str] = None
    event_id: Optional[str] = None
    round_name: Optional[str] = None
    round_number: Optional[int] = None
    round_position: Optional[int] = None
    match_type: Optional[str] = None
    match_format: Optional[str] = None
    match_status: Optional[str] = None
    stage: Optional[str] = None
    structure_name: Optional[str] = None

class TournamentMatchParticipant(BaseModel):
    participant_id: Optional[str] = None
    participant_name: Optional[str] = None
    draw_position: Optional[int] = None
    seed_number: Optional[int] = None
    school_name: Optional[str] = None
    school_id: Optional[str] = None
    player1_id: Optional[str] = None
    player1_name: Optional[str] = None
    player2_id: Optional[str] = None  # For doubles
    player2_name: Optional[str] = None  # For doubles

class TournamentMatchResponse(TournamentMatchBase):
    id: int
    
    # Side 1 participant data
    side1: Optional[TournamentMatchParticipant] = None
    
    # Side 2 participant data
    side2: Optional[TournamentMatchParticipant] = None
    
    # Match outcome
    winning_side: Optional[int] = None
    winner_participant_id: Optional[str] = None
    winner_participant_name: Optional[str] = None
    
    # Scores
    score_side1: Optional[str] = None
    score_side2: Optional[str] = None
    
    # Scheduling
    scheduled_date: Optional[datetime] = None
    scheduled_time: Optional[datetime] = None
    venue_name: Optional[str] = None
    
    # Timestamps
    created_at_api: Optional[datetime] = None
    updated_at_api: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

# Tournament basic info schema
class TournamentInfo(BaseModel):
    tournament_id: str
    name: Optional[str] = None
    start_date_time: Optional[datetime] = None
    end_date_time: Optional[datetime] = None
    location_name: Optional[str] = None
    organization_name: Optional[str] = None
    tournament_type: Optional[str] = None
    
    class Config:
        from_attributes = True

# Combined response schemas
class TournamentDrawWithMatches(TournamentDrawResponse):
    matches: List[TournamentMatchResponse] = []

class TournamentWithDraws(TournamentInfo):
    draws: List[TournamentDrawResponse] = []

class TournamentDrawDetails(TournamentDrawResponse):
    tournament: Optional[TournamentInfo] = None
    matches: List[TournamentMatchResponse] = []
    
    # Draw statistics
    total_matches: int = 0
    completed_matches: int = 0
    scheduled_matches: int = 0
    participants_count: int = 0

# Tournament list response for frontend
class TournamentListItem(BaseModel):
    tournament_id: str
    name: Optional[str] = None
    start_date_time: Optional[datetime] = None
    end_date_time: Optional[datetime] = None
    location_name: Optional[str] = None
    organization_name: Optional[str] = None
    organization_division: Optional[str] = None  
    tournament_type: Optional[str] = None
    draws_count: int = 0
    events: List[str] = []  # List of event types like ["Men's Singles", "Women's Doubles"]
    
    class Config:
        from_attributes = True

# Bracket visualization schema
class BracketPosition(BaseModel):
    draw_position: int
    round_number: int
    participant_name: Optional[str] = None
    participant_id: Optional[str] = None
    seed_number: Optional[int] = None
    school_name: Optional[str] = None
    is_bye: bool = False
    is_winner: bool = False
    advanced_to_position: Optional[int] = None

class TournamentBracket(BaseModel):
    draw_id: str
    draw_name: str
    draw_type: str
    draw_size: int
    rounds: int
    positions: List[BracketPosition] = []
    
    class Config:
        from_attributes = True

# Search and filter schemas
class TournamentSearchFilters(BaseModel):
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    tournament_type: Optional[str] = None
    location: Optional[str] = None
    organization: Optional[str] = None
    division: Optional[str] = None  
    gender: Optional[str] = None
    event_type: Optional[str] = None
    status: Optional[str] = None  # upcoming, current, completed

class TournamentSearchResponse(BaseModel):
    tournaments: List[TournamentListItem]
    total_count: int
    page: int
    page_size: int
    has_next: bool
    has_previous: bool